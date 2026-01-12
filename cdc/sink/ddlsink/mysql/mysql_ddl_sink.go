// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/coreos/go-semver/semver"
	lru "github.com/hashicorp/golang-lru"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/version"
	"github.com/pingcap/tidb/dumpling/export"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	tidbmysql "github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/ddlsink"
	"github.com/pingcap/tiflow/cdc/sink/metrics"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/errorutil"
	"github.com/pingcap/tiflow/pkg/quotes"
	"github.com/pingcap/tiflow/pkg/retry"
	"github.com/pingcap/tiflow/pkg/sink"
	pmysql "github.com/pingcap/tiflow/pkg/sink/mysql"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

const (
	defaultDDLMaxRetry uint64 = 20

	// networkDriftDuration is used to construct a context timeout for database operations.
	networkDriftDuration = 5 * time.Second

	defaultSupportVectorVersion = "8.4.0"
)

// GetDBConnImpl is the implementation of pmysql.IDBConnectionFactory.
// Exported for testing.
var GetDBConnImpl pmysql.IDBConnectionFactory = &pmysql.DBConnectionFactory{}

// Assert Sink implementation
var _ ddlsink.Sink = (*DDLSink)(nil)

// DDLSink is a sink that writes DDL events to MySQL.
type DDLSink struct {
	// id indicates which processor (changefeed) this sink belongs to.
	id model.ChangeFeedID
	// db is the database connection.
	db  *sql.DB
	cfg *pmysql.Config
	// statistics is the statistics of this sink.
	// We use it to record the DDL count.
	statistics *metrics.Statistics

	// lastExecutedNormalDDLCache is a fast path to check whether aync DDL of a table
	// is running in downstream.
	// map: model.TableName -> timodel.ActionType
	lastExecutedNormalDDLCache *lru.Cache

	needFormat bool
}

// NewDDLSink creates a new DDLSink.
func NewDDLSink(
	ctx context.Context,
	changefeedID model.ChangeFeedID,
	sinkURI *url.URL,
	replicaConfig *config.ReplicaConfig,
) (*DDLSink, error) {
	cfg := pmysql.NewConfig()
	err := cfg.Apply(config.GetGlobalServerConfig().TZ, changefeedID, sinkURI, replicaConfig)
	if err != nil {
		return nil, err
	}

	dsnStr, err := pmysql.GenerateDSN(ctx, sinkURI, cfg, GetDBConnImpl.CreateTemporaryConnection)
	if err != nil {
		return nil, err
	}

	db, err := GetDBConnImpl.CreateStandardConnection(ctx, dsnStr)
	if err != nil {
		return nil, err
	}

	cfg.IsTiDB = pmysql.CheckIsTiDB(ctx, db)

	cfg.IsWriteSourceExisted, err = pmysql.CheckIfBDRModeIsSupported(ctx, db)
	if err != nil {
		return nil, err
	}

	lruCache, err := lru.New(1024)
	if err != nil {
		return nil, err
	}

	m := &DDLSink{
		id:                         changefeedID,
		db:                         db,
		cfg:                        cfg,
		statistics:                 metrics.NewStatistics(changefeedID, sink.TxnSink),
		lastExecutedNormalDDLCache: lruCache,
		needFormat:                 needFormatDDL(db, cfg),
	}

	log.Info("MySQL DDL sink is created",
		zap.String("namespace", m.id.Namespace),
		zap.String("changefeed", m.id.ID))
	return m, nil
}

// WriteDDLEvent writes a DDL event to the mysql database.
func (m *DDLSink) WriteDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	m.waitAsynExecDone(ctx, ddl)

	if m.shouldAsyncExecDDL(ddl) {
		m.lastExecutedNormalDDLCache.Remove(ddl.TableInfo.TableName)
		return m.asyncExecDDL(ctx, ddl)
	}

	if err := m.execDDLWithMaxRetries(ctx, ddl); err != nil {
		return errors.Trace(err)
	}
	m.lastExecutedNormalDDLCache.Add(ddl.TableInfo.TableName, ddl.Type)
	return nil
}

func (m *DDLSink) execDDLWithMaxRetries(ctx context.Context, ddl *model.DDLEvent) error {
	return retry.Do(ctx, func() error {
		err := m.statistics.RecordDDLExecution(func() error { return m.execDDL(ctx, ddl) })
		if err != nil {
			if errorutil.IsIgnorableMySQLDDLError(err) {
				// NOTE: don't change the log, some tests depend on it.
				log.Info("Execute DDL failed, but error can be ignored",
					zap.Uint64("startTs", ddl.StartTs), zap.String("ddl", ddl.Query),
					zap.String("namespace", m.id.Namespace),
					zap.String("changefeed", m.id.ID),
					zap.Error(err))
				// If the error is ignorable, we will ignore the error directly.
				return nil
			}
			log.Warn("Execute DDL with error, retry later",
				zap.Uint64("startTs", ddl.StartTs), zap.String("ddl", ddl.Query),
				zap.String("namespace", m.id.Namespace),
				zap.String("changefeed", m.id.ID),
				zap.Error(err))
			return err
		}
		return nil
	}, retry.WithBackoffBaseDelay(pmysql.BackoffBaseDelay.Milliseconds()),
		retry.WithBackoffMaxDelay(pmysql.BackoffMaxDelay.Milliseconds()),
		retry.WithMaxTries(defaultDDLMaxRetry),
		retry.WithIsRetryableErr(errorutil.IsRetryableDDLError))
}

// isReorgOrPartitionDDL returns true if given ddl type is reorg ddl or
// partition ddl.
func isReorgOrPartitionDDL(t timodel.ActionType) bool {
	// partition related ddl
	return t == timodel.ActionAddTablePartition ||
		t == timodel.ActionExchangeTablePartition ||
		t == timodel.ActionReorganizePartition ||
		// reorg ddls
		t == timodel.ActionAddPrimaryKey ||
		t == timodel.ActionAddIndex ||
		t == timodel.ActionModifyColumn ||
		// following ddls can be fast when the downstream is TiDB, we must
		// still take them into consideration to ensure compatibility with all
		// MySQL-compatible databases.
		t == timodel.ActionAddColumn ||
		t == timodel.ActionAddColumns ||
		t == timodel.ActionDropColumn ||
		t == timodel.ActionDropColumns
}

func (m *DDLSink) execDDL(pctx context.Context, ddl *model.DDLEvent) error {
	ctx := pctx
	// When executing Reorg and Partition DDLs in TiDB, there is no timeout
	// mechanism by default. Instead, the system will wait for the DDL operation
	// to be executed or completed before proceeding.
	if !isReorgOrPartitionDDL(ddl.Type) {
		writeTimeout, _ := time.ParseDuration(m.cfg.WriteTimeout)
		writeTimeout += networkDriftDuration
		var cancelFunc func()
		ctx, cancelFunc = context.WithTimeout(pctx, writeTimeout)
		defer cancelFunc()
	}

	shouldSwitchDB := needSwitchDB(ddl)

	// Convert vector type to string type for unsupport database
	if m.needFormat {
		if newQuery := formatQuery(ddl.Query); newQuery != ddl.Query {
			log.Warn("format ddl query", zap.String("newQuery", newQuery), zap.String("query", ddl.Query), zap.String("collate", ddl.Collate), zap.String("charset", ddl.Charset))
			ddl.Query = newQuery
		}
	}

	failpoint.Inject("MySQLSinkExecDDLDelay", func() {
		select {
		case <-ctx.Done():
			failpoint.Return(ctx.Err())
		case <-time.After(time.Hour):
		}
		failpoint.Return(nil)
	})

	start := time.Now()
	log.Info("Start exec DDL", zap.String("namespace", m.id.Namespace), zap.String("changefeed", m.id.ID),
		zap.Uint64("commitTs", ddl.CommitTs), zap.Uint64("startTs", ddl.StartTs), zap.String("DDL", ddl.Query))
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	if shouldSwitchDB {
		_, err = tx.ExecContext(ctx, "USE "+quotes.QuoteName(ddl.TableInfo.TableName.Schema)+";")
		if err != nil {
			if tsErr := resetSessionTimestamp(ctx, tx); tsErr != nil {
				log.Warn("Failed to reset session timestamp after USE failure", zap.Error(tsErr))
			}
			if rbErr := tx.Rollback(); rbErr != nil {
				log.Error("Failed to rollback", zap.String("namespace", m.id.Namespace),
					zap.String("changefeed", m.id.ID), zap.Error(err))
			}
			return err
		}
	}

	// we try to set cdc write source for the ddl
	if err = pmysql.SetWriteSource(pctx, m.cfg, tx); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			if errors.Cause(rbErr) != context.Canceled {
				log.Error("Failed to rollback",
					zap.String("namespace", m.id.Namespace),
					zap.String("changefeed", m.id.ID), zap.Error(err))
			}
		}
		return err
	}

	// Each DDL statement should use its own StartTs timestamp from upstream
	// This ensures that timestamp functions like NOW(), CURRENT_TIMESTAMP(), LOCALTIME()
	// are evaluated with the exact same temporal context as upstream
	tsToUse := ddl.StartTs
	if tsToUse == 0 {
		// If StartTs is not available, fall back to CommitTs
		// This preserves timestamp consistency for DDL statements executed in the same transaction
		tsToUse = ddl.CommitTs
	}
	ddlTime := oracle.GetTimeFromTS(tsToUse)
	// Use second-level precision to match upstream default value evaluation.
	ddlTimestamp := float64(ddlTime.Unix())

	if ts, ok := ddlSessionTimestampFromOriginDefault(ddl, m.cfg.Timezone); ok {
		ddlTimestamp = ts
	}

	// Set the session timestamp to match upstream DDL execution time
	// This is critical for preserving timestamp function behavior
	if err := setSessionTimestamp(ctx, tx, ddlTimestamp); err != nil {
		log.Error("Fail to set session timestamp for DDL",
			zap.Float64("timestamp", ddlTimestamp),
			zap.Uint64("startTs", tsToUse),
			zap.Uint64("commitTs", ddl.CommitTs),
			zap.String("query", ddl.Query),
			zap.Error(err))
		if rbErr := tx.Rollback(); rbErr != nil {
			log.Error("Failed to rollback", zap.String("namespace", m.id.Namespace),
				zap.String("changefeed", m.id.ID), zap.Error(err))
		}
		return err
	}

	if _, err = tx.ExecContext(ctx, ddl.Query); err != nil {
		log.Error("Failed to execute DDL", zap.Any("err", err), zap.Any("query", ddl.Query))
		if tsErr := resetSessionTimestamp(ctx, tx); tsErr != nil {
			log.Warn("Failed to reset session timestamp after DDL execution failure", zap.Error(tsErr))
		}
		if rbErr := tx.Rollback(); rbErr != nil {
			log.Error("Failed to rollback",
				zap.String("namespace", m.id.Namespace),
				zap.String("changefeed", m.id.ID),
				zap.String("sql", ddl.Query),
				zap.Error(err))
		}
		return err
	}

	// Reset session timestamp after DDL execution to avoid affecting subsequent operations
	if err := resetSessionTimestamp(ctx, tx); err != nil {
		log.Error("Failed to reset session timestamp after DDL execution", zap.Error(err))
		if rbErr := tx.Rollback(); rbErr != nil {
			log.Error("Failed to rollback", zap.String("namespace", m.id.Namespace),
				zap.String("changefeed", m.id.ID), zap.Error(err))
		}
		return errors.WrapError(errors.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("Query info: %s; ", ddl.Query)))
	}

	// Log successful DDL execution with timestamp information for debugging
	log.Debug("DDL executed with timestamp",
		zap.String("query", ddl.Query),
		zap.Uint64("startTs", tsToUse),
		zap.Float64("sessionTimestamp", ddlTimestamp))

	if err = tx.Commit(); err != nil {
		log.Error("Failed to exec DDL", zap.String("namespace", m.id.Namespace), zap.String("changefeed", m.id.ID),
			zap.Duration("duration", time.Since(start)), zap.String("sql", ddl.Query), zap.Error(err))
		return errors.WrapError(errors.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("Query info: %s; ", ddl.Query)))
	}

	log.Info("Exec DDL succeeded",
		zap.String("namespace", m.id.Namespace), zap.String("changefeed", m.id.ID),
		zap.Duration("duration", time.Since(start)), zap.Uint64("startTs", tsToUse),
		zap.Float64("sessionTimestamp", ddlTimestamp), zap.String("sql", ddl.Query))
	return nil
}

func setSessionTimestamp(ctx context.Context, tx *sql.Tx, unixTimestamp float64) error {
	_, err := tx.ExecContext(ctx, fmt.Sprintf("SET TIMESTAMP = %s", formatUnixTimestamp(unixTimestamp)))
	return err
}

func resetSessionTimestamp(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, "SET TIMESTAMP = DEFAULT")
	return err
}

func formatUnixTimestamp(unixTimestamp float64) string {
	return strconv.FormatFloat(unixTimestamp, 'f', 6, 64)
}

func ddlSessionTimestampFromOriginDefault(ddl *model.DDLEvent, timezone string) (float64, bool) {
	if ddl == nil || ddl.TableInfo == nil {
		return 0, false
	}
	targetColumns, err := extractCurrentTimestampDefaultColumns(ddl.Query)
	if err != nil || len(targetColumns) == 0 {
		return 0, false
	}

	for _, col := range ddl.TableInfo.Columns {
		if _, ok := targetColumns[col.Name.L]; !ok {
			continue
		}
		val := col.GetOriginDefaultValue()
		valStr, ok := val.(string)
		if !ok || valStr == "" {
			continue
		}
		ts, err := parseOriginDefaultTimestamp(valStr, col, timezone)
		if err != nil {
			log.Warn("Failed to parse OriginDefaultValue for DDL timestamp",
				zap.String("column", col.Name.O),
				zap.String("originDefault", valStr),
				zap.Error(err))
			continue
		}
		log.Info("Using OriginDefaultValue for DDL timestamp",
			zap.String("column", col.Name.O),
			zap.String("originDefault", valStr),
			zap.Float64("timestamp", ts),
			zap.String("timezone", timezone))
		return ts, true
	}

	return 0, false
}

func extractCurrentTimestampDefaultColumns(query string) (map[string]struct{}, error) {
	p := parser.New()
	stmt, err := p.ParseOneStmt(query, "", "")
	if err != nil {
		return nil, err
	}

	cols := make(map[string]struct{})
	switch s := stmt.(type) {
	case *ast.CreateTableStmt:
		for _, col := range s.Cols {
			if hasCurrentTimestampDefault(col) {
				cols[col.Name.Name.L] = struct{}{}
			}
		}
	case *ast.AlterTableStmt:
		for _, spec := range s.Specs {
			switch spec.Tp {
			case ast.AlterTableAddColumns, ast.AlterTableModifyColumn, ast.AlterTableChangeColumn, ast.AlterTableAlterColumn:
				for _, col := range spec.NewColumns {
					if hasCurrentTimestampDefault(col) {
						cols[col.Name.Name.L] = struct{}{}
					}
				}
			}
		}
	}

	return cols, nil
}

func hasCurrentTimestampDefault(col *ast.ColumnDef) bool {
	if col == nil {
		return false
	}
	for _, opt := range col.Options {
		if opt.Tp != ast.ColumnOptionDefaultValue {
			continue
		}
		if isCurrentTimestampExpr(opt.Expr) {
			return true
		}
	}
	return false
}

func isCurrentTimestampExpr(expr ast.ExprNode) bool {
	if expr == nil {
		return false
	}
	switch v := expr.(type) {
	case *ast.FuncCallExpr:
		return isCurrentTimestampFuncName(v.FnName.L)
	case ast.ValueExpr:
		return isCurrentTimestampFuncName(strings.ToLower(v.GetString()))
	default:
		return false
	}
}

func isCurrentTimestampFuncName(name string) bool {
	switch name {
	case ast.CurrentTimestamp, ast.Now, ast.LocalTime, ast.LocalTimestamp:
		return true
	default:
		return false
	}
}

func parseOriginDefaultTimestamp(val string, col *timodel.ColumnInfo, timezone string) (float64, error) {
	loc, err := resolveOriginDefaultLocation(col, timezone)
	if err != nil {
		return 0, err
	}
	return parseTimestampInLocation(val, loc)
}

func resolveOriginDefaultLocation(col *timodel.ColumnInfo, timezone string) (*time.Location, error) {
	if col != nil && col.GetType() == tidbmysql.TypeTimestamp && col.Version >= timodel.ColumnInfoVersion1 {
		return time.UTC, nil
	}
	if timezone == "" {
		return time.UTC, nil
	}
	tz := strings.Trim(timezone, "\"")
	return time.LoadLocation(tz)
}

func parseTimestampInLocation(val string, loc *time.Location) (float64, error) {
	formats := []string{
		"2006-01-02 15:04:05",
		"2006-01-02 15:04:05.999999",
	}
	for _, f := range formats {
		t, err := time.ParseInLocation(f, val, loc)
		if err == nil {
			return float64(t.UnixNano()) / float64(time.Second), nil
		}
	}
	return 0, fmt.Errorf("failed to parse timestamp: %s", val)
}

func needSwitchDB(ddl *model.DDLEvent) bool {
	if len(ddl.TableInfo.TableName.Schema) == 0 {
		return false
	}
	if ddl.Type == timodel.ActionCreateSchema || ddl.Type == timodel.ActionDropSchema {
		return false
	}
	return true
}

// needFormatDDL checks vector type support
func needFormatDDL(db *sql.DB, cfg *pmysql.Config) bool {
	if !cfg.HasVectorType {
		log.Warn("please set `has-vector-type` to be true if a column is vector type when the downstream is not TiDB or TiDB version less than specify version",
			zap.Any("hasVectorType", cfg.HasVectorType), zap.Any("supportVectorVersion", defaultSupportVectorVersion))
		return false
	}
	versionInfo, err := export.SelectVersion(db)
	if err != nil {
		log.Warn("fail to get version", zap.Error(err), zap.Bool("isTiDB", cfg.IsTiDB))
		return false
	}
	serverInfo := version.ParseServerInfo(versionInfo)
	version := semver.New(defaultSupportVectorVersion)
	if !cfg.IsTiDB || serverInfo.ServerVersion.LessThan(*version) {
		log.Error("downstream unsupport vector type. it will be converted to longtext", zap.String("version", serverInfo.ServerVersion.String()), zap.String("supportVectorVersion", defaultSupportVectorVersion), zap.Bool("isTiDB", cfg.IsTiDB))
		return true
	}
	return false
}

// WriteCheckpointTs does nothing.
func (m *DDLSink) WriteCheckpointTs(_ context.Context, _ uint64, _ []*model.TableInfo) error {
	// Only for RowSink for now.
	return nil
}

// Close closes the database connection.
func (m *DDLSink) Close() {
	if m.statistics != nil {
		m.statistics.Close()
	}
	if m.db != nil {
		if err := m.db.Close(); err != nil {
			log.Warn("MySQL ddl sink close db wit error",
				zap.String("namespace", m.id.Namespace),
				zap.String("changefeed", m.id.ID),
				zap.Error(err))
		}
	}
}
