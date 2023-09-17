/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.reporting.sql;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.sql.bulletins.BulletinTable;
import org.apache.nifi.reporting.sql.connectionstatus.ConnectionStatusTable;
import org.apache.nifi.reporting.sql.connectionstatuspredictions.ConnectionStatusPredictionsTable;
import org.apache.nifi.reporting.sql.flowconfighistory.FlowConfigHistoryTable;
import org.apache.nifi.reporting.sql.metrics.JvmMetricsTable;
import org.apache.nifi.reporting.sql.processgroupstatus.ProcessGroupStatusTable;
import org.apache.nifi.reporting.sql.processorstatus.ProcessorStatusTable;
import org.apache.nifi.reporting.sql.provenance.ProvenanceTable;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.ResultSetRecordSet;
import org.apache.nifi.util.db.JdbcCommon;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;

public class MetricsSqlQueryService implements MetricsQueryService {

    private final ComponentLog logger;
    private final int defaultPrecision;
    private final int defaultScale;

    private final Cache<String, BlockingQueue<CachedStatement>> statementQueues = Caffeine.newBuilder()
            .maximumSize(25)
            .removalListener(this::onCacheEviction)
            .build();

    public MetricsSqlQueryService(ComponentLog logger, final int defaultPrecision, final int defaultScale) {
        this.defaultPrecision = defaultPrecision;
        this.defaultScale = defaultScale;
        try {
            DriverManager.registerDriver(new org.apache.calcite.jdbc.Driver());
        } catch (final SQLException e) {
            throw new ProcessException("Failed to load Calcite JDBC Driver", e);
        }

        this.logger = logger;
    }

    public ComponentLog getLogger() {
        return logger;
    }

    public QueryResult query(final ReportingContext context, final String sql)
            throws Exception {

        final Supplier<CachedStatement> statementBuilder = () -> {
            try {
                return buildCachedStatement(sql, context);
            } catch(Exception e) {
                throw new PreparedStatementException(e);
            }
        };

        final CachedStatement cachedStatement = getStatement(sql, statementBuilder, statementQueues);
        final PreparedStatement stmt = cachedStatement.getStatement();
        final ResultSet rs = stmt.executeQuery();

        return new QueryResult() {
            @Override
            public void close() throws IOException {
                final BlockingQueue<CachedStatement> statementQueue = statementQueues.getIfPresent(sql);
                if (statementQueue == null || !statementQueue.offer(cachedStatement)) {
                    try {
                        cachedStatement.getConnection().close();
                    } catch (SQLException e) {
                        throw new IOException("Failed to close statement", e);
                    }
                }
            }

            @Override
            public ResultSet getResultSet() {
                return rs;
            }

            @Override
            public int getRecordsRead() {
                return 0;
            }

        };
    }

    public ResultSetRecordSet getResultSetRecordSet(QueryResult queryResult) throws Exception{

        final ResultSet rs = queryResult.getResultSet();
        ResultSetRecordSet recordSet = null;
        // Create the RecordSchema from the ResultSet (same way ExecuteSQL does it)
        final RecordSchema writerSchema = AvroTypeUtil.createSchema(JdbcCommon.createSchema(rs));

        try {
            recordSet = new ResultSetRecordSet(rs, writerSchema, defaultPrecision, defaultScale);
        } catch (final SQLException e) {
            getLogger().error("Error creating record set from query results due to {}", e.getMessage(), e);
        }

        return recordSet;
    }

    private synchronized CachedStatement getStatement(final String sql, final Supplier<CachedStatement> statementBuilder, Cache<String, BlockingQueue<CachedStatement>> statementQueues) {
        final BlockingQueue<CachedStatement> statementQueue = statementQueues.get(sql, key -> new LinkedBlockingQueue<>());

        if(statementQueue != null) {
            final CachedStatement cachedStmt = statementQueue.poll();
            if (cachedStmt != null) {
                return cachedStmt;
            }
        }

        return statementBuilder.get();
    }

    private CachedStatement buildCachedStatement(final String sql, final ReportingContext context) throws Exception {

        final CalciteConnection connection = createConnection();
        final SchemaPlus rootSchema = createRootSchema(connection);

        final ConnectionStatusTable connectionStatusTable = new ConnectionStatusTable(context, getLogger());
        rootSchema.add("CONNECTION_STATUS", connectionStatusTable);
        if (context.isAnalyticsEnabled()) {
            final ConnectionStatusPredictionsTable connectionStatusPredictionsTable = new ConnectionStatusPredictionsTable(context, getLogger());
            rootSchema.add("CONNECTION_STATUS_PREDICTIONS", connectionStatusPredictionsTable);
        } else {
            getLogger().debug("Analytics is not enabled, CONNECTION_STATUS_PREDICTIONS table is not available for querying");
        }
        final ProcessorStatusTable processorStatusTable = new ProcessorStatusTable(context, getLogger());
        rootSchema.add("PROCESSOR_STATUS", processorStatusTable);
        final ProcessGroupStatusTable processGroupStatusTable = new ProcessGroupStatusTable(context, getLogger());
        rootSchema.add("PROCESS_GROUP_STATUS", processGroupStatusTable);
        final JvmMetricsTable jvmMetricsTable = new JvmMetricsTable(context, getLogger());
        rootSchema.add("JVM_METRICS", jvmMetricsTable);
        final BulletinTable bulletinTable = new BulletinTable(context, getLogger());
        rootSchema.add("BULLETINS", bulletinTable);
        final ProvenanceTable provenanceTable = new ProvenanceTable(context, getLogger());
        rootSchema.add("PROVENANCE", provenanceTable);
        final FlowConfigHistoryTable flowConfigHistoryTable = new FlowConfigHistoryTable(context, getLogger());
        rootSchema.add("FLOW_CONFIG_HISTORY", flowConfigHistoryTable);

        rootSchema.setCacheEnabled(false);

        final PreparedStatement stmt = connection.prepareStatement(sql);
        return new CachedStatement(stmt, connection);
    }

    private SchemaPlus createRootSchema(final CalciteConnection calciteConnection) {
        final SchemaPlus rootSchema = calciteConnection.getRootSchema();
        // Add any custom functions here
        return rootSchema;
    }

    private CalciteConnection createConnection() {
        final Properties properties = new Properties();
        properties.put(CalciteConnectionProperty.LEX.camelName(), Lex.MYSQL_ANSI.name());

        try {
            final Connection connection = DriverManager.getConnection("jdbc:calcite:", properties);
            return connection.unwrap(CalciteConnection.class);
        } catch (final Exception e) {
            throw new ProcessException(e);
        }
    }

    private void clearQueue(final BlockingQueue<CachedStatement> statementQueue) {
        CachedStatement stmt;
        while ((stmt = statementQueue.poll()) != null) {
            closeQuietly(stmt.getStatement(), stmt.getConnection());
        }
    }

    public void closeQuietly(final AutoCloseable... closeables) {
        if (closeables == null) {
            return;
        }

        for (final AutoCloseable closeable : closeables) {
            if (closeable == null) {
                continue;
            }

            try {
                closeable.close();
            } catch (final Exception e) {
                getLogger().warn("Failed to close SQL resource", e);
            }
        }
    }

    private void onCacheEviction(final String key, final BlockingQueue<CachedStatement> queue, final RemovalCause cause) {
        clearQueue(queue);
    }

    private class PreparedStatementException extends RuntimeException {

        public PreparedStatementException() {
            super();
        }

        public PreparedStatementException(String message) {
            super(message);
        }

        public PreparedStatementException(String message, Throwable cause) {
            super(message, cause);
        }

        public PreparedStatementException(Throwable cause) {
            super(cause);
        }

        public PreparedStatementException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
            super(message, cause, enableSuppression, writableStackTrace);
        }
    }
}
