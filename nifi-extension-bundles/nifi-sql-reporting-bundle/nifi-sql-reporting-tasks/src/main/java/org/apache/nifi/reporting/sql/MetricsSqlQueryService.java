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
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.sql.datasources.BulletinDataSource;
import org.apache.nifi.reporting.sql.datasources.ConnectionStatusDataSource;
import org.apache.nifi.reporting.sql.datasources.ConnectionStatusPredictionDataSource;
import org.apache.nifi.reporting.sql.datasources.FlowConfigHistoryDataSource;
import org.apache.nifi.reporting.sql.datasources.GroupStatusCache;
import org.apache.nifi.reporting.sql.datasources.JvmMetricsDataSource;
import org.apache.nifi.reporting.sql.datasources.ProcessGroupStatusDataSource;
import org.apache.nifi.reporting.sql.datasources.ProcessorStatusDataSource;
import org.apache.nifi.reporting.sql.datasources.ProvenanceDataSource;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.ResultSetRecordSet;
import org.apache.nifi.sql.CalciteDatabase;
import org.apache.nifi.sql.NiFiTable;
import org.apache.nifi.sql.ResettableDataSource;
import org.apache.nifi.util.db.JdbcCommon;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
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
        this.logger = logger;
    }

    public ComponentLog getLogger() {
        return logger;
    }

    @Override
    public QueryResult query(final ReportingContext context, final String sql) throws Exception {
        final Supplier<CachedStatement> statementBuilder = () -> {
            try {
                return buildCachedStatement(sql, context);
            } catch (Exception e) {
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

    @Override
    public ResultSetRecordSet getResultSetRecordSet(QueryResult queryResult) throws Exception {

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

        if (statementQueue != null) {
            final CachedStatement cachedStmt = statementQueue.poll();
            if (cachedStmt != null) {
                return cachedStmt;
            }
        }

        return statementBuilder.get();
    }

    private CachedStatement buildCachedStatement(final String sql, final ReportingContext context) throws Exception {
        final CalciteDatabase database = new CalciteDatabase();

        final GroupStatusCache groupStatusCache = new GroupStatusCache(Duration.ofSeconds(60));

        final ResettableDataSource connectionStatusDataSource = new ConnectionStatusDataSource(context, groupStatusCache);
        final NiFiTable connectionStatusTable = new NiFiTable("CONNECTION_STATUS", connectionStatusDataSource, getLogger());
        database.addTable(connectionStatusTable);

        final ResettableDataSource predictionDataSource = new ConnectionStatusPredictionDataSource(context, groupStatusCache);
        final NiFiTable connectionStatusPredictionsTable = new NiFiTable("CONNECTION_STATUS_PREDICTIONS", predictionDataSource, getLogger());
        database.addTable(connectionStatusPredictionsTable);
        if (!context.isAnalyticsEnabled()) {
            getLogger().info("Analytics is not enabled, CONNECTION_STATUS_PREDICTIONS table will not contain any rows");
        }

        final ResettableDataSource processorStatusDataSource = new ProcessorStatusDataSource(context, groupStatusCache);
        final NiFiTable processorStatusTable = new NiFiTable("PROCESSOR_STATUS", processorStatusDataSource, getLogger());
        database.addTable(processorStatusTable);

        final ResettableDataSource processGroupStatusDataSource = new ProcessGroupStatusDataSource(context, groupStatusCache);
        final NiFiTable processGroupStatusTable = new NiFiTable("PROCESS_GROUP_STATUS", processGroupStatusDataSource, getLogger());
        database.addTable(processGroupStatusTable);

        final ResettableDataSource jvmMetricsDataSource = new JvmMetricsDataSource();
        final NiFiTable jvmMetricsTable = new NiFiTable("JVM_METRICS", jvmMetricsDataSource, getLogger());
        database.addTable(jvmMetricsTable);

        final ResettableDataSource bulletinDataSource = new BulletinDataSource(context);
        final NiFiTable bulletinTable = new NiFiTable("BULLETINS", bulletinDataSource, getLogger());
        database.addTable(bulletinTable);

        final ResettableDataSource provenanceDataSource = new ProvenanceDataSource(context);
        final NiFiTable provenanceTable = new NiFiTable("PROVENANCE", provenanceDataSource, getLogger());
        database.addTable(provenanceTable);

        final ResettableDataSource flowConfigHistoryDataSource = new FlowConfigHistoryDataSource(context);
        final NiFiTable flowConfigHistoryTable = new NiFiTable("FLOW_CONFIG_HISTORY", flowConfigHistoryDataSource, getLogger());
        database.addTable(flowConfigHistoryTable);

        final Connection connection = database.getConnection();
        final PreparedStatement stmt = connection.prepareStatement(sql);
        return new CachedStatement(stmt, database);
    }

    private void clearQueue(final BlockingQueue<CachedStatement> statementQueue) {
        CachedStatement stmt;
        while ((stmt = statementQueue.poll()) != null) {
            closeQuietly(stmt.getStatement(), stmt.getDatabase());
        }
    }

    @Override
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

    private static class PreparedStatementException extends RuntimeException {
        public PreparedStatementException(final Throwable cause) {
            super(cause);
        }
    }
}
