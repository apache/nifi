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

package org.apache.nifi.processors.standard.enrichment;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.standard.calcite.RecordPathFunctions;
import org.apache.nifi.queryrecord.FlowFileTable;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.Tuple;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class SqlJoinCache implements AutoCloseable {
    private final ComponentLog logger;
    private final Cache<Tuple<String, RecordSchema>, BlockingQueue<SqlJoinCalciteParameters>> calciteParameterQueues = Caffeine.newBuilder()
        .maximumSize(25)
        .removalListener(this::onCacheEviction)
        .build();


    public SqlJoinCache(final ComponentLog logger) {
        this.logger = logger;
    }

    public SqlJoinCalciteParameters getCalciteParameters(final String sql, final ProcessSession session, final RecordSchema schema, final RecordJoinInput originalInput,
                                                          final RecordJoinInput enrichmentInput) throws SQLException {
        final Tuple<String, RecordSchema> tuple = new Tuple<>(sql, schema);
        final BlockingQueue<SqlJoinCalciteParameters> queue = calciteParameterQueues.get(tuple, key -> new LinkedBlockingQueue<>());

        final SqlJoinCalciteParameters cachedStmt = queue.poll();
        if (cachedStmt != null) {
            return cachedStmt;
        }

        return createCalciteParameters(sql, session, originalInput, enrichmentInput, queue);
    }

    public void returnCalciteParameters(final String sql, final RecordSchema schema, final SqlJoinCalciteParameters parameters) {
        final Tuple<String, RecordSchema> tuple = new Tuple<>(sql, schema);
        final BlockingQueue<SqlJoinCalciteParameters> queue = calciteParameterQueues.getIfPresent(tuple);

        if (queue == null || !queue.offer(parameters)) {
            parameters.close();
        }
    }

    private SqlJoinCalciteParameters createCalciteParameters(final String sql, final ProcessSession session, final RecordJoinInput originalInput, final RecordJoinInput enrichmentInput,
                                                             final BlockingQueue<SqlJoinCalciteParameters> parameterQueue) throws SQLException {
        final CalciteConnection connection = createCalciteConnection();

        final SchemaPlus rootSchema = RecordPathFunctions.createRootSchema(connection);

        final FlowFileTable originalTable = new FlowFileTable(session, originalInput.getFlowFile(), originalInput.getRecordSchema(), originalInput.getRecordReaderFactory(), logger);
        rootSchema.add("ORIGINAL", originalTable);

        final FlowFileTable enrichmentTable = new FlowFileTable(session, enrichmentInput.getFlowFile(), enrichmentInput.getRecordSchema(), enrichmentInput.getRecordReaderFactory(), logger);
        rootSchema.add("ENRICHMENT", enrichmentTable);

        rootSchema.setCacheEnabled(false);

        final PreparedStatement preparedStatement = connection.prepareStatement(sql);
        return new SqlJoinCalciteParameters(sql, connection, preparedStatement, originalTable, enrichmentTable);
    }

    private CalciteConnection createCalciteConnection() {
        final Properties properties = new Properties();
        properties.put(CalciteConnectionProperty.LEX.camelName(), Lex.MYSQL_ANSI.name());
        properties.put(CalciteConnectionProperty.TIME_ZONE, "UTC");

        try {
            final Connection connection = DriverManager.getConnection("jdbc:calcite:", properties);
            final CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
            return calciteConnection;
        } catch (final Exception e) {
            throw new ProcessException(e);
        }
    }

    private void onCacheEviction(final Tuple<String, RecordSchema> key, final BlockingQueue<SqlJoinCalciteParameters> queue, final RemovalCause cause) {
        clearQueue(queue);
    }

    private void clearQueue(final BlockingQueue<SqlJoinCalciteParameters> parameterQueue) {
        SqlJoinCalciteParameters parameters;
        while ((parameters = parameterQueue.poll()) != null) {
            parameters.close();
        }
    }

    @Override
    public void close() throws Exception {
        for (final BlockingQueue<SqlJoinCalciteParameters> statementQueue : calciteParameterQueues.asMap().values()) {
            clearQueue(statementQueue);
        }

        calciteParameterQueues.invalidateAll();
    }
}
