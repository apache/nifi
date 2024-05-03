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
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processors.standard.calcite.RecordPathFunctions;
import org.apache.nifi.queryrecord.RecordDataSource;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.sql.CalciteDatabase;
import org.apache.nifi.sql.NiFiTable;
import org.apache.nifi.util.Tuple;

import java.sql.PreparedStatement;
import java.sql.SQLException;
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

    public SqlJoinCalciteParameters getCalciteParameters(final String sql, final RecordSchema schema, final RecordJoinInput originalInput, final RecordJoinInput enrichmentInput)
        throws SQLException {

        final Tuple<String, RecordSchema> tuple = new Tuple<>(sql, schema);
        final BlockingQueue<SqlJoinCalciteParameters> queue = calciteParameterQueues.get(tuple, key -> new LinkedBlockingQueue<>());

        final SqlJoinCalciteParameters cachedStmt = queue.poll();
        if (cachedStmt != null) {
            return cachedStmt;
        }

        return createCalciteParameters(sql, originalInput, enrichmentInput);
    }

    public void returnCalciteParameters(final String sql, final RecordSchema schema, final SqlJoinCalciteParameters parameters) {
        final Tuple<String, RecordSchema> tuple = new Tuple<>(sql, schema);
        final BlockingQueue<SqlJoinCalciteParameters> queue = calciteParameterQueues.getIfPresent(tuple);

        if (queue == null || !queue.offer(parameters)) {
            parameters.close();
        }
    }

    private SqlJoinCalciteParameters createCalciteParameters(final String sql, final RecordJoinInput originalInput, final RecordJoinInput enrichmentInput) throws SQLException {
        final CalciteDatabase database = new CalciteDatabase();
        RecordPathFunctions.addToDatabase(database);

        final NiFiTable originalTable = new NiFiTable(SqlJoinStrategy.ORIGINAL_TABLE_NAME, RecordDataSource.createTableSchema(originalInput.getRecordSchema()), logger);
        database.addTable(originalTable);

        final NiFiTable enrichmentTable = new NiFiTable(SqlJoinStrategy.ENRICHMENT_TABLE_NAME, RecordDataSource.createTableSchema(enrichmentInput.getRecordSchema()), logger);
        database.addTable(enrichmentTable);

        final PreparedStatement preparedStatement = database.getConnection().prepareStatement(sql);
        return new SqlJoinCalciteParameters(sql, database, preparedStatement);
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
