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

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.nifi.queryrecord.FlowFileTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;

public class SqlJoinCalciteParameters implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(SqlJoinCalciteParameters.class);

    private final String sql;
    private final CalciteConnection connection;
    private final PreparedStatement preparedStatement;
    private final FlowFileTable originalTable;
    private final FlowFileTable enrichmentTable;

    public SqlJoinCalciteParameters(final String sql, final CalciteConnection connection, final PreparedStatement preparedStatement,
                                    final FlowFileTable originalTable, final FlowFileTable enrichmentTable) {
        this.sql = sql;
        this.connection = connection;
        this.preparedStatement = preparedStatement;
        this.originalTable = originalTable;
        this.enrichmentTable = enrichmentTable;
    }

    public String getSql() {
        return sql;
    }

    public CalciteConnection getConnection() {
        return connection;
    }

    public PreparedStatement getPreparedStatement() {
        return preparedStatement;
    }

    public FlowFileTable getOriginalTable() {
        return originalTable;
    }

    public FlowFileTable getEnrichmentTable() {
        return enrichmentTable;
    }

    @Override
    public void close() {
        closeQuietly(preparedStatement, "Calcite Prepared Statement");
        closeQuietly(connection, "Calcite Connection");
        closeQuietly(originalTable, "Calcite 'Original' Table");
        closeQuietly(enrichmentTable, "Calcite 'Enrichment' Table");
    }

    private void closeQuietly(final AutoCloseable closeable, final String description) {
        if (closeable == null) {
            return;
        }

        try {
            closeable.close();
        } catch (final Exception e) {
            logger.warn("Failed to close {}", description, e);
        }
    }
}