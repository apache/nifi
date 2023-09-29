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

import org.apache.nifi.sql.CalciteDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;

public class SqlJoinCalciteParameters implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(SqlJoinCalciteParameters.class);

    private final String sql;
    private final CalciteDatabase database;
    private final PreparedStatement preparedStatement;

    public SqlJoinCalciteParameters(final String sql, final CalciteDatabase database, final PreparedStatement preparedStatement) {
        this.sql = sql;
        this.database = database;
        this.preparedStatement = preparedStatement;
    }

    public String getSql() {
        return sql;
    }

    public CalciteDatabase getDatabase() {
        return database;
    }

    public PreparedStatement getPreparedStatement() {
        return preparedStatement;
    }


    @Override
    public void close() {
        closeQuietly(preparedStatement, "Calcite Prepared Statement");
        closeQuietly(database, "Calcite Database");
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