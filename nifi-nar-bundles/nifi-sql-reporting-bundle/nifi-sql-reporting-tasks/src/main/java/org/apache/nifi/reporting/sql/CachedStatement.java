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

import org.apache.nifi.sql.CalciteDatabase;

import java.sql.Connection;
import java.sql.PreparedStatement;

class CachedStatement {
    private final PreparedStatement statement;
    private final CalciteDatabase database;

    CachedStatement(final PreparedStatement statement, final CalciteDatabase database) {
        this.statement = statement;
        this.database = database;
    }

    PreparedStatement getStatement() {
        return statement;
    }

    Connection getConnection() {
        return database.getConnection();
    }

    CalciteDatabase getDatabase() {
        return database;
    }
}
