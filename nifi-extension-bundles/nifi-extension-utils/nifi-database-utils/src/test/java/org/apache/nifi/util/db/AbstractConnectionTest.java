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
package org.apache.nifi.util.db;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

abstract class AbstractConnectionTest {
    private static final String DRIVER_CLASS = "org.hsqldb.jdbc.JDBCDriver";
    private static final String CONNECTION_URL_FORMAT = "jdbc:hsqldb:file:%s";

    private static String connectionUrl;

    @BeforeAll
    static void setConnectionUrl(@TempDir final Path tempDir) {
        try {
            Class.forName(DRIVER_CLASS);
        } catch (final ClassNotFoundException e) {
            throw new IllegalStateException("Driver Class [%s] not found".formatted(DRIVER_CLASS), e);
        }
        connectionUrl = CONNECTION_URL_FORMAT.formatted(tempDir);
    }

    /**
     * Get SQL Connection from initialized temporary database location
     *
     * @return SQL Connection
     * @throws SQLException Thrown on connection retrieval failures
     */
    protected Connection getConnection() throws SQLException {
        return DriverManager.getConnection(connectionUrl);
    }
}
