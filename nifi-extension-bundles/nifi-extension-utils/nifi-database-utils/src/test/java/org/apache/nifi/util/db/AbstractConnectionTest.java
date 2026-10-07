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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.UUID;

abstract class AbstractConnectionTest {
    private static final String DRIVER_CLASS = "org.hsqldb.jdbc.JDBCDriver";
    private static final String CONNECTION_URL_FORMAT = "jdbc:hsqldb:mem:%s;shutdown=true";

    private static String connectionUrl;
    // Keeps a single connection alive to prevent the memory DB from evaporating
    private static Connection keepAliveConnection;

    @BeforeAll
    static void setConnectionUrl() throws SQLException {
        try {
            Class.forName(DRIVER_CLASS);
        } catch (final ClassNotFoundException e) {
            throw new IllegalStateException("Driver Class [%s] not found".formatted(DRIVER_CLASS), e);
        }

        // Each test instance gets its own independent database context path
        final String uniqueDbName = UUID.randomUUID().toString();
        connectionUrl = CONNECTION_URL_FORMAT.formatted(uniqueDbName);

        // Open a lease connection right away. While this remains open, connection count > 0,
        // which completely prevents HSQLDB from wiping tables prematurely.
        keepAliveConnection = DriverManager.getConnection(connectionUrl);
    }

    @AfterAll
    public static void teardownConnection() throws Exception {
        // Explicitly release the keep-alive connection when the entire test class is done
        if (keepAliveConnection != null && !keepAliveConnection.isClosed()) {
            keepAliveConnection.close();
        }
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
