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
package org.apache.nifi.h2.database.migration;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * Database Statement Runner handles Connection and Statement resources to run provided commands
 */
class DatabaseStatementRunner {
    private static final String USER_PROPERTY = "user";

    private static final String PASSWORD_PROPERTY = "password";

    /**
     * Run command using current H2 Database Driver
     *
     * @param url Database URL
     * @param user Database User
     * @param password Database Password
     * @param command SQL command
     * @throws SQLException Thrown on connection or statement execution failures
     */
    static void run(final DatabaseVersion databaseVersion, final String url, final String user, final String password, final String command) throws SQLException {
        final Properties properties = new Properties();
        properties.put(USER_PROPERTY, user);
        properties.put(PASSWORD_PROPERTY, password);

        final Driver driver = databaseVersion.getDriver();
        try (
                Connection connection = driver.connect(url, properties);
                Statement s = connection.createStatement()
        ) {
            s.execute(command);
        }
    }
}
