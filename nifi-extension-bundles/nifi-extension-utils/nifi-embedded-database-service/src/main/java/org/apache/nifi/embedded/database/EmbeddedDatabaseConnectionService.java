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
package org.apache.nifi.embedded.database;

import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.dbcp.DBCPService;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;
import java.util.UUID;

public class EmbeddedDatabaseConnectionService extends AbstractControllerService implements DBCPService, Closeable {

    // Disable Lock File and Log creation to avoid issues with temporary directories
    private static final String CONNECTION_URL_FORMAT = "jdbc:hsqldb:file:%s;hsqldb.lock_file=false;hsqldb.log_data=false;hsqldb.log_size=0";

    private static final String DRIVER_CLASS = "org.hsqldb.jdbc.JDBCDriver";

    private static final String SHUTDOWN_COMMAND = "SHUTDOWN";

    private final String connectionUrl;

    public EmbeddedDatabaseConnectionService(final Path databaseLocation) {
        Objects.requireNonNull(databaseLocation, "Database Location required");

        final String randomDirectory = UUID.randomUUID().toString();
        final Path serviceDirectory = databaseLocation.resolve(randomDirectory).toAbsolutePath();

        connectionUrl = CONNECTION_URL_FORMAT.formatted(serviceDirectory);

        try {
            Class.forName(DRIVER_CLASS);
        } catch (final ClassNotFoundException e) {
            throw new IllegalStateException("JDBC Driver [%s] not found".formatted(DRIVER_CLASS), e);
        }
    }

    @Override
    public Connection getConnection() {
        try {
            return DriverManager.getConnection(connectionUrl);
        } catch (final SQLException e) {
            throw new IllegalStateException("Connection retrieval failed [%s]".formatted(connectionUrl), e);
        }
    }

    /**
     * Run SHUTDOWN command to close file resources
     *
     * @throws IOException Thrown on shutdown failures
     */
    @OnDisabled
    @Override
    public void close() throws IOException {
        try (
                Connection connection = getConnection();
                Statement statement = connection.createStatement()
        ) {
            statement.execute(SHUTDOWN_COMMAND);
        } catch (final SQLException e) {
            throw new IOException("Shutdown command failed", e);
        }
    }
}
