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
package org.apache.nifi.processors.standard;

import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.dbcp.DBCPService;

import java.io.Closeable;
import java.io.OutputStream;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Objects;
import java.util.UUID;

public class EmbeddedDatabaseConnectionService extends AbstractControllerService implements DBCPService, Closeable {

    private final String connectionUrl;

    private final String shutdownUrl;

    // Error Field for ignoring Derby logs
    public static final OutputStream ERROR_FIELD;

    static {
        ERROR_FIELD = new OutputStream() {
            @Override
            public void write(final int character) {

            }
        };

        System.setProperty("derby.stream.error.field", "org.apache.nifi.processors.standard.EmbeddedDatabaseConnectionService.ERROR_FIELD");
    }

    public EmbeddedDatabaseConnectionService(final Path databaseLocation) {
        Objects.requireNonNull(databaseLocation, "Database Location required");

        final Path serviceDirectory = databaseLocation.resolve(UUID.randomUUID().toString());
        connectionUrl = "jdbc:derby:%s;create=true".formatted(serviceDirectory);
        shutdownUrl = "jdbc:derby:%s;shutdown=true".formatted(serviceDirectory);

        try {
            Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
        } catch (final ClassNotFoundException e) {
            throw new IllegalStateException("Apache Derby EmbeddedDriver not found", e);
        }
    }

    @Override
    public Connection getConnection() {
        try {
            return DriverManager.getConnection(connectionUrl);
        } catch (final SQLException e) {
            throw new IllegalStateException("Apache Derby Connection failed [%s]".formatted(connectionUrl), e);
        }
    }

    @OnDisabled
    @Override
    public void close() {
        try {
            DriverManager.getConnection(shutdownUrl);
        } catch (final SQLException ignored) {

        }
    }
}
