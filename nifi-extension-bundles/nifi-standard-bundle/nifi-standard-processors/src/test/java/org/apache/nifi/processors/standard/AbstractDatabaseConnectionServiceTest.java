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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.embedded.database.EmbeddedDatabaseConnectionService;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * Abstract class for embedded Database Connection Service tests with standard lifecycle methods
 */
abstract class AbstractDatabaseConnectionServiceTest {
    private static final String CONNECTION_SERVICE_ID = EmbeddedDatabaseConnectionService.class.getSimpleName();

    private static EmbeddedDatabaseConnectionService connectionService;

    @BeforeAll
    static void setConnectionService(@TempDir final Path databaseLocation) {
        connectionService = new EmbeddedDatabaseConnectionService(databaseLocation);
    }

    @AfterAll
    static void close() throws IOException {
        connectionService.close();
    }

    /**
     * Create new Test Runner using Processor Class and set Database Connection Service using Service Property Descriptor
     *
     * @param processorClass Processor Class for Test Runner
     * @return Test Runner
     * @throws InitializationException Thrown on failure to add Database Connection Service to Test Runner
     */
    TestRunner newTestRunner(final Class<? extends Processor> processorClass) throws InitializationException {
        final Processor processor = createProcessor(processorClass);
        final TestRunner runner = TestRunners.newTestRunner(processor);

        runner.addControllerService(CONNECTION_SERVICE_ID, connectionService);
        runner.enableControllerService(connectionService);

        final PropertyDescriptor databaseServicePropertyDescriptor = getDatabaseServicePropertyDescriptor(processor);
        runner.setProperty(databaseServicePropertyDescriptor, CONNECTION_SERVICE_ID);

        return runner;
    }

    DBCPService getConnectionService() {
        return connectionService;
    }

    Connection getConnection() {
        return connectionService.getConnection();
    }

    void executeSql(final String sql) throws SQLException {
        try (
                Connection connection = connectionService.getConnection();
                Statement statement = connection.createStatement()
        ) {
            statement.execute(sql);
        }
    }

    private Processor createProcessor(final Class<? extends Processor> processorClass) {
        try {
            return processorClass.getConstructor().newInstance();
        } catch (final Exception e) {
            throw new IllegalArgumentException("Processor Class instantiation failed", e);
        }
    }

    private PropertyDescriptor getDatabaseServicePropertyDescriptor(final Processor processor) {
        final List<PropertyDescriptor> propertyDescriptors = processor.getPropertyDescriptors();

        return propertyDescriptors.stream()
                .filter(propertyDescriptor -> {
                    final Class<? extends ControllerService> controllerServiceDefinition = propertyDescriptor.getControllerServiceDefinition();
                    final boolean found;

                    if (controllerServiceDefinition == null) {
                        found = false;
                    } else {
                        found = controllerServiceDefinition.isAssignableFrom(DBCPService.class);
                    }

                    return found;
                })
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("DBCPService Property Descriptor not found"));
    }
}
