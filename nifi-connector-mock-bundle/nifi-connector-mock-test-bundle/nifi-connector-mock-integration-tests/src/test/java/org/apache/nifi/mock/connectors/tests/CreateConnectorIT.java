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

package org.apache.nifi.mock.connectors.tests;

import org.apache.nifi.mock.connector.StandardConnectorTestRunner;
import org.apache.nifi.mock.connector.server.ConnectorTestRunner;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CreateConnectorIT {

    @Test
    public void testCreateStartAndStopGenerateAndUpdateConnector() throws IOException {
        try (final ConnectorTestRunner testRunner = new StandardConnectorTestRunner.Builder()
            .connectorClassName("org.apache.nifi.mock.connectors.GenerateAndLog")
            .narLibraryDirectory(new File("target/libDir"))
            .build()) {

            testRunner.startConnector();
            testRunner.stopConnector();
        }
    }

    @Test
    public void testConnectorWithMissingBundleThrowsException() {
        final IllegalStateException exception = assertThrows(IllegalStateException.class, () -> {
            new StandardConnectorTestRunner.Builder()
                .connectorClassName("org.apache.nifi.mock.connectors.MissingBundleConnector")
                .narLibraryDirectory(new File("target/libDir"))
                .build();
        });

        final String message = exception.getMessage();
        assertTrue(message.contains("com.example.nonexistent:missing-nar:1.0.0"), "Expected exception message to contain missing bundle coordinates but was: " + message);
        assertTrue(message.contains("com.example.nonexistent.MissingProcessor"), "Expected exception message to contain missing processor type but was: " + message);
    }
}
