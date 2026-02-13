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

import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.mock.connector.StandardConnectorTestRunner;
import org.apache.nifi.mock.connector.server.ConnectorTestRunner;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CreateConnectorIT {

    @Test
    public void testCreateStartAndStopGenerateAndUpdateConnector() throws IOException {
        try (final ConnectorTestRunner testRunner = new StandardConnectorTestRunner.Builder()
            .connectorClassName("org.apache.nifi.mock.connectors.GenerateAndLog")
            .narLibraryDirectory(new File("target/libDir"))
            .build()) {

            // Verify the active flow snapshot reflects the initial flow loaded from Generate_and_Update.json
            final VersionedExternalFlow activeFlow = testRunner.getActiveFlowSnapshot();
            final VersionedProcessGroup rootGroup = activeFlow.getFlowContents();

            final Set<VersionedProcessor> processors = rootGroup.getProcessors();
            assertEquals(3, processors.size());
            assertTrue(findProcessorByType(processors, "org.apache.nifi.processors.standard.GenerateFlowFile").isPresent());
            assertTrue(findProcessorByType(processors, "org.apache.nifi.processors.standard.LookupAttribute").isPresent());
            assertTrue(findProcessorByType(processors, "org.apache.nifi.processors.attributes.UpdateAttribute").isPresent());

            assertEquals(2, rootGroup.getConnections().size());

            final Set<VersionedControllerService> controllerServices = rootGroup.getControllerServices();
            assertEquals(1, controllerServices.size());
            assertEquals("org.apache.nifi.lookup.SimpleKeyValueLookupService", controllerServices.iterator().next().getType());

            testRunner.startConnector();
            testRunner.stopConnector();
        }
    }

    @Test
    public void testConnectorWithMissingBundleFailsValidate() throws IOException {

        try (final ConnectorTestRunner testRunner = new StandardConnectorTestRunner.Builder()
                .connectorClassName("org.apache.nifi.mock.connectors.MissingBundleConnector")
                .narLibraryDirectory(new File("target/libDir"))
                .build()) {

            final List<ValidationResult> results = testRunner.validate();
            assertEquals(results.size(), 1);
            final String message = results.getFirst().getExplanation();
            assertTrue(message.contains("com.example.nonexistent:missing-nar:1.0.0"), "Expected exception message to contain missing bundle coordinates but was: " + message);
            assertTrue(message.contains("com.example.nonexistent.MissingProcessor"), "Expected exception message to contain missing processor type but was: " + message);
        }
    }

    private Optional<VersionedProcessor> findProcessorByType(final Set<VersionedProcessor> processors, final String type) {
        for (final VersionedProcessor processor : processors) {
            if (type.equals(processor.getType())) {
                return Optional.of(processor);
            }
        }
        return Optional.empty();
    }
}
