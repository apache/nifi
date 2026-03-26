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

import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.mock.connector.StandardConnectorTestRunner;
import org.apache.nifi.mock.connector.server.ConnectorConfigVerificationResult;
import org.apache.nifi.mock.connector.server.ConnectorTestRunner;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests that verify fetchable allowable values are properly validated
 * during configuration verification. These tests exercise the fix for NIFI-15258,
 * where {@code StandardConnectorNode.fetchAllowableValues} incorrectly used the
 * active flow context instead of the working flow context passed as a parameter.
 *
 * <p>The {@code AllowableValuesConnector} returns allowable values only when the
 * flow context type is WORKING. If the framework incorrectly passes the ACTIVE
 * context, the connector returns an empty list, causing all values to be accepted.</p>
 */
public class AllowableValuesIT {

    private static final String CONNECTOR_CLASS = "org.apache.nifi.mock.connectors.AllowableValuesConnector";

    @Test
    public void testVerifyConfigurationRejectsInvalidAllowableValue() throws IOException {
        try (final ConnectorTestRunner runner = createRunner()) {
            final ConnectorConfigVerificationResult result = runner.verifyConfiguration("Selection", Map.of("Color", "purple"));

            final List<ConfigVerificationResult> failedResults = result.getFailedResults();
            assertFalse(failedResults.isEmpty(), "Expected at least one failed result for invalid allowable value 'purple'");
            assertEquals(1, failedResults.size());

            final String explanation = failedResults.getFirst().getExplanation();
            assertTrue(explanation.contains("allowable values"), "Expected explanation to mention allowable values but was: " + explanation);
        }
    }

    @Test
    public void testVerifyConfigurationAcceptsValidAllowableValue() throws IOException {
        try (final ConnectorTestRunner runner = createRunner()) {
            final ConnectorConfigVerificationResult result = runner.verifyConfiguration("Selection", Map.of("Color", "red"));
            result.assertNoFailures();
        }
    }

    @Test
    public void testVerifyConfigurationAcceptsAllValidAllowableValues() throws IOException {
        try (final ConnectorTestRunner runner = createRunner()) {
            for (final String validColor : List.of("red", "green", "blue")) {
                final ConnectorConfigVerificationResult result = runner.verifyConfiguration("Selection", Map.of("Color", validColor));
                result.assertNoFailures();
            }
        }
    }

    private ConnectorTestRunner createRunner() {
        return new StandardConnectorTestRunner.Builder()
            .connectorClassName(CONNECTOR_CLASS)
            .narLibraryDirectory(new File("target/libDir"))
            .build();
    }
}
