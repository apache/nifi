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

import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Integration tests that verify CRON expression validation during configuration verification.
 * The {@code CronScheduleConnector} maps the "Trigger Schedule" connector property to a
 * CRON-driven GenerateFlowFile processor's scheduling period parameter, and verification
 * delegates to the processor's validation which checks the CRON expression.
 */
public class CronScheduleIT {

    private static final String CONNECTOR_CLASS = "org.apache.nifi.mock.connectors.CronScheduleConnector";

    @Test
    public void testValidCronExpression() throws IOException {
        try (final ConnectorTestRunner runner = createRunner()) {
            final ConnectorConfigVerificationResult result = runner.verifyConfiguration("Schedule",
                    Map.of("Trigger Schedule", "0 0 * * * *"));
            result.assertNoFailures();
        }
    }

    @Test
    public void testInvalidCronExpression() throws IOException {
        try (final ConnectorTestRunner runner = createRunner()) {
            final ConnectorConfigVerificationResult result = runner.verifyConfiguration("Schedule",
                    Map.of("Trigger Schedule", "invalid-cron"));
            final List<ConfigVerificationResult> failedResults = result.getFailedResults();
            assertFalse(failedResults.isEmpty());
        }
    }

    private ConnectorTestRunner createRunner() {
        return new StandardConnectorTestRunner.Builder()
                .connectorClassName(CONNECTOR_CLASS)
                .narLibraryDirectory(new File("target/libDir"))
                .build();
    }
}
