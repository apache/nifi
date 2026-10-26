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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.mock.connector.StandardConnectorTestRunner;
import org.apache.nifi.mock.connector.server.ConnectorTestRunner;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.metrics.CommitTiming;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class MockProcessorIT {
    private static final String COUNTER_NAME = "counter";
    private static final String GAUGE_NAME = "gauge";
    private static final Map<String, String> METRIC_ATTRIBUTES = Map.of("key", "value");

    @TempDir
    private Path temporaryDirectory;

    @Test
    @Timeout(10)
    public void testMockProcessor() throws IOException {
        MockProcessor.invocationCounter.set(0L);
        MockProcessor.metricsRecorded.set(false);

        try (final ConnectorTestRunner runner = new StandardConnectorTestRunner.Builder()
            .narLibraryDirectory(new File("target/libDir"))
            .connectorClassName("org.apache.nifi.mock.connectors.GenerateAndLog")
            .mockProcessor("org.apache.nifi.processors.attributes.UpdateAttribute", MockProcessor.class)
            .instanceDirectory(temporaryDirectory.toFile())
            .build()) {

            runner.startConnector();

            // Wait until MockProcessor is triggered at least once. We use @Timeout on the test to avoid
            // hanging indefinitely in case of failure.
            while (MockProcessor.invocationCounter.get() < 1) {
                Thread.yield();
            }
            runner.stopConnector();

            final VersionedProcessor processor = runner.getActiveFlowSnapshot().getFlowContents().getProcessors().stream()
                .filter(versionedProcessor -> "UpdateAttribute".equals(versionedProcessor.getName()))
                .findFirst()
                .orElseThrow();
            final String componentId = processor.getInstanceIdentifier();

            assertEquals(5L, runner.getCounterValue(componentId, COUNTER_NAME));
            assertEquals(2L, runner.getCounterValue(componentId, COUNTER_NAME, Map.of()));
            assertEquals(3L, runner.getCounterValue(componentId, COUNTER_NAME, METRIC_ATTRIBUTES));
            assertNull(runner.getCounterValue(componentId, "unrecorded"));
            assertEquals(List.of(2.0, 3.0), runner.getGaugeValues(componentId, GAUGE_NAME));
            assertEquals(List.of(2.0), runner.getGaugeValues(componentId, GAUGE_NAME, Map.of()));
            assertEquals(List.of(3.0), runner.getGaugeValues(componentId, GAUGE_NAME, METRIC_ATTRIBUTES));
        }
    }

    public static class MockProcessor extends AbstractProcessor {
        private static final AtomicLong invocationCounter = new AtomicLong(0L);
        private static final AtomicBoolean metricsRecorded = new AtomicBoolean();

        public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .build();

        @Override
        public Set<Relationship> getRelationships() {
            return Set.of(REL_SUCCESS);
        }

        // Support any properties so that properties of UpdateAttribute can be set without the Processor becoming invalid
        @Override
        protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
            return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .addValidator(Validator.VALID)
                .build();
        }

        @Override
        public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
            invocationCounter.incrementAndGet();

            if (metricsRecorded.compareAndSet(false, true)) {
                session.adjustCounter(COUNTER_NAME, 2, Map.of(), CommitTiming.NOW);
                session.adjustCounter(COUNTER_NAME, 3, METRIC_ATTRIBUTES, CommitTiming.NOW);
                session.recordGauge(GAUGE_NAME, 2.0, Map.of(), CommitTiming.NOW);
                session.recordGauge(GAUGE_NAME, 3.0, METRIC_ATTRIBUTES, CommitTiming.NOW);
            }

            final FlowFile flowFile = session.get();
            if (flowFile != null) {
                session.transfer(flowFile, REL_SUCCESS);
            }
        }
    }
}
