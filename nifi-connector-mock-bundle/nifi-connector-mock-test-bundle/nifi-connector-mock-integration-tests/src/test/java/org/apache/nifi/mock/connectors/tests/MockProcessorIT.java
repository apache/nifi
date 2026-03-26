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
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.mock.connector.StandardConnectorTestRunner;
import org.apache.nifi.mock.connector.server.ConnectorTestRunner;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class MockProcessorIT {

    @Test
    @Timeout(10)
    public void testMockProcessor() throws IOException {
        try (final ConnectorTestRunner runner = new StandardConnectorTestRunner.Builder()
            .narLibraryDirectory(new File("target/libDir"))
            .connectorClassName("org.apache.nifi.mock.connectors.GenerateAndLog")
            .mockProcessor("org.apache.nifi.processors.attributes.UpdateAttribute", MockProcessor.class)
            .build()) {

            runner.startConnector();

            // Wait until MockProcessor is triggered at least once. We use @Timeout on the test to avoid
            // hanging indefinitely in case of failure.
            while (MockProcessor.invocationCounter.get() < 1) {
                Thread.yield();
            }
            runner.stopConnector();
        }
    }

    public static class MockProcessor extends AbstractProcessor {
        private static final AtomicLong invocationCounter = new AtomicLong(0L);

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

            final FlowFile flowFile = session.get();
            if (flowFile != null) {
                session.transfer(flowFile, REL_SUCCESS);
            }
        }
    }
}
