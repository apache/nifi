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
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.lookup.StringLookupService;
import org.apache.nifi.mock.connector.StandardConnectorTestRunner;
import org.apache.nifi.mock.connector.server.ConnectorTestRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class MockControllerServiceIT {

    @Test
    @Timeout(10)
    public void testMockControllerService() throws IOException {
        try (final ConnectorTestRunner runner = new StandardConnectorTestRunner.Builder()
                .narLibraryDirectory(new File("target/libDir"))
                .connectorClassName("org.apache.nifi.mock.connectors.GenerateAndLog")
                .mockControllerService("org.apache.nifi.lookup.SimpleKeyValueLookupService", MockStringLookupService.class)
                .build()) {

            runner.startConnector();

            // Wait until MockStringLookupService.lookup is invoked at least once.
            // We use @Timeout on the test to avoid hanging indefinitely in case of failure.
            while (MockStringLookupService.lookupCounter.get() < 1) {
                Thread.yield();
            }

            assertTrue(MockStringLookupService.lookupCounter.get() >= 1);

            runner.stopConnector();
        }
    }

    public static class MockStringLookupService extends AbstractControllerService implements StringLookupService {
        private static final AtomicLong lookupCounter = new AtomicLong(0L);

        @Override
        protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
            return new PropertyDescriptor.Builder()
                    .name(propertyDescriptorName)
                    .addValidator(Validator.VALID)
                    .dynamic(true)
                    .build();
        }

        @Override
        public Optional<String> lookup(final Map<String, Object> coordinates) throws LookupFailureException {
            lookupCounter.incrementAndGet();
            return Optional.of("mock-value");
        }

        @Override
        public Set<String> getRequiredKeys() {
            return Set.of("key");
        }
    }
}
