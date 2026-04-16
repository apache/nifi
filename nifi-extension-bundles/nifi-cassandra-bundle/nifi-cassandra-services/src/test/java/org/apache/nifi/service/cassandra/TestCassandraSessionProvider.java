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
package org.apache.nifi.service.cassandra;

import org.apache.nifi.cassandra.CassandraClient;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class TestCassandraSessionProvider {

    private static final String SERVICE_ID = "cassandra-session-provider";

    private static TestRunner runner;
    private static CassandraSessionProvider sessionProvider;
    private static CassandraClient mockClient;

    @BeforeEach
    void setup() throws InitializationException {
        sessionProvider = spy(new CassandraSessionProvider());
        doNothing().when(sessionProvider).connectToCassandra(any(ConfigurationContext.class));
        mockClient = mock(CassandraClient.class);
        doReturn(mockClient).when(sessionProvider).getClient();

        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        runner.setValidateExpressionUsage(false);
        runner.addControllerService(SERVICE_ID, sessionProvider);
    }

    @Test
    void testGetPropertyDescriptors() {
        List<PropertyDescriptor> properties = sessionProvider.getPropertyDescriptors();

        assertEquals(10, properties.size(), "Property count mismatch");
        assertTrue(properties.contains(CassandraSessionProvider.CONSISTENCY_LEVEL));
        assertTrue(properties.contains(CassandraSessionProvider.CONTACT_POINTS));
        assertTrue(properties.contains(CassandraSessionProvider.KEYSPACE));
        assertTrue(properties.contains(CassandraSessionProvider.PASSWORD));
        assertTrue(properties.contains(CassandraSessionProvider.PROP_SSL_CONTEXT_SERVICE));
        assertTrue(properties.contains(CassandraSessionProvider.USERNAME));
        assertTrue(properties.contains(CassandraSessionProvider.LOCAL_DATACENTER));
    }

    @Test
    void testDefaultsBeforeEnabling() {
        assertNotNull(sessionProvider.getClient(), "Client should return mocked client");
    }

    @Test
    void testEnableServiceWithValidProperties() throws InitializationException {

        runner.setProperty(sessionProvider, CassandraSessionProvider.CONTACT_POINTS, "localhost:9042");
        runner.setProperty(sessionProvider, CassandraSessionProvider.LOCAL_DATACENTER, "datacenter1");
        runner.setProperty(sessionProvider, CassandraSessionProvider.CONSISTENCY_LEVEL, "QUORUM");

        runner.enableControllerService(sessionProvider);
        runner.assertValid(sessionProvider);
        assertNotNull(sessionProvider.getClient(), "Cassandra client should be mocked after enabling service");
    }

    @Test
    void testEnableServiceMissingRequiredProperties() {
        runner.setProperty(sessionProvider, CassandraSessionProvider.CONTACT_POINTS, "localhost:9042");
        runner.assertNotValid(sessionProvider);
    }

    @Test
    void testConnectToCassandraIsCalledOnEnable() throws InitializationException {
        runner.setProperty(sessionProvider, CassandraSessionProvider.CONTACT_POINTS, "localhost:9042");
        runner.setProperty(sessionProvider, CassandraSessionProvider.LOCAL_DATACENTER, "datacenter1");
        runner.enableControllerService(sessionProvider);

        verify(sessionProvider, times(1)).connectToCassandra(any(ConfigurationContext.class));
    }

}
