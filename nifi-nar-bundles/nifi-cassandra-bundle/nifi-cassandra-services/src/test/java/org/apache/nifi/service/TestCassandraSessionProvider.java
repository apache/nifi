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
package org.apache.nifi.service;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestCassandraSessionProvider {

    private static TestRunner runner;
    private static CassandraSessionProvider sessionProvider;

    @BeforeClass
    public static void setup() throws InitializationException {
        MockCassandraProcessor mockCassandraProcessor = new MockCassandraProcessor();
        sessionProvider = new CassandraSessionProvider();

        runner = TestRunners.newTestRunner(mockCassandraProcessor);
        runner.addControllerService("cassandra-session-provider", sessionProvider);
    }

    @Test
    public void testGetPropertyDescriptors() {
        List<PropertyDescriptor> properties = sessionProvider.getPropertyDescriptors();

        assertEquals(7, properties.size());
        assertTrue(properties.contains(CassandraSessionProvider.CLIENT_AUTH));
        assertTrue(properties.contains(CassandraSessionProvider.CONSISTENCY_LEVEL));
        assertTrue(properties.contains(CassandraSessionProvider.CONTACT_POINTS));
        assertTrue(properties.contains(CassandraSessionProvider.KEYSPACE));
        assertTrue(properties.contains(CassandraSessionProvider.PASSWORD));
        assertTrue(properties.contains(CassandraSessionProvider.PROP_SSL_CONTEXT_SERVICE));
        assertTrue(properties.contains(CassandraSessionProvider.USERNAME));
    }

}
