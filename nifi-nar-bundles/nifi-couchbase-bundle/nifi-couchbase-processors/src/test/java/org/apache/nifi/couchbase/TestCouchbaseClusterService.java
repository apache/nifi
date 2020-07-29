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
package org.apache.nifi.couchbase;

import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class TestCouchbaseClusterService {

    private static final String SERVICE_ID = "couchbaseClusterService";
    private TestRunner testRunner;

    public static class SampleProcessor extends AbstractProcessor {
        @Override
        public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        }
    }

    @Before
    public void init() throws Exception {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi.processors.couchbase.PutCouchbaseKey", "debug");
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi.couchbase.CouchbaseClusterService", "debug");
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi.couchbase.TestCouchbaseClusterService", "debug");

        testRunner = TestRunners.newTestRunner(SampleProcessor.class);
        testRunner.setValidateExpressionUsage(false);
    }

    @Test
    public void testConnectionFailure() throws InitializationException {
        String connectionString = "invalid-protocol://invalid-hostname";
        CouchbaseClusterControllerService service = new CouchbaseClusterService();
        testRunner.addControllerService(SERVICE_ID, service);
        testRunner.setProperty(service, CouchbaseClusterService.CONNECTION_STRING, connectionString);
        try {
            testRunner.enableControllerService(service);
            Assert.fail("The service shouldn't be enabled when it couldn't connect to a cluster.");
        } catch (AssertionError e) {
        }
    }

}
