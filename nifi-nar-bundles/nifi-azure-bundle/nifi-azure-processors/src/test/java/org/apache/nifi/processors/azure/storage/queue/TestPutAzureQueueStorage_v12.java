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
package org.apache.nifi.processors.azure.storage.queue;

import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestPutAzureQueueStorage_v12 extends AbstractTestAzureQueueStorage_v12 {
    @BeforeEach
    public void setup() throws InitializationException {
        runner = TestRunners.newTestRunner(PutAzureQueueStorage_v12.class);
        setupStorageCredentialsService();
        runner.enableControllerService(credentialsService);
        runner.setProperty(PutAzureQueueStorage_v12.STORAGE_CREDENTIALS_SERVICE, CREDENTIALS_SERVICE_IDENTIFIER);
    }

    @Test
    public void testValidVisibilityTimeout() {
        runner.setProperty(PutAzureQueueStorage_v12.QUEUE, "dummyqueue");
        runner.setProperty(PutAzureQueueStorage_v12.TTL, "7 days");
        runner.setProperty(PutAzureQueueStorage_v12.VISIBILITY_TIMEOUT, "10 secs");

        ProcessContext processContext = runner.getProcessContext();
        Collection<ValidationResult> results = new HashSet<>();
        if (processContext instanceof MockProcessContext) {
            results = ((MockProcessContext) processContext).validate();
        }

        assertEquals(0, results.size());
    }

    @Test
    public void testInvalidVisibilityTimeoutZeroSecs() {
        runner.setProperty(PutAzureQueueStorage_v12.QUEUE, "dummyqueue");
        runner.setProperty(PutAzureQueueStorage_v12.TTL, "7 days");
        runner.setProperty(PutAzureQueueStorage_v12.VISIBILITY_TIMEOUT, "0 secs");

        ProcessContext processContext = runner.getProcessContext();
        Collection<ValidationResult> results = new HashSet<>();
        if (processContext instanceof MockProcessContext) {
            results = ((MockProcessContext) processContext).validate();
        }

        assertEquals(1, results.size());
        Iterator<ValidationResult> iterator = results.iterator();
        assertTrue(iterator.next().getExplanation().contains("0 secs"));
    }

    @Test
    public void testInvalidVisibilityTimeoutMoreThanSevenDays() {
        runner.setProperty(PutAzureQueueStorage_v12.QUEUE, "dummyqueue");
        runner.setProperty(PutAzureQueueStorage_v12.TTL, "7 days");
        runner.setProperty(PutAzureQueueStorage_v12.VISIBILITY_TIMEOUT, "8 days");

        ProcessContext processContext = runner.getProcessContext();
        Collection<ValidationResult> results = new HashSet<>();
        if (processContext instanceof MockProcessContext) {
            results = ((MockProcessContext) processContext).validate();
        }

        assertEquals(1, results.size());
        Iterator<ValidationResult> iterator = results.iterator();
        assertTrue(iterator.next().getExplanation().contains("7 days"));
    }

    @Test
    public void testValidTTL() {
        runner.setProperty(PutAzureQueueStorage_v12.QUEUE, "dummyqueue");
        runner.setProperty(PutAzureQueueStorage_v12.TTL, "7 days");
        runner.setProperty(PutAzureQueueStorage_v12.VISIBILITY_TIMEOUT, "30 secs");

        ProcessContext processContext = runner.getProcessContext();
        Collection<ValidationResult> results = new HashSet<>();
        if (processContext instanceof MockProcessContext) {
            results = ((MockProcessContext) processContext).validate();
        }

        assertEquals(0, results.size());
    }

    @Test
    public void testInvalidTTLZeroSecs() {
        runner.setProperty(PutAzureQueueStorage_v12.QUEUE, "dummyqueue");
        runner.setProperty(PutAzureQueueStorage_v12.TTL, "0 secs");
        runner.setProperty(PutAzureQueueStorage_v12.VISIBILITY_TIMEOUT, "30 secs");

        ProcessContext processContext = runner.getProcessContext();
        Collection<ValidationResult> results = new HashSet<>();
        if (processContext instanceof MockProcessContext) {
            results = ((MockProcessContext) processContext).validate();
        }

        assertEquals(1, results.size());
        Iterator<ValidationResult> iterator = results.iterator();
        assertTrue(iterator.next().getExplanation().contains("positive number"));
    }

    @Test
    public void testValidRequestTimeout() {
        runner.setProperty(PutAzureQueueStorage_v12.QUEUE, "dummyqueue");
        runner.setProperty(PutAzureQueueStorage_v12.TTL, "7 days");
        runner.setProperty(PutAzureQueueStorage_v12.VISIBILITY_TIMEOUT, "30 secs");
        runner.setProperty(PutAzureQueueStorage_v12.REQUEST_TIMEOUT, "15 secs");

        ProcessContext processContext = runner.getProcessContext();
        Collection<ValidationResult> results = new HashSet<>();
        if (processContext instanceof MockProcessContext) {
            results = ((MockProcessContext) processContext).validate();
        }

        assertEquals(0, results.size());
    }

    @Test
    public void testInvalidRequestTimeoutZeroSecs() {
        runner.setProperty(PutAzureQueueStorage_v12.QUEUE, "dummyqueue");
        runner.setProperty(PutAzureQueueStorage_v12.TTL, "7 days");
        runner.setProperty(PutAzureQueueStorage_v12.VISIBILITY_TIMEOUT, "30 secs");
        runner.setProperty(PutAzureQueueStorage_v12.REQUEST_TIMEOUT, "0 secs");

        ProcessContext processContext = runner.getProcessContext();
        Collection<ValidationResult> results = new HashSet<>();
        if (processContext instanceof MockProcessContext) {
            results = ((MockProcessContext) processContext).validate();
        }

        assertEquals(1, results.size());
        Iterator<ValidationResult> iterator = results.iterator();
        assertTrue(iterator.next().getExplanation().contains("0 secs"));
    }

    @Test
    public void testInvalidRequestTimeoutMoreThanThirtySecs() {
        runner.setProperty(PutAzureQueueStorage_v12.QUEUE, "dummyqueue");
        runner.setProperty(PutAzureQueueStorage_v12.TTL, "7 days");
        runner.setProperty(PutAzureQueueStorage_v12.VISIBILITY_TIMEOUT, "30 secs");
        runner.setProperty(PutAzureQueueStorage_v12.REQUEST_TIMEOUT, "31 secs");

        ProcessContext processContext = runner.getProcessContext();
        Collection<ValidationResult> results = new HashSet<>();
        if (processContext instanceof MockProcessContext) {
            results = ((MockProcessContext) processContext).validate();
        }

        assertEquals(1, results.size());
        Iterator<ValidationResult> iterator = results.iterator();
        assertTrue(iterator.next().getExplanation().contains("30 secs"));
    }
}
