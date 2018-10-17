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
package org.apache.nifi.processors.standard;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import java.util.Collections;

import static org.apache.nifi.processors.standard.RetryCount.REL_OVER_LIMIT;
import static org.apache.nifi.processors.standard.RetryCount.REL_RETRY;
import static org.apache.nifi.processors.standard.RetryCount.RETRY_COUNT_ATTRIBUTE_KEY;
import static junit.framework.Assert.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;


public class TestRetryCount {

    @Test
    public void testOnceDefault() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new RetryCount());
        testRunner.assertValid();

        testRunner.enqueue("t".getBytes());
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(REL_RETRY, 1);
        MockFlowFile mockFlowFile = testRunner.getFlowFilesForRelationship(REL_RETRY).get(0);

        mockFlowFile.assertAttributeEquals(RETRY_COUNT_ATTRIBUTE_KEY, "1");
        assertTrue(mockFlowFile.isPenalized());
    }

    @Test
    public void testOverDefaultRetryLimit() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new RetryCount());
        testRunner.assertValid();

        testRunner.enqueue("t".getBytes(), Collections.singletonMap(RETRY_COUNT_ATTRIBUTE_KEY, "3"));
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(REL_OVER_LIMIT, 1);
        MockFlowFile mockFlowFile = testRunner.getFlowFilesForRelationship(REL_OVER_LIMIT).get(0);

        mockFlowFile.assertAttributeEquals(RETRY_COUNT_ATTRIBUTE_KEY, "3");
        assertFalse(mockFlowFile.isPenalized());
        assertEquals(1, testRunner.getLogger().getWarnMessages().size());
    }

    @Test
    public void testOverInputQueueCountLimit() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new RetryCount());
        testRunner.setProperty(RetryCount.PROP_INPUT_QUEUE_COUNT_LIMIT, "1");
        testRunner.assertValid();

        testRunner.enqueue("1".getBytes());
        testRunner.enqueue("2".getBytes());
        testRunner.enqueue("3".getBytes());
        testRunner.run(2);

        testRunner.assertTransferCount(REL_RETRY, 1);
        testRunner.assertTransferCount(REL_OVER_LIMIT, 1);
        MockFlowFile mockFlowFileOverLimit = testRunner.getFlowFilesForRelationship(REL_OVER_LIMIT).get(0);

        mockFlowFileOverLimit.assertAttributeNotExists(RETRY_COUNT_ATTRIBUTE_KEY);
        assertFalse(mockFlowFileOverLimit.isPenalized());
        assertEquals(1, testRunner.getLogger().getErrorMessages().size());
        String overlimitContent = new String(testRunner.getContentAsByteArray(mockFlowFileOverLimit));

        assertEquals("1", overlimitContent);

        MockFlowFile mockFlowFileRetry = testRunner.getFlowFilesForRelationship(REL_RETRY).get(0);

        mockFlowFileRetry.assertAttributeEquals(RETRY_COUNT_ATTRIBUTE_KEY, "1");
        assertTrue(mockFlowFileRetry.isPenalized());
        assertEquals(1, testRunner.getLogger().getErrorMessages().size());
        String retryContent = new String(testRunner.getContentAsByteArray(mockFlowFileRetry));

        assertEquals("2", retryContent);
    }

    @Test
    public void testOverInputDataSizeLimit() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new RetryCount());
        testRunner.setProperty(RetryCount.PROP_INPUT_QUEUE_DATA_SIZE_LIMIT, "1 B");
        testRunner.assertValid();

        testRunner.enqueue("1".getBytes());
        testRunner.enqueue("2".getBytes());
        testRunner.enqueue("3".getBytes());
        testRunner.run(2);

        testRunner.assertTransferCount(REL_RETRY, 1);
        testRunner.assertTransferCount(REL_OVER_LIMIT, 1);
        MockFlowFile mockFlowFileOverLimit = testRunner.getFlowFilesForRelationship(REL_OVER_LIMIT).get(0);

        mockFlowFileOverLimit.assertAttributeNotExists(RETRY_COUNT_ATTRIBUTE_KEY);
        assertFalse(mockFlowFileOverLimit.isPenalized());
        assertEquals(1, testRunner.getLogger().getErrorMessages().size());
        String overlimitContent = new String(testRunner.getContentAsByteArray(mockFlowFileOverLimit));

        assertEquals("1", overlimitContent);

        MockFlowFile mockFlowFileRetry = testRunner.getFlowFilesForRelationship(REL_RETRY).get(0);

        mockFlowFileRetry.assertAttributeEquals(RETRY_COUNT_ATTRIBUTE_KEY, "1");
        assertTrue(mockFlowFileRetry.isPenalized());
        assertEquals(1, testRunner.getLogger().getErrorMessages().size());
        String retryContent = new String(testRunner.getContentAsByteArray(mockFlowFileRetry));

        assertEquals("2", retryContent);
    }

    @Test
    public void testFullCycle() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new RetryCount());
        testRunner.setProperty(RetryCount.PROP_RETRY_LIMIT, "${literal('1')}");
        testRunner.setProperty(RetryCount.PROP_PENALIZE_FLOWFILE, "false");
        testRunner.setProperty(RetryCount.PROP_WARN_ON_OVER_LIMIT, "false");
        testRunner.assertValid();

        testRunner.enqueue("t".getBytes());
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(REL_RETRY, 1);
        MockFlowFile first = testRunner.getFlowFilesForRelationship(REL_RETRY).get(0);

        first.assertAttributeEquals(RETRY_COUNT_ATTRIBUTE_KEY, "1");
        assertFalse(first.isPenalized());

        testRunner.clearTransferState();
        testRunner.enqueue(first);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(REL_OVER_LIMIT, 1);
        MockFlowFile second = testRunner.getFlowFilesForRelationship(REL_OVER_LIMIT).get(0);

        second.assertAttributeEquals(RETRY_COUNT_ATTRIBUTE_KEY, "1");
        assertEquals(0, testRunner.getLogger().getWarnMessages().size());
    }
}

