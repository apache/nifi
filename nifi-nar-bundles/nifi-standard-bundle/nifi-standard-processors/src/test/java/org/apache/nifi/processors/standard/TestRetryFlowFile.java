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

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class TestRetryFlowFile {
    TestRunner runner;

    @Before
    public void before() {
        runner = TestRunners.newTestRunner(new RetryFlowFile());
    }

    @After
    public void after() {
        runner.shutdown();
    }

    @Test
    public void testNoRetryAttribute() {
        runner.enqueue("");
        runner.run();

        runner.assertTransferCount(RetryFlowFile.RETRY, 1);
        runner.assertTransferCount(RetryFlowFile.RETRIES_EXCEEDED, 0);
        runner.assertTransferCount(RetryFlowFile.FAILURE, 0);

        runner.assertAllConditionsMet(RetryFlowFile.RETRY, mff -> {
            mff.assertAttributeExists("flowfile.retries");
            mff.assertAttributeExists("flowfile.retries.uuid");
            mff.assertAttributeEquals("flowfile.retries", "1");
            return true;
        });
    }

    @Test
    public void testRetryPenalize() {
        runner.enqueue("", Collections.singletonMap("flowfile.retries", "2"));
        runner.run();

        runner.assertTransferCount(RetryFlowFile.RETRY, 1);
        runner.assertTransferCount(RetryFlowFile.RETRIES_EXCEEDED, 0);
        runner.assertTransferCount(RetryFlowFile.FAILURE, 0);

        runner.assertAllConditionsMet(RetryFlowFile.RETRY, mff -> {
            mff.assertAttributeExists("flowfile.retries");
            mff.assertAttributeExists("flowfile.retries.uuid");
            mff.assertAttributeEquals("flowfile.retries", "3");
            Assert.assertTrue("FlowFile was not penalized!", mff.isPenalized());
            return true;
        });
    }

    @Test
    public void testRetryClustered() {
        runner.setClustered(true);
        runner.setThreadCount(5);
        for (int i = 0; i < 5; i++) {
            runner.enqueue("", Collections.singletonMap("flowfile.retries", "2"));
        }
        runner.run(5);

        runner.assertTransferCount(RetryFlowFile.RETRY, 5);
        runner.assertTransferCount(RetryFlowFile.RETRIES_EXCEEDED, 0);
        runner.assertTransferCount(RetryFlowFile.FAILURE, 0);

        runner.assertAllConditionsMet(RetryFlowFile.RETRY, mff -> {
            mff.assertAttributeExists("flowfile.retries");
            mff.assertAttributeExists("flowfile.retries.uuid");
            mff.assertAttributeEquals("flowfile.retries", "3");
            Assert.assertTrue("FlowFile was not penalized!", mff.isPenalized());
            return true;
        });
    }

    @Test
    public void testRetryNoPenalize() {
        runner.setProperty(RetryFlowFile.PENALIZE_RETRIED, "false");
        runner.enqueue("", Collections.singletonMap("flowfile.retries", "2"));
        runner.run();

        runner.assertTransferCount(RetryFlowFile.RETRY, 1);
        runner.assertTransferCount(RetryFlowFile.RETRIES_EXCEEDED, 0);
        runner.assertTransferCount(RetryFlowFile.FAILURE, 0);

        runner.assertAllConditionsMet(RetryFlowFile.RETRY, mff -> {
            mff.assertAttributeExists("flowfile.retries.uuid");
            mff.assertAttributeExists("flowfile.retries");
            mff.assertAttributeEquals("flowfile.retries", "3");
            Assert.assertFalse("FlowFile was not penalized!", mff.isPenalized());
            return true;
        });
    }

    @Test
    public void testNoFailOnOverwrite() {
        runner.enqueue("", Collections.singletonMap("flowfile.retries", "ZZAaa"));
        runner.run();

        runner.assertTransferCount(RetryFlowFile.RETRY, 1);
        runner.assertTransferCount(RetryFlowFile.RETRIES_EXCEEDED, 0);
        runner.assertTransferCount(RetryFlowFile.FAILURE, 0);

        runner.assertAllConditionsMet(RetryFlowFile.RETRY, mff -> {
            mff.assertAttributeExists("flowfile.retries.uuid");
            mff.assertAttributeExists("flowfile.retries");
            mff.assertAttributeEquals("flowfile.retries", "1");
            Assert.assertTrue("FlowFile was not penalized!", mff.isPenalized());
            return true;
        });
    }

    @Test
    public void testFailOnOverwrite() {
        runner.setProperty(RetryFlowFile.FAIL_ON_OVERWRITE, "true");
        runner.enqueue("", Collections.singletonMap("flowfile.retries", "ZZAaa"));
        runner.run();

        runner.assertTransferCount(RetryFlowFile.RETRY, 0);
        runner.assertTransferCount(RetryFlowFile.RETRIES_EXCEEDED, 0);
        runner.assertTransferCount(RetryFlowFile.FAILURE, 1);
    }

    @Test
    public void testRetriesExceeded() {
        runner.setProperty("exceeded.time", "${now():toString()}");
        runner.setProperty("reason", "${uuid} exceeded retries");
        runner.enqueue("", Collections.singletonMap("flowfile.retries", "3"));
        runner.run();

        runner.assertTransferCount(RetryFlowFile.RETRY, 0);
        runner.assertTransferCount(RetryFlowFile.RETRIES_EXCEEDED, 1);
        runner.assertTransferCount(RetryFlowFile.FAILURE, 0);

        runner.assertAllConditionsMet(RetryFlowFile.RETRIES_EXCEEDED, mff -> {
            mff.assertAttributeExists("exceeded.time");
            mff.assertAttributeExists("reason");
            Assert.assertFalse("Expression language not evaluated!",
                    mff.getAttribute("reason").contains("${uuid}"));
            return true;
        });
    }

    @Test
    public void testReuseFail() {
        runner.setProperty(RetryFlowFile.REUSE_MODE, RetryFlowFile.FAIL_ON_REUSE.getValue());
        Map<String, String> inputAttributes = new HashMap<>();
        inputAttributes.put("flowfile.retries", "2");
        inputAttributes.put("flowfile.retries.uuid", "1122334455");
        runner.enqueue("", inputAttributes);
        runner.run();

        runner.assertTransferCount(RetryFlowFile.RETRY, 0);
        runner.assertTransferCount(RetryFlowFile.RETRIES_EXCEEDED, 0);
        runner.assertTransferCount(RetryFlowFile.FAILURE, 1);
    }

    @Test
    public void testReuseWarn() {
        runner.setProperty(RetryFlowFile.REUSE_MODE, RetryFlowFile.WARN_ON_REUSE.getValue());
        Map<String, String> inputAttributes = new HashMap<>();
        inputAttributes.put("flowfile.retries", "2");
        inputAttributes.put("flowfile.retries.uuid", "1122334455");
        runner.enqueue("", inputAttributes);
        runner.run();

        runner.assertTransferCount(RetryFlowFile.RETRY, 1);
        runner.assertTransferCount(RetryFlowFile.RETRIES_EXCEEDED, 0);
        runner.assertTransferCount(RetryFlowFile.FAILURE, 0);

        runner.assertAllConditionsMet(RetryFlowFile.RETRY, mff -> {
            mff.assertAttributeExists("flowfile.retries");
            mff.assertAttributeEquals("flowfile.retries", "1");
            return true;
        });
    }

    @Test
    public void testReuseReset() {
        runner.setProperty(RetryFlowFile.REUSE_MODE, RetryFlowFile.RESET_ON_REUSE.getValue());
        Map<String, String> inputAttributes = new HashMap<>();
        inputAttributes.put("flowfile.retries", "2");
        inputAttributes.put("flowfile.retries.uuid", "1122334455");
        runner.enqueue("", inputAttributes);
        runner.run();

        runner.assertTransferCount(RetryFlowFile.RETRY, 1);
        runner.assertTransferCount(RetryFlowFile.RETRIES_EXCEEDED, 0);
        runner.assertTransferCount(RetryFlowFile.FAILURE, 0);

        runner.assertAllConditionsMet(RetryFlowFile.RETRY, mff -> {
            mff.assertAttributeExists("flowfile.retries");
            mff.assertAttributeEquals("flowfile.retries", "1");
            return true;
        });
    }

    @Test
    public void testAlternativeAttributeMaxRetries() {
        runner.setProperty(RetryFlowFile.MAXIMUM_RETRIES, "${retry.max}");
        Map<String, String> attributeMap = new HashMap<>();
        attributeMap.put("retry.max", "3");
        attributeMap.put("flowfile.retries", "2");
        runner.enqueue("", attributeMap);
        runner.run();

        runner.assertTransferCount(RetryFlowFile.RETRY, 1);
        runner.assertTransferCount(RetryFlowFile.RETRIES_EXCEEDED, 0);
        runner.assertTransferCount(RetryFlowFile.FAILURE, 0);

        runner.assertAllConditionsMet(RetryFlowFile.RETRY, mff -> {
            mff.assertAttributeExists("flowfile.retries");
            mff.assertAttributeExists("flowfile.retries.uuid");
            mff.assertAttributeEquals("flowfile.retries", "3");
            Assert.assertTrue("FlowFile was not penalized!", mff.isPenalized());
            return true;
        });
    }

    @Test
    public void testInvalidAlternativeAttributeMaxRetries() {
        runner.setProperty(RetryFlowFile.MAXIMUM_RETRIES, "${retry.max}");
        Map<String, String> attributeMap = new HashMap<>();
        attributeMap.put("retry.max", "NiFi");
        attributeMap.put("flowfile.retries", "2");
        runner.enqueue("", attributeMap);
        runner.run();

        runner.assertTransferCount(RetryFlowFile.RETRY, 0);
        runner.assertTransferCount(RetryFlowFile.RETRIES_EXCEEDED, 0);
        runner.assertTransferCount(RetryFlowFile.FAILURE, 1);
    }
}
