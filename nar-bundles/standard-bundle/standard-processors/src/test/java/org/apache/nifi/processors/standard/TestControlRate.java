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

import org.apache.nifi.processors.standard.ControlRate;
import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import org.junit.Test;

public class TestControlRate {

    @Test
    public void testViaAttribute() throws InterruptedException {
        final TestRunner runner = TestRunners.newTestRunner(new ControlRate());
        runner.setProperty(ControlRate.RATE_CONTROL_CRITERIA, ControlRate.ATTRIBUTE_RATE);
        runner.setProperty(ControlRate.RATE_CONTROL_ATTRIBUTE_NAME, "count");
        runner.setProperty(ControlRate.MAX_RATE, "20000");
        runner.setProperty(ControlRate.TIME_PERIOD, "1 sec");

        createFlowFile(runner, 1000);
        createFlowFile(runner, 1000);
        createFlowFile(runner, 5000);
        createFlowFile(runner, 20000);
        createFlowFile(runner, 1000);

        runner.run(4);

        runner.assertAllFlowFilesTransferred(ControlRate.REL_SUCCESS, 4);
        runner.clearTransferState();

        runner.run();
        runner.assertTransferCount(ControlRate.REL_SUCCESS, 0);
        runner.assertTransferCount(ControlRate.REL_FAILURE, 0);
        runner.assertQueueNotEmpty();

        // at this point, we have sent through 27,000 but our max is 20,000 per second. 
        // After 1 second, we should be able to send another 13,000
        Thread.sleep(1200L);
        runner.run();
        runner.assertTransferCount(ControlRate.REL_SUCCESS, 1);
        runner.assertTransferCount(ControlRate.REL_FAILURE, 0);
        runner.assertQueueEmpty();
    }

    private void createFlowFile(final TestRunner runner, final int value) {
        final Map<String, String> attributeMap = new HashMap<>();
        attributeMap.put("count", String.valueOf(value));
        runner.enqueue(new byte[0], attributeMap);
    }
}
