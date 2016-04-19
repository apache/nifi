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
package org.apache.nifi.processors.kafka.pubsub;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessSession;
import org.apache.nifi.util.MockSessionFactory;
import org.apache.nifi.util.SharedSessionState;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

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
public class AbstractKafkaProcessorLifecycelTest {

    /*
     * The goal of this test is to validate the correctness of
     * AbstractKafkaProcessor's implementation of onTrigger() in a highly
     * concurrent environment. That is:
     * - upon processing failures (e.g., unhandled exceptions), the target Kafka
     *   resource is reset (closed and re-created)
     * - no FlowFile is unaccounted for. FlowFiles left in the queue and FlowFiles
     *   in Success relationship = testCount
     * - failed executions that did not result in the call to close/reset summed with
     *   verified calls to close should equal total request failed
     */
    @Test
    public void validateLifecycleCorrectnessWithProcessingFailures() throws Exception {

        final int testRun = 10000;
        final TestRunner runner = TestRunners.newTestRunner(PublishKafkaConditionEmulatingProcessor.class);
        runner.setProperty(PublishKafka.TOPIC, "foo");
        runner.setProperty(PublishKafka.CLIENT_ID, "foo");
        runner.setProperty(PublishKafka.KEY, "key1");
        runner.setProperty(PublishKafka.BOOTSTRAP_SERVERS, "okeydokey:1234");
        runner.setProperty(PublishKafka.MESSAGE_DEMARCATOR, "\n");


        final PublishKafkaConditionEmulatingProcessor targetProc = (PublishKafkaConditionEmulatingProcessor) runner.getProcessor();

        MockSessionFactory sf = (MockSessionFactory) runner.getProcessSessionFactory();
        Field f = MockSessionFactory.class.getDeclaredField("sharedState");
        f.setAccessible(true);
        final SharedSessionState sharedState = (SharedSessionState) f.get(sf);
        final MockProcessSession session = new MockProcessSession(sharedState, targetProc);

        ExecutorService enqueueExecutor = Executors.newSingleThreadExecutor();
        enqueueExecutor.execute(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < testRun; i++) {
                    MockFlowFile flowFile = session.importFrom(new ByteArrayInputStream(("Hello-" + i).getBytes()), session.create());
                    sharedState.getFlowFileQueue().offer(flowFile);
                }
            }
        });

        final CountDownLatch latch = new CountDownLatch(testRun);
        ExecutorService processingExecutor = Executors.newFixedThreadPool(16);
        for (int i = 0; i < testRun; i++) {
            processingExecutor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        targetProc.onTrigger(runner.getProcessContext(), runner.getProcessSessionFactory());
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }
        latch.await();
        processingExecutor.shutdown();
        processingExecutor.awaitTermination(10000, TimeUnit.MILLISECONDS);
        enqueueExecutor.shutdown();
        enqueueExecutor.awaitTermination(10000, TimeUnit.MILLISECONDS);

        targetProc.close();

        System.out.println("FF left in queue: " + sharedState.getFlowFileQueue().size());

        System.out.println("Requests failed: " + targetProc.getIntentionalFailuresCount());
        System.out.println("Resource rebuilt: " + targetProc.getKafkaResourceBuiltCount());
        System.out.println("Calls to close(): " + targetProc.getCloseRequestsCount());
        System.out.println("Failed request that did not own close() call: " + targetProc.getFailuresThatDoNotOwnCloseCount());

        List<MockFlowFile> success = runner.getFlowFilesForRelationship(PublishKafka.REL_SUCCESS);
        List<MockFlowFile> failure = runner.getFlowFilesForRelationship(PublishKafka.REL_FAILURE);
        System.out.println("SUCCESS Relationship size: " + success.size());
        System.out.println("FAILURE Relationship size: " + failure.size());

        assertEquals(0, failure.size());

        int flowFilesLeftInQueue = sharedState.getFlowFileQueue().size().getObjectCount();
        assertEquals(testRun, flowFilesLeftInQueue + success.size());
        /*
         * calls to close() == calls to build new target resource OR calls to
         * close() is 1 more then calls to build target source which could
         * happen if last request fails and no request are coming in
         */
        assertTrue(targetProc.getKafkaResourceBuiltCount().get() == targetProc.getCloseRequestsCount().get()
                || targetProc.getCloseRequestsCount().get() - targetProc.getKafkaResourceBuiltCount().get() == 1);
        assertEquals(targetProc.getIntentionalFailuresCount().get(),
                targetProc.getCloseRequestsCount().get() + targetProc.getFailuresThatDoNotOwnCloseCount().get());
    }
}
