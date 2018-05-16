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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.stream.IntStream;

import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.standard.TestNotify.MockCacheClient;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

public class TestWait {

    private TestRunner runner;
    private MockCacheClient service;

    @Before
    public void setup() throws InitializationException {
        runner = TestRunners.newTestRunner(Wait.class);

        service = new MockCacheClient();
        runner.addControllerService("service", service);
        runner.enableControllerService(service);
        runner.setProperty(Wait.DISTRIBUTED_CACHE_SERVICE, "service");
    }

    @Test
    public void testWait() throws InitializationException {
        runner.setProperty(Wait.RELEASE_SIGNAL_IDENTIFIER, "${releaseSignalAttribute}");

        final Map<String, String> props = new HashMap<>();
        props.put("releaseSignalAttribute", "1");
        runner.enqueue(new byte[]{}, props);

        runner.run();

        // no cache key attribute
        runner.assertAllFlowFilesTransferred(Wait.REL_WAIT, 1);
        runner.clearTransferState();
    }

    @Test
    public void testWaitKeepInUpstreamConnection() throws InitializationException {
        runner.setProperty(Wait.RELEASE_SIGNAL_IDENTIFIER, "${releaseSignalAttribute}");
        runner.setProperty(Wait.WAIT_MODE, Wait.WAIT_MODE_KEEP_IN_UPSTREAM);

        final Map<String, String> props = new HashMap<>();
        props.put("releaseSignalAttribute", "1");
        runner.enqueue(new byte[]{}, props);

        runner.run();

        // The FlowFile stays in the upstream connection.
        runner.assertQueueNotEmpty();
        runner.clearTransferState();
    }

    @Test
    public void testExpired() throws InitializationException, InterruptedException {
        runner.setProperty(Wait.RELEASE_SIGNAL_IDENTIFIER, "${releaseSignalAttribute}");
        runner.setProperty(Wait.EXPIRATION_DURATION, "100 ms");

        final Map<String, String> props = new HashMap<>();
        props.put("releaseSignalAttribute", "1");
        runner.enqueue(new byte[]{}, props);

        runner.run();

        runner.assertAllFlowFilesTransferred(Wait.REL_WAIT, 1);
        MockFlowFile ff = runner.getFlowFilesForRelationship(Wait.REL_WAIT).get(0);

        runner.clearTransferState();
        runner.enqueue(ff);

        Thread.sleep(101L);
        runner.run();

        runner.assertAllFlowFilesTransferred(Wait.REL_EXPIRED, 1);
        runner.clearTransferState();
    }

    @Test
    public void testCounterExpired() throws InitializationException, InterruptedException, IOException {
        runner.setProperty(Wait.RELEASE_SIGNAL_IDENTIFIER, "${releaseSignalAttribute}");
        runner.setProperty(Wait.TARGET_SIGNAL_COUNT, "5");
        runner.setProperty(Wait.EXPIRATION_DURATION, "100 ms");

        final Map<String, String> props = new HashMap<>();
        props.put("releaseSignalAttribute", "notification-id");
        runner.enqueue(new byte[]{}, props);

        runner.run();

        runner.assertAllFlowFilesTransferred(Wait.REL_WAIT, 1);
        MockFlowFile ff = runner.getFlowFilesForRelationship(Wait.REL_WAIT).get(0);

        runner.clearTransferState();
        runner.enqueue(ff);

        final WaitNotifyProtocol protocol = new WaitNotifyProtocol(service);
        final Map<String, String> signalAttributes = new HashMap<>();
        signalAttributes.put("signal-attr-1", "signal-attr-1-value");
        signalAttributes.put("signal-attr-2", "signal-attr-2-value");
        protocol.notify("notification-id", "counter-A", 1, signalAttributes);
        protocol.notify("notification-id", "counter-B", 2, signalAttributes);

        Thread.sleep(101L);
        runner.run();

        runner.assertAllFlowFilesTransferred(Wait.REL_EXPIRED, 1);
        ff = runner.getFlowFilesForRelationship(Wait.REL_EXPIRED).get(0);
        // Even if wait didn't complete, signal attributes should be set
        ff.assertAttributeEquals("wait.counter.total", "3");
        ff.assertAttributeEquals("wait.counter.counter-A", "1");
        ff.assertAttributeEquals("wait.counter.counter-B", "2");
        ff.assertAttributeEquals("signal-attr-1", "signal-attr-1-value");
        ff.assertAttributeEquals("signal-attr-2", "signal-attr-2-value");
        runner.clearTransferState();
    }

    @Test
    public void testBadWaitStartTimestamp() throws InitializationException, InterruptedException {
        runner.setProperty(Wait.RELEASE_SIGNAL_IDENTIFIER, "${releaseSignalAttribute}");
        runner.setProperty(Wait.EXPIRATION_DURATION, "100 ms");

        final Map<String, String> props = new HashMap<>();
        props.put("releaseSignalAttribute", "1");
        props.put("wait.start.timestamp", "blue bunny");
        runner.enqueue(new byte[]{}, props);

        runner.run();

        runner.assertAllFlowFilesTransferred(Wait.REL_FAILURE, 1);
        runner.clearTransferState();
    }

    @Test
    public void testEmptyReleaseSignal() throws InitializationException, InterruptedException {
        runner.setProperty(Wait.RELEASE_SIGNAL_IDENTIFIER, "${releaseSignalAttribute}");

        final Map<String, String> props = new HashMap<>();
        runner.enqueue(new byte[]{}, props);

        runner.run();

        runner.assertAllFlowFilesTransferred(Wait.REL_FAILURE, 1);
        runner.getFlowFilesForRelationship(Wait.REL_FAILURE).get(0).assertAttributeNotExists("wait.counter.total");
        runner.clearTransferState();
    }

    @Test
    public void testFailingCacheService() throws InitializationException, IOException {
        service.setFailOnCalls(true);
        runner.setProperty(Wait.RELEASE_SIGNAL_IDENTIFIER, "${releaseSignalAttribute}");

        final Map<String, String> props = new HashMap<>();
        props.put("releaseSignalAttribute", "2");
        runner.enqueue(new byte[]{}, props);
        try {
            runner.run();
            fail("Expect the processor to receive an IO exception from the cache service and throws ProcessException.");
        } catch (final AssertionError e) {
            assertTrue(e.getCause() instanceof ProcessException);
            assertTrue(e.getCause().getCause() instanceof IOException);
        } finally {
            service.setFailOnCalls(false);
        }
    }

    @Test
    public void testReplaceAttributes() throws InitializationException, IOException {
        Map<String, String> cachedAttributes = new HashMap<>();
        cachedAttributes.put("both", "notifyValue");
        cachedAttributes.put("uuid", "notifyUuid");
        cachedAttributes.put("notify.only", "notifyValue");

        // Setup existing cache entry.
        final WaitNotifyProtocol protocol = new WaitNotifyProtocol(service);
        protocol.notify("key", "default", 1, cachedAttributes);

        runner.setProperty(Wait.RELEASE_SIGNAL_IDENTIFIER, "${releaseSignalAttribute}");
        runner.setProperty(Wait.ATTRIBUTE_COPY_MODE, Wait.ATTRIBUTE_COPY_REPLACE.getValue());

        final Map<String, String> waitAttributes = new HashMap<>();
        waitAttributes.put("releaseSignalAttribute", "key");
        waitAttributes.put("wait.only", "waitValue");
        waitAttributes.put("both", "waitValue");
        waitAttributes.put("uuid", UUID.randomUUID().toString());
        String flowFileContent = "content";
        runner.enqueue(flowFileContent.getBytes("UTF-8"), waitAttributes);

        // make sure the key is in the cache before Wait runs
        assertNotNull(protocol.getSignal("key"));

        runner.run();

        runner.assertAllFlowFilesTransferred(Wait.REL_SUCCESS, 1);
        runner.assertTransferCount(Wait.REL_SUCCESS, 1);

        final MockFlowFile outputFlowFile = runner.getFlowFilesForRelationship(Wait.REL_SUCCESS).get(0);

        // show a new attribute was copied from the cache
        assertEquals("notifyValue", outputFlowFile.getAttribute("notify.only"));
        // show that uuid was not overwritten
        assertEquals(waitAttributes.get("uuid"), outputFlowFile.getAttribute("uuid"));
        // show that the original attributes are still there
        assertEquals("waitValue", outputFlowFile.getAttribute("wait.only"));

        // here's the important part: show that the cached attribute replaces the original
        assertEquals("notifyValue", outputFlowFile.getAttribute("both"));
        runner.clearTransferState();

        // make sure Wait removed this key from the cache
        assertNull(protocol.getSignal("key"));
    }

    @Test
    public void testKeepOriginalAttributes() throws InitializationException, IOException {
        Map<String, String> cachedAttributes = new HashMap<>();
        cachedAttributes.put("both", "notifyValue");
        cachedAttributes.put("uuid", "notifyUuid");
        cachedAttributes.put("notify.only", "notifyValue");

        // Setup existing cache entry.
        final WaitNotifyProtocol protocol = new WaitNotifyProtocol(service);
        protocol.notify("key", "default", 1, cachedAttributes);

        runner.setProperty(Wait.RELEASE_SIGNAL_IDENTIFIER, "${releaseSignalAttribute}");
        runner.setProperty(Wait.ATTRIBUTE_COPY_MODE, Wait.ATTRIBUTE_COPY_KEEP_ORIGINAL.getValue());

        final Map<String, String> waitAttributes = new HashMap<>();
        waitAttributes.put("releaseSignalAttribute", "key");
        waitAttributes.put("wait.only", "waitValue");
        waitAttributes.put("both", "waitValue");
        waitAttributes.put("uuid", UUID.randomUUID().toString());
        String flowFileContent = "content";
        runner.enqueue(flowFileContent.getBytes("UTF-8"), waitAttributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(Wait.REL_SUCCESS, 1);
        runner.assertTransferCount(Wait.REL_SUCCESS, 1);

        final MockFlowFile outputFlowFile = runner.getFlowFilesForRelationship(Wait.REL_SUCCESS).get(0);

        // show a new attribute was copied from the cache
        assertEquals("notifyValue", outputFlowFile.getAttribute("notify.only"));
        // show that uuid was not overwritten
        assertEquals(waitAttributes.get("uuid"), outputFlowFile.getAttribute("uuid"));
        // show that the original attributes are still there
        assertEquals("waitValue", outputFlowFile.getAttribute("wait.only"));

        // here's the important part: show that the original attribute is kept
        assertEquals("waitValue", outputFlowFile.getAttribute("both"));
        runner.clearTransferState();
    }

    @Test
    public void testWaitForTotalCount() throws InitializationException, IOException {
        Map<String, String> cachedAttributes = new HashMap<>();
        cachedAttributes.put("both", "notifyValue");
        cachedAttributes.put("uuid", "notifyUuid");
        cachedAttributes.put("notify.only", "notifyValue");

        // Setup existing cache entry.
        final WaitNotifyProtocol protocol = new WaitNotifyProtocol(service);
        protocol.notify("key", "counter-A", 1, cachedAttributes);

        runner.setProperty(Wait.RELEASE_SIGNAL_IDENTIFIER, "${releaseSignalAttribute}");
        runner.setProperty(Wait.TARGET_SIGNAL_COUNT, "${targetSignalCount}");

        final Map<String, String> waitAttributes = new HashMap<>();
        waitAttributes.put("releaseSignalAttribute", "key");
        waitAttributes.put("targetSignalCount", "3");
        waitAttributes.put("wait.only", "waitValue");
        waitAttributes.put("both", "waitValue");
        waitAttributes.put("uuid", UUID.randomUUID().toString());
        String flowFileContent = "content";
        runner.enqueue(flowFileContent.getBytes("UTF-8"), waitAttributes);

        /*
         * 1st iteration
         */
        runner.run();
        runner.assertAllFlowFilesTransferred(Wait.REL_WAIT, 1);
        MockFlowFile waitingFlowFile = runner.getFlowFilesForRelationship(Wait.REL_WAIT).get(0);

        /*
         * 2nd iteration.
         */
        runner.clearTransferState();
        runner.enqueue(waitingFlowFile);

        // Notify with other counter.
        protocol.notify("key", "counter-B", 1, cachedAttributes);

        runner.run();
        runner.assertAllFlowFilesTransferred(Wait.REL_WAIT, 1);
        // Still waiting since total count doesn't reach to 3.
        waitingFlowFile = runner.getFlowFilesForRelationship(Wait.REL_WAIT).get(0);

        /*
         * 3rd iteration.
         */
        runner.clearTransferState();
        runner.enqueue(waitingFlowFile);

        runner.run();
        runner.assertAllFlowFilesTransferred(Wait.REL_WAIT, 1);
        // Still waiting since total count doesn't reach to 3.
        waitingFlowFile = runner.getFlowFilesForRelationship(Wait.REL_WAIT).get(0);

        /*
         * 4th iteration.
         */
        runner.clearTransferState();
        runner.enqueue(waitingFlowFile);

        // Notify with other counter.
        protocol.notify("key", "counter-C", 1, cachedAttributes);

        runner.run();
        runner.assertAllFlowFilesTransferred(Wait.REL_SUCCESS, 1);

        final MockFlowFile outputFlowFile = runner.getFlowFilesForRelationship(Wait.REL_SUCCESS).get(0);

        // show a new attribute was copied from the cache
        assertEquals("notifyValue", outputFlowFile.getAttribute("notify.only"));
        // show that uuid was not overwritten
        assertEquals(waitAttributes.get("uuid"), outputFlowFile.getAttribute("uuid"));
        // show that the original attributes are still there
        assertEquals("waitValue", outputFlowFile.getAttribute("wait.only"));
        // show that the original attribute is kept
        assertEquals("waitValue", outputFlowFile.getAttribute("both"));
        runner.clearTransferState();

        assertNull("The key no longer exist", protocol.getSignal("key"));
    }

    @Test
    public void testWaitForSpecificCount() throws InitializationException, IOException {
        Map<String, String> cachedAttributes = new HashMap<>();
        cachedAttributes.put("both", "notifyValue");
        cachedAttributes.put("uuid", "notifyUuid");
        cachedAttributes.put("notify.only", "notifyValue");

        // Setup existing cache entry.
        final WaitNotifyProtocol protocol = new WaitNotifyProtocol(service);
        protocol.notify("key", "counter-A", 1, cachedAttributes);

        runner.setProperty(Wait.RELEASE_SIGNAL_IDENTIFIER, "${releaseSignalAttribute}");
        runner.setProperty(Wait.TARGET_SIGNAL_COUNT, "${targetSignalCount}");
        runner.setProperty(Wait.SIGNAL_COUNTER_NAME, "${signalCounterName}");

        final Map<String, String> waitAttributes = new HashMap<>();
        waitAttributes.put("releaseSignalAttribute", "key");
        waitAttributes.put("targetSignalCount", "2");
        waitAttributes.put("signalCounterName", "counter-B");
        waitAttributes.put("wait.only", "waitValue");
        waitAttributes.put("both", "waitValue");
        waitAttributes.put("uuid", UUID.randomUUID().toString());
        String flowFileContent = "content";
        runner.enqueue(flowFileContent.getBytes("UTF-8"), waitAttributes);

        /*
         * 1st iteration
         */
        runner.run();
        runner.assertAllFlowFilesTransferred(Wait.REL_WAIT, 1);
        MockFlowFile waitingFlowFile = runner.getFlowFilesForRelationship(Wait.REL_WAIT).get(0);

        /*
         * 2nd iteration.
         */
        runner.clearTransferState();
        runner.enqueue(waitingFlowFile);

        // Notify with target counter.
        protocol.notify("key", "counter-B", 1, cachedAttributes);

        runner.run();
        runner.assertAllFlowFilesTransferred(Wait.REL_WAIT, 1);
        // Still waiting since counter-B doesn't reach to 2.
        waitingFlowFile = runner.getFlowFilesForRelationship(Wait.REL_WAIT).get(0);

        /*
         * 3rd iteration.
         */
        runner.clearTransferState();
        runner.enqueue(waitingFlowFile);

        // Notify with other counter.
        protocol.notify("key", "counter-C", 1, cachedAttributes);

        runner.run();
        runner.assertAllFlowFilesTransferred(Wait.REL_WAIT, 1);
        // Still waiting since total count doesn't reach to 3.
        waitingFlowFile = runner.getFlowFilesForRelationship(Wait.REL_WAIT).get(0);

        /*
         * 4th iteration.
         */
        runner.clearTransferState();
        runner.enqueue(waitingFlowFile);

        // Notify with target counter.
        protocol.notify("key", "counter-B", 1, cachedAttributes);

        runner.run();
        runner.assertAllFlowFilesTransferred(Wait.REL_SUCCESS, 1);

        final MockFlowFile outputFlowFile = runner.getFlowFilesForRelationship(Wait.REL_SUCCESS).get(0);

        // show a new attribute was copied from the cache
        assertEquals("notifyValue", outputFlowFile.getAttribute("notify.only"));
        // show that uuid was not overwritten
        assertEquals(waitAttributes.get("uuid"), outputFlowFile.getAttribute("uuid"));
        // show that the original attributes are still there
        assertEquals("waitValue", outputFlowFile.getAttribute("wait.only"));
        // show that the original attribute is kept
        assertEquals("waitValue", outputFlowFile.getAttribute("both"));

        outputFlowFile.assertAttributeEquals("wait.counter.total", "4");
        outputFlowFile.assertAttributeEquals("wait.counter.counter-A", "1");
        outputFlowFile.assertAttributeEquals("wait.counter.counter-B", "2");
        outputFlowFile.assertAttributeEquals("wait.counter.counter-C", "1");

        runner.clearTransferState();

    }

    @Test
    public void testDecrementCache() throws ConcurrentModificationException, IOException {
        Map<String, String> cachedAttributes = new HashMap<>();
        cachedAttributes.put("both", "notifyValue");
        cachedAttributes.put("uuid", "notifyUuid");
        cachedAttributes.put("notify.only", "notifyValue");

        // Setup existing cache entry.
        final WaitNotifyProtocol protocol = new WaitNotifyProtocol(service);

        // A flow file comes in Notify and increment the counter
        protocol.notify("key", "counter", 1, cachedAttributes);

        // another flow files comes in Notify and increment the counter
        protocol.notify("key", "counter", 1, cachedAttributes);

        runner.setProperty(Wait.RELEASE_SIGNAL_IDENTIFIER, "${releaseSignalAttribute}");
        runner.setProperty(Wait.SIGNAL_COUNTER_NAME, "${signalCounterName}");
        runner.setProperty(Wait.TARGET_SIGNAL_COUNT, "1");
        runner.assertValid();

        final Map<String, String> waitAttributes = new HashMap<>();
        waitAttributes.put("releaseSignalAttribute", "key");
        waitAttributes.put("signalCounterName", "counter");
        waitAttributes.put("wait.only", "waitValue");
        waitAttributes.put("both", "waitValue");
        waitAttributes.put("uuid", UUID.randomUUID().toString());
        String flowFileContent = "content";
        runner.enqueue(flowFileContent.getBytes("UTF-8"), waitAttributes);

        /*
         * 1st iteration
         */
        runner.run();
        runner.assertAllFlowFilesTransferred(Wait.REL_SUCCESS, 1);
        MockFlowFile outputFlowFile = runner.getFlowFilesForRelationship(Wait.REL_SUCCESS).get(0);
        outputFlowFile.assertAttributeEquals("wait.counter.counter", "2");

        // expect counter to be decremented to 0 and releasable count remains 1.
        assertEquals("0", Long.toString(protocol.getSignal("key").getCount("counter")));
        assertEquals("1", Long.toString(protocol.getSignal("key").getReleasableCount()));

        // introduce a second flow file with the same signal attribute
        runner.enqueue(flowFileContent.getBytes("UTF-8"), waitAttributes);

        /*
         * 2nd iteration
         */
        runner.clearTransferState();
        runner.run();
        runner.assertAllFlowFilesTransferred(Wait.REL_SUCCESS, 1);
        outputFlowFile = runner.getFlowFilesForRelationship(Wait.REL_SUCCESS).get(0);
        // All counters are consumed.
        outputFlowFile.assertAttributeEquals("wait.counter.counter", "0");

        assertNull("The key no longer exist", protocol.getSignal("key"));
        runner.clearTransferState();
    }

    private class TestIteration {
        final List<MockFlowFile> released = new ArrayList<>();
        final List<MockFlowFile> waiting = new ArrayList<>();
        final List<MockFlowFile> failed = new ArrayList<>();

        final List<String> expectedReleased = new ArrayList<>();
        final List<String> expectedWaiting = new ArrayList<>();
        final List<String> expectedFailed = new ArrayList<>();

        void run() {
            released.clear();
            waiting.clear();
            failed.clear();

            runner.run();

            released.addAll(runner.getFlowFilesForRelationship(Wait.REL_SUCCESS));
            waiting.addAll(runner.getFlowFilesForRelationship(Wait.REL_WAIT));
            failed.addAll(runner.getFlowFilesForRelationship(Wait.REL_FAILURE));

            assertEquals(expectedReleased.size(), released.size());
            assertEquals(expectedWaiting.size(), waiting.size());
            assertEquals(expectedFailed.size(), failed.size());

            final BiConsumer<List<String>, List<MockFlowFile>> assertContents = (expected, actual) -> {
                for (int i = 0; i < expected.size(); i++) {
                    actual.get(i).assertContentEquals(expected.get(i));
                }
            };

            assertContents.accept(expectedReleased, released);
            assertContents.accept(expectedWaiting, waiting);
            assertContents.accept(expectedFailed, failed);

            runner.clearTransferState();
            expectedReleased.clear();
            expectedWaiting.clear();
            expectedFailed.clear();
        }
    }

    @Test
    public void testWaitBufferCount() throws InitializationException, IOException {
        Map<String, String> cachedAttributes = new HashMap<>();
        cachedAttributes.put("notified", "notified-value");

        // Setup existing cache entry.
        final WaitNotifyProtocol protocol = new WaitNotifyProtocol(service);

        runner.setProperty(Wait.RELEASE_SIGNAL_IDENTIFIER, "${releaseSignalAttribute}");
        runner.setProperty(Wait.TARGET_SIGNAL_COUNT, "${targetSignalCount}");
        runner.setProperty(Wait.SIGNAL_COUNTER_NAME, "${signalCounterName}");
        runner.setProperty(Wait.WAIT_BUFFER_COUNT, "2");

        final Map<String, String> waitAttributesA = new HashMap<>();
        waitAttributesA.put("releaseSignalAttribute", "key-A");
        waitAttributesA.put("targetSignalCount", "1");
        waitAttributesA.put("signalCounterName", "counter");

        final Map<String, String> waitAttributesB = new HashMap<>();
        waitAttributesB.put("releaseSignalAttribute", "key-B");
        waitAttributesB.put("targetSignalCount", "3");
        waitAttributesB.put("signalCounterName", "counter");

        final Map<String, String> waitAttributesBInvalid = new HashMap<>();
        waitAttributesBInvalid.putAll(waitAttributesB);
        waitAttributesBInvalid.remove("releaseSignalAttribute");

        final TestIteration testIteration = new TestIteration();

        // Enqueue multiple wait FlowFiles.
        runner.enqueue("1".getBytes(), waitAttributesB); // Should be picked at 1st and 2nd itr
        runner.enqueue("2".getBytes(), waitAttributesA); // Should be picked at 3rd itr
        runner.enqueue("3".getBytes(), waitAttributesBInvalid);
        runner.enqueue("4".getBytes(), waitAttributesA); // Should be picked at 3rd itr
        runner.enqueue("5".getBytes(), waitAttributesB); // Should be picked at 1st
        runner.enqueue("6".getBytes(), waitAttributesB); // Should be picked at 2nd itr

        /*
         * 1st run:
         * pick 1 key-B
         * skip 2 cause key-A
         * skip 3 cause invalid
         * skip 4 cause key-A
         * pick 5 key-B
         */
        testIteration.expectedWaiting.addAll(Arrays.asList("1", "5")); // Picked, but not enough counter.
        testIteration.expectedFailed.add("3"); // invalid.
        testIteration.run();

        /*
         * 2nd run:
         * pick 6 key-B
         * pick 1 key-B
         */
        protocol.notify("key-B", "counter", 3, cachedAttributes);
        testIteration.expectedReleased.add("6");
        testIteration.expectedWaiting.add("1"); // Picked but only one FlowFile can be released.
        // enqueue waiting, simulating wait relationship back to self
        testIteration.waiting.forEach(f -> runner.enqueue(f));
        testIteration.run();

    }

    @Test
    public void testReleaseMultipleFlowFiles() throws InitializationException, IOException {
        Map<String, String> cachedAttributes = new HashMap<>();
        cachedAttributes.put("notified", "notified-value");

        // Setup existing cache entry.
        final WaitNotifyProtocol protocol = new WaitNotifyProtocol(service);

        runner.setProperty(Wait.RELEASE_SIGNAL_IDENTIFIER, "${releaseSignalAttribute}");
        runner.setProperty(Wait.TARGET_SIGNAL_COUNT, "${targetSignalCount}");
        runner.setProperty(Wait.SIGNAL_COUNTER_NAME, "${signalCounterName}");
        runner.setProperty(Wait.WAIT_BUFFER_COUNT, "2");
        runner.setProperty(Wait.RELEASABLE_FLOWFILE_COUNT, "${fragmentCount}");

        final Map<String, String> waitAttributes = new HashMap<>();
        waitAttributes.put("releaseSignalAttribute", "key");
        waitAttributes.put("targetSignalCount", "3");
        waitAttributes.put("signalCounterName", "counter");
        waitAttributes.put("fragmentCount", "6");

        final TestIteration testIteration = new TestIteration();

        // Enqueue 6 wait FlowFiles. 1,2,3,4,5,6
        IntStream.range(1, 7).forEach(i -> runner.enqueue(String.valueOf(i).getBytes(), waitAttributes));

        /*
         * 1st run
         */
        testIteration.expectedWaiting.addAll(Arrays.asList("1", "2"));
        testIteration.run();

        WaitNotifyProtocol.Signal signal = protocol.getSignal("key");
        assertNull(signal);

        /*
         * 2nd run
         */
        protocol.notify("key", "counter", 3, cachedAttributes);
        testIteration.expectedReleased.addAll(Arrays.asList("3", "4"));
        testIteration.waiting.forEach(f -> runner.enqueue(f));
        testIteration.run();

        signal = protocol.getSignal("key");
        assertEquals(0, signal.getCount("count"));
        assertEquals(4, signal.getReleasableCount());

        /*
         * 3rd run
         */
        testIteration.expectedReleased.addAll(Arrays.asList("5", "6"));
        testIteration.waiting.forEach(f -> runner.enqueue(f));
        testIteration.run();

        signal = protocol.getSignal("key");
        assertEquals(0, signal.getCount("count"));
        assertEquals(2, signal.getReleasableCount());
    }

    @Test
    public void testOpenGate() throws InitializationException, IOException {
        Map<String, String> cachedAttributes = new HashMap<>();
        cachedAttributes.put("notified", "notified-value");

        // Setup existing cache entry.
        final WaitNotifyProtocol protocol = new WaitNotifyProtocol(service);

        runner.setProperty(Wait.RELEASE_SIGNAL_IDENTIFIER, "${releaseSignalAttribute}");
        runner.setProperty(Wait.TARGET_SIGNAL_COUNT, "${targetSignalCount}");
        runner.setProperty(Wait.SIGNAL_COUNTER_NAME, "${signalCounterName}");
        runner.setProperty(Wait.WAIT_BUFFER_COUNT, "2");
        runner.setProperty(Wait.RELEASABLE_FLOWFILE_COUNT, "0"); // Leave gate open

        final Map<String, String> waitAttributes = new HashMap<>();
        waitAttributes.put("releaseSignalAttribute", "key");
        waitAttributes.put("targetSignalCount", "3");
        waitAttributes.put("signalCounterName", "counter");

        final TestIteration testIteration = new TestIteration();

        // Enqueue 6 wait FlowFiles. 1,2,3,4,5,6
        IntStream.range(1, 7).forEach(i -> runner.enqueue(String.valueOf(i).getBytes(), waitAttributes));

        /*
         * 1st run
         */
        testIteration.expectedWaiting.addAll(Arrays.asList("1", "2"));
        testIteration.run();

        WaitNotifyProtocol.Signal signal = protocol.getSignal("key");
        assertNull(signal);

        /*
         * 2nd run
         */
        protocol.notify("key", "counter", 3, cachedAttributes);
        testIteration.expectedReleased.addAll(Arrays.asList("3", "4"));
        testIteration.waiting.forEach(f -> runner.enqueue(f));
        testIteration.run();

        signal = protocol.getSignal("key");
        assertEquals(3, signal.getCount("counter"));
        assertEquals(0, signal.getReleasableCount());

        /*
         * 3rd run
         */
        testIteration.expectedReleased.addAll(Arrays.asList("5", "6"));
        testIteration.waiting.forEach(f -> runner.enqueue(f));
        testIteration.run();

        signal = protocol.getSignal("key");
        assertEquals(3, signal.getCount("counter"));
        assertEquals(0, signal.getReleasableCount());
    }
}