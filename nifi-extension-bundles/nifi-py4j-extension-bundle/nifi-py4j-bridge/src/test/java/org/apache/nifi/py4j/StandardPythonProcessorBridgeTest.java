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

package org.apache.nifi.py4j;

import org.apache.nifi.components.AsyncLoadedProcessor.LoadState;
import org.apache.nifi.python.PythonController;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StandardPythonProcessorBridgeTest {

    @Mock
    private PythonController controller;

    @Mock
    private ProcessorCreationWorkflow creationWorkflow;

    @TempDir
    private Path tempDir;

    private StandardPythonProcessorBridge bridge;

    @BeforeEach
    void setUp() throws IOException {
        MockitoAnnotations.openMocks(this);

        // Create a temporary module file
        final File moduleFile = Files.createFile(tempDir.resolve("test_processor.py")).toFile();
        final File workDir = Files.createDirectory(tempDir.resolve("work")).toFile();

        bridge = new StandardPythonProcessorBridge.Builder()
                .controller(controller)
                .creationWorkflow(creationWorkflow)
                .processorType("TestProcessor")
                .processorVersion("1.0.0")
                .workingDirectory(workDir)
                .moduleFile(moduleFile)
                .build();
    }

    @Test
    void testCancelSetsFlag() {
        assertFalse(bridge.isCanceled());

        bridge.cancel();

        assertTrue(bridge.isCanceled());
    }

    @Test
    void testCancelSetsLoadStateToCanceled() {
        assertNotSame(LoadState.CANCELED, bridge.getLoadState());

        bridge.cancel();

        assertEquals(LoadState.CANCELED, bridge.getLoadState());
    }

    @Test
    void testCancelIsIdempotent() {
        bridge.cancel();
        assertTrue(bridge.isCanceled());
        assertEquals(LoadState.CANCELED, bridge.getLoadState());

        bridge.cancel();
        assertTrue(bridge.isCanceled());
        assertEquals(LoadState.CANCELED, bridge.getLoadState());

        bridge.cancel();
        assertTrue(bridge.isCanceled());
        assertEquals(LoadState.CANCELED, bridge.getLoadState());
    }

    @Test
    void testIsCanceledReturnsFalseInitially() {
        assertFalse(bridge.isCanceled());
    }

    @Test
    void testIsCanceledReturnsTrueAfterCancel() {
        bridge.cancel();
        assertTrue(bridge.isCanceled());
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void testCancelFromDifferentThread() throws InterruptedException {
        final CountDownLatch cancelLatch = new CountDownLatch(1);
        final AtomicBoolean canceledFromThread = new AtomicBoolean(false);

        Thread cancelThread = new Thread(() -> {
            bridge.cancel();
            canceledFromThread.set(bridge.isCanceled());
            cancelLatch.countDown();
        });

        cancelThread.start();
        assertTrue(cancelLatch.await(5, TimeUnit.SECONDS));
        assertTrue(canceledFromThread.get());
        assertTrue(bridge.isCanceled());
    }

    @Test
    void testLoadStateTransitionsToCanceledOnCancel() {
        assertNotSame(LoadState.CANCELED, bridge.getLoadState());

        bridge.cancel();

        assertEquals(LoadState.CANCELED, bridge.getLoadState());
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void testConcurrentCancelCalls() throws InterruptedException {
        final int numThreads = 10;
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch doneLatch = new CountDownLatch(numThreads);
        final AtomicReference<Exception> exception = new AtomicReference<>();

        for (int i = 0; i < numThreads; i++) {
            new Thread(() -> {
                try {
                    startLatch.await();
                    bridge.cancel();
                } catch (Exception e) {
                    exception.set(e);
                } finally {
                    doneLatch.countDown();
                }
            }).start();
        }

        startLatch.countDown();

        assertTrue(doneLatch.await(10, TimeUnit.SECONDS));

        assertNull(exception.get());

        assertTrue(bridge.isCanceled());
        assertEquals(LoadState.CANCELED, bridge.getLoadState());
    }
}
