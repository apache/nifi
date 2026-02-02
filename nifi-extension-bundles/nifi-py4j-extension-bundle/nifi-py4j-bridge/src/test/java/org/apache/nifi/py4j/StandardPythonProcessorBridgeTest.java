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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for StandardPythonProcessorBridge cancellation functionality.
 */
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
        assertFalse(bridge.isCanceled(), "Bridge should not be canceled initially");

        bridge.cancel();

        assertTrue(bridge.isCanceled(), "Bridge should be canceled after cancel() is called");
    }

    @Test
    void testCancelSetsLoadStateToCanceled() {
        // Initially should be in some non-canceled state
        LoadState initialState = bridge.getLoadState();
        assertFalse(initialState == LoadState.CANCELED, "Initial state should not be CANCELED");

        bridge.cancel();

        assertEquals(LoadState.CANCELED, bridge.getLoadState(),
                "Load state should be CANCELED after cancel() is called");
    }

    @Test
    void testCancelIsIdempotent() {
        bridge.cancel();
        assertTrue(bridge.isCanceled());
        assertEquals(LoadState.CANCELED, bridge.getLoadState());

        // Call cancel again - should not throw or change state
        bridge.cancel();
        assertTrue(bridge.isCanceled());
        assertEquals(LoadState.CANCELED, bridge.getLoadState());

        // And again
        bridge.cancel();
        assertTrue(bridge.isCanceled());
        assertEquals(LoadState.CANCELED, bridge.getLoadState());
    }

    @Test
    void testIsCanceledReturnsFalseInitially() {
        assertFalse(bridge.isCanceled(), "isCanceled() should return false before cancel() is called");
    }

    @Test
    void testIsCanceledReturnsTrueAfterCancel() {
        bridge.cancel();
        assertTrue(bridge.isCanceled(), "isCanceled() should return true after cancel() is called");
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
        assertTrue(cancelLatch.await(5, TimeUnit.SECONDS), "Cancel should complete within timeout");
        assertTrue(canceledFromThread.get(), "Bridge should be canceled from another thread");
        assertTrue(bridge.isCanceled(), "Bridge should be canceled as seen from main thread");
    }

    @Test
    void testLoadStateTransitionsToCanceledOnCancel() {
        // Get initial state
        LoadState beforeCancel = bridge.getLoadState();

        // Cancel
        bridge.cancel();

        // Verify state changed to CANCELED
        LoadState afterCancel = bridge.getLoadState();
        assertEquals(LoadState.CANCELED, afterCancel,
                "Load state should transition to CANCELED, was: " + beforeCancel);
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

        // Start all threads at once
        startLatch.countDown();

        // Wait for all to complete
        assertTrue(doneLatch.await(10, TimeUnit.SECONDS), "All threads should complete");

        // Verify no exceptions occurred
        if (exception.get() != null) {
            throw new AssertionError("Exception during concurrent cancel", exception.get());
        }

        // Verify final state
        assertTrue(bridge.isCanceled(), "Bridge should be canceled");
        assertEquals(LoadState.CANCELED, bridge.getLoadState(), "Load state should be CANCELED");
    }
}
