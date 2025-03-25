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
package org.apache.nifi.processors.aws.kinesis.stream.pause;

import org.junit.jupiter.api.Test;

import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestStandardRecordProcessorBlocker {

    @Test
    public void testBlockAndUnblock() {
        final TestThreadInspector blockerInspector = new TestThreadInspector();

        final StandardRecordProcessorBlocker recordProcessorBlocker = StandardRecordProcessorBlocker.create();

        recordProcessorBlocker.block();
        final Thread thread = new Thread(createRunnableWithInspector(recordProcessorBlocker, blockerInspector));
        thread.start();

        blockerInspector.awaitBlockAwaited();
        assertTrue(thread.isAlive());

        recordProcessorBlocker.unblock();
        blockerInspector.awaitBlockExited();
    }

    @Test
    public void testNoBlock() {
        final TestThreadInspector blockerInspector = new TestThreadInspector();

        final StandardRecordProcessorBlocker recordProcessorBlocker = StandardRecordProcessorBlocker.create();

        recordProcessorBlocker.unblock();
        final Thread thread = new Thread(createRunnableWithInspector(recordProcessorBlocker, blockerInspector));
        thread.start();

        blockerInspector.awaitBlockExited();
    }

    @Test
    public void testBlock() {
        final TestThreadInspector blockerInspector = new TestThreadInspector();

        final StandardRecordProcessorBlocker recordProcessorBlocker = StandardRecordProcessorBlocker.create();
        recordProcessorBlocker.block();
        final Thread thread = new Thread(createRunnableWithInspector(recordProcessorBlocker, blockerInspector));
        thread.start();

        blockerInspector.awaitBlockAwaited();
        assertTrue(thread.isAlive());

        recordProcessorBlocker.unblock();
    }

    private static Runnable createRunnableWithInspector(final StandardRecordProcessorBlocker recordProcessorBlocker, TestThreadInspector blockerInspector) {
        return () -> {
            try {
                blockerInspector.onBlockAwaited();
                recordProcessorBlocker.await();
            } catch (final InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                blockerInspector.onBlockExited();
            }
        };
    }

    private static class TestThreadInspector {
        private boolean blockAwaited = false;
        private boolean blockExited = false;

        public void onBlockAwaited() {
            blockAwaited = true;
        }

        public void onBlockExited() {
            blockExited = true;
        }

        public void awaitBlockAwaited() {
            busyWait(() -> !blockAwaited);
        }

        public void awaitBlockExited() {
            busyWait(() -> !blockExited);
        }

        private void busyWait(final Supplier<Boolean> condition) {
            final long maxWait = System.currentTimeMillis() + 1000;
            do {
                if (System.currentTimeMillis() > maxWait) {
                    throw new RuntimeException("Timed out waiting for condition");
                }
            } while (condition.get());
        }
    }
}
