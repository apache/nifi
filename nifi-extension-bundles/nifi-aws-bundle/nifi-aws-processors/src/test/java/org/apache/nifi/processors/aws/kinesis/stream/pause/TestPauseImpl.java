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

import org.apache.nifi.processors.aws.kinesis.stream.ConsumeKinesisStream;
import org.apache.nifi.util.MockProcessContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.function.Supplier;

public class TestPauseImpl {

    private static final ConsumeKinesisStream consumeKinesisStream = new ConsumeKinesisStream();

    @Test
    public void testResumeAfterPause() {
        final TestPauseInspector pauseInspector = new TestPauseInspector();
        final MockProcessContext mockProcessContext = new MockProcessContext(consumeKinesisStream);
        mockProcessContext.setUnavailableRelationships(Set.of(ConsumeKinesisStream.REL_SUCCESS));

        final PauseImpl consumeHalter = PauseImpl.create(pauseInspector);

        consumeHalter.onTrigger(mockProcessContext);
        final Thread thread = new Thread(createPauseRunnable(consumeHalter));
        thread.start();

        pauseInspector.awaitAwaited();
        Assertions.assertTrue(thread.isAlive());

        mockProcessContext.setUnavailableRelationships(Set.of());
        consumeHalter.onTrigger(mockProcessContext);

        pauseInspector.awaitFinished();
    }

    @Test
    public void testNoPause() {
        final TestPauseInspector pauseInspector = new TestPauseInspector();
        final MockProcessContext mockProcessContext = new MockProcessContext(consumeKinesisStream);
        mockProcessContext.setUnavailableRelationships(Set.of());

        final PauseImpl consumeHalter = PauseImpl.create(pauseInspector);

        consumeHalter.onTrigger(mockProcessContext);
        final Thread thread = new Thread(createPauseRunnable(consumeHalter));
        thread.start();

        pauseInspector.awaitFinished();
    }

    @Test
    public void testPause() {
        final TestPauseInspector pauseInspector = new TestPauseInspector();
        final MockProcessContext mockProcessContext = new MockProcessContext(consumeKinesisStream);
        mockProcessContext.setUnavailableRelationships(Set.of(ConsumeKinesisStream.REL_SUCCESS));

        final PauseImpl consumeHalter = PauseImpl.create(pauseInspector);
        consumeHalter.onTrigger(mockProcessContext);
        final Thread thread = new Thread(createPauseRunnable(consumeHalter));
        thread.start();

        pauseInspector.awaitAwaited();
        Assertions.assertTrue(thread.isAlive());
        thread.interrupt();
    }

    private static Runnable createPauseRunnable(final PauseImpl consumeHalter) {
        return () -> {
            try {
                consumeHalter.consumePause();
            } catch (final InterruptedException e) {
                throw new RuntimeException(e);
            }
        };
    }

    private static class TestPauseInspector implements PauseInspector {
        private boolean onPauseAwaited = false;
        private boolean onPauseFinished = false;

        @Override
        public void onPauseAwaited() {
            onPauseAwaited = true;
        }

        @Override
        public void onPauseFinished() {
            onPauseFinished = true;
        }

        public void awaitAwaited() {
            busyWait(() -> !onPauseAwaited);
        }

        public void awaitFinished() {
            busyWait(() -> !onPauseFinished);
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
