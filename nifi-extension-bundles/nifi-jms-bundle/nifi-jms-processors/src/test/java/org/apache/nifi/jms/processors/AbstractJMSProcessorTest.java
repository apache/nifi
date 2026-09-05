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
package org.apache.nifi.jms.processors;

import jakarta.jms.ConnectionFactory;
import org.apache.nifi.jms.cf.IJMSConnectionFactoryProvider;
import org.apache.nifi.util.LogMessage;
import org.apache.nifi.util.MockComponentLog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

public class AbstractJMSProcessorTest {

    @Test
    @Timeout(value = 10)
    public void earlyUnscheduleShouldCloseIdleAndActiveWorkers() {
        final RecordingConnectionFactoryProvider provider = new RecordingConnectionFactoryProvider();
        final MockComponentLog logger = new MockComponentLog("processor", new Object());
        final JmsWorkerLifecycle<TestWorker> lifecycle = new JmsWorkerLifecycle<>(provider, 2, logger);
        final JmsWorkerLifecycle.Generation<TestWorker> generation = lifecycle.captureGeneration();

        final TestWorker idleWorker = new TestWorker("idle");
        final TestWorker activeWorker = new TestWorker("active");

        assertTrue(lifecycle.registerWorker(generation, idleWorker));
        lifecycle.releaseWorker(generation, idleWorker);
        assertTrue(lifecycle.registerWorker(generation, activeWorker));

        lifecycle.closeCycle();

        assertEquals(1, idleWorker.getDestroyCalls());
        assertEquals(1, activeWorker.getDestroyCalls());
        assertNull(lifecycle.pollIdleWorker(generation));
    }

    @Test
    @Timeout(value = 10)
    public void retiredGenerationShouldCloseConstructedWorkerBeforeJmsUse() throws Exception {
        final RecordingConnectionFactoryProvider provider = new RecordingConnectionFactoryProvider();
        final JmsWorkerLifecycle<TestWorker> lifecycle = new JmsWorkerLifecycle<>(provider, 1, new MockComponentLog("processor", new Object()));
        final JmsWorkerLifecycle.Generation<TestWorker> generation = lifecycle.captureGeneration();
        final CountDownLatch workerConstructed = new CountDownLatch(1);
        final CountDownLatch retireRequested = new CountDownLatch(1);
        final AtomicReference<TestWorker> workerReference = new AtomicReference<>();
        final ExecutorService executorService = Executors.newSingleThreadExecutor();

        try {
            final Future<Boolean> workerUsedFuture = executorService.submit(() -> {
                assertTrue(lifecycle.canCreateWorker(generation));

                final TestWorker worker = new TestWorker("constructing");
                workerReference.set(worker);
                workerConstructed.countDown();

                assertTrue(retireRequested.await(5, TimeUnit.SECONDS));
                final boolean accepted = lifecycle.registerWorker(generation, worker);
                if (accepted) {
                    worker.markUsed();
                } else {
                    worker.shutdown();
                }

                return worker.wasUsed();
            });

            assertTrue(workerConstructed.await(5, TimeUnit.SECONDS));
            lifecycle.retireGeneration();
            retireRequested.countDown();

            assertFalse(workerUsedFuture.get(5, TimeUnit.SECONDS));
        } finally {
            executorService.shutdownNow();
            executorService.awaitTermination(5, TimeUnit.SECONDS);
        }

        final TestWorker worker = workerReference.get();
        assertNotNull(worker);
        assertEquals(1, worker.getDestroyCalls());
        assertFalse(worker.wasUsed());
    }

    @Test
    @Timeout(value = 10)
    public void returnedWorkerShouldBeClosedInsteadOfRePooledAfterRetirement() {
        final RecordingConnectionFactoryProvider provider = new RecordingConnectionFactoryProvider();
        final JmsWorkerLifecycle<TestWorker> lifecycle = new JmsWorkerLifecycle<>(provider, 1, new MockComponentLog("processor", new Object()));
        final JmsWorkerLifecycle.Generation<TestWorker> generation = lifecycle.captureGeneration();
        final TestWorker worker = new TestWorker("returned");

        assertTrue(lifecycle.registerWorker(generation, worker));

        lifecycle.retireGeneration();
        lifecycle.releaseWorker(generation, worker);
        lifecycle.activateFreshGeneration();

        assertEquals(1, worker.getDestroyCalls());
        assertNull(lifecycle.pollIdleWorker(generation));
    }

    @Test
    @Timeout(value = 10)
    public void retiredGenerationShouldNotResetOrRebuildInvalidWorker() {
        final RecordingConnectionFactoryProvider provider = new RecordingConnectionFactoryProvider();
        final JmsWorkerLifecycle<TestWorker> lifecycle = new JmsWorkerLifecycle<>(provider, 1, new MockComponentLog("processor", new Object()));
        final JmsWorkerLifecycle.Generation<TestWorker> generation = lifecycle.captureGeneration();
        final TestWorker invalidWorker = new TestWorker("invalid");

        assertTrue(lifecycle.registerWorker(generation, invalidWorker));
        invalidWorker.setValid(false);
        lifecycle.retireGeneration();

        final boolean rebuildAllowed = lifecycle.handleInvalidWorker(generation, invalidWorker, provider.getConnectionFactory());

        assertFalse(rebuildAllowed);
        assertEquals(0, provider.getResetCalls());
        assertEquals(1, invalidWorker.getDestroyCalls());
    }

    @Test
    @Timeout(value = 10)
    public void oldCycleCleanupShouldNotMutateReplacementCycle() {
        final JmsWorkerLifecycle<TestWorker> oldLifecycle = new JmsWorkerLifecycle<>(new RecordingConnectionFactoryProvider(), 1, new MockComponentLog("old", new Object()));
        final JmsWorkerLifecycle<TestWorker> replacementLifecycle = new JmsWorkerLifecycle<>(new RecordingConnectionFactoryProvider(), 1, new MockComponentLog("replacement", new Object()));
        final JmsWorkerLifecycle.Generation<TestWorker> oldGeneration = oldLifecycle.captureGeneration();
        final JmsWorkerLifecycle.Generation<TestWorker> replacementGeneration = replacementLifecycle.captureGeneration();
        final TestWorker oldWorker = new TestWorker("old-worker");
        final TestWorker oldActiveWorker = new TestWorker("old-active-worker");
        final TestWorker replacementWorker = new TestWorker("replacement-worker");

        assertTrue(oldLifecycle.registerWorker(oldGeneration, oldWorker));
        assertTrue(oldLifecycle.registerWorker(oldGeneration, oldActiveWorker));
        assertTrue(replacementLifecycle.registerWorker(replacementGeneration, replacementWorker));
        replacementLifecycle.releaseWorker(replacementGeneration, replacementWorker);

        oldLifecycle.closeCycle();
        oldLifecycle.releaseWorker(oldGeneration, oldActiveWorker);

        assertEquals(1, oldWorker.getDestroyCalls());
        assertEquals(1, oldActiveWorker.getDestroyCalls());
        assertEquals(0, replacementWorker.getDestroyCalls());
        assertSame(replacementWorker, replacementLifecycle.pollIdleWorker(replacementLifecycle.captureGeneration()));
    }

    @Test
    @Timeout(value = 10)
    public void primaryRevocationShouldRetireGenerationAndElectionShouldUseFreshGenerationOnly() {
        final RecordingConnectionFactoryProvider provider = new RecordingConnectionFactoryProvider();
        final JmsWorkerLifecycle<TestWorker> lifecycle = new JmsWorkerLifecycle<>(provider, 2, new MockComponentLog("processor", new Object()));
        final JmsWorkerLifecycle.Generation<TestWorker> revokedGeneration = lifecycle.captureGeneration();
        final TestWorker idleWorker = new TestWorker("idle");
        final TestWorker activeWorker = new TestWorker("active");

        assertTrue(lifecycle.registerWorker(revokedGeneration, idleWorker));
        lifecycle.releaseWorker(revokedGeneration, idleWorker);
        assertTrue(lifecycle.registerWorker(revokedGeneration, activeWorker));

        lifecycle.retireGeneration();
        lifecycle.activateFreshGeneration();

        final JmsWorkerLifecycle.Generation<TestWorker> freshGeneration = lifecycle.captureGeneration();
        final TestWorker freshWorker = new TestWorker("fresh");
        assertTrue(lifecycle.registerWorker(freshGeneration, freshWorker));
        lifecycle.releaseWorker(freshGeneration, freshWorker);
        lifecycle.releaseWorker(revokedGeneration, activeWorker);

        assertEquals(1, idleWorker.getDestroyCalls());
        assertEquals(1, activeWorker.getDestroyCalls());
        assertSame(freshWorker, lifecycle.pollIdleWorker(freshGeneration));
    }

    @Test
    @Timeout(value = 10)
    public void bulkCloseShouldContinueAfterShutdownFailureAndLogWorker() {
        final RecordingConnectionFactoryProvider provider = new RecordingConnectionFactoryProvider();
        final MockComponentLog logger = new MockComponentLog("processor", new Object());
        final JmsWorkerLifecycle<TestWorker> lifecycle = new JmsWorkerLifecycle<>(provider, 3, logger);
        final JmsWorkerLifecycle.Generation<TestWorker> generation = lifecycle.captureGeneration();
        final TestWorker healthyWorker = new TestWorker("healthy");
        final TestWorker failingWorker = new TestWorker("failing", true);
        final TestWorker trailingWorker = new TestWorker("trailing");

        assertTrue(lifecycle.registerWorker(generation, healthyWorker));
        assertTrue(lifecycle.registerWorker(generation, failingWorker));
        assertTrue(lifecycle.registerWorker(generation, trailingWorker));

        lifecycle.closeCycle();

        assertEquals(1, healthyWorker.getDestroyCalls());
        assertEquals(1, failingWorker.getDestroyCalls());
        assertEquals(1, trailingWorker.getDestroyCalls());
        assertTrue(logger.getErrorMessages().stream().anyMatch(message -> containsWorkerReference(message, failingWorker)));
    }

    @Test
    public void closeCycleShouldBeIdempotent() {
        final JmsWorkerLifecycle<TestWorker> lifecycle = new JmsWorkerLifecycle<>(
                new RecordingConnectionFactoryProvider(), 1, new MockComponentLog("processor", new Object()));
        final JmsWorkerLifecycle.Generation<TestWorker> generation = lifecycle.captureGeneration();
        final TestWorker worker = new TestWorker("worker");
        assertTrue(lifecycle.registerWorker(generation, worker));

        lifecycle.closeCycle();
        lifecycle.closeCycle();

        assertEquals(1, worker.getDestroyCalls());
    }

    private boolean containsWorkerReference(final LogMessage message, final TestWorker worker) {
        final Object[] arguments = message.getArgs();
        return arguments != null && Arrays.stream(arguments).anyMatch(argument -> argument == worker);
    }

    private static final class RecordingConnectionFactoryProvider implements IJMSConnectionFactoryProvider {

        private final ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
        private final AtomicInteger resetCalls = new AtomicInteger();

        @Override
        public ConnectionFactory getConnectionFactory() {
            return connectionFactory;
        }

        @Override
        public void resetConnectionFactory(final ConnectionFactory cachedFactory) {
            resetCalls.incrementAndGet();
        }

        private int getResetCalls() {
            return resetCalls.get();
        }

    }

    private static final class TestWorker extends JMSWorker {

        private final DestroyTrackingCachingConnectionFactory cachingConnectionFactory;
        private final AtomicBoolean used = new AtomicBoolean();

        private TestWorker(final String destinationName) {
            this(destinationName, false);
        }

        private TestWorker(final String destinationName, final boolean failDestroy) {
            this(new DestroyTrackingCachingConnectionFactory(mock(ConnectionFactory.class), failDestroy), destinationName);
        }

        private TestWorker(final DestroyTrackingCachingConnectionFactory connectionFactory, final String destinationName) {
            super(connectionFactory, createJmsTemplate(connectionFactory, destinationName), new MockComponentLog(destinationName, destinationName));
            this.cachingConnectionFactory = connectionFactory;
        }

        private static org.springframework.jms.core.JmsTemplate createJmsTemplate(final DestroyTrackingCachingConnectionFactory connectionFactory, final String destinationName) {
            final org.springframework.jms.core.JmsTemplate jmsTemplate = new org.springframework.jms.core.JmsTemplate();
            jmsTemplate.setConnectionFactory(connectionFactory);
            jmsTemplate.setDefaultDestinationName(destinationName);
            return jmsTemplate;
        }

        private void markUsed() {
            used.set(true);
        }

        private boolean wasUsed() {
            return used.get();
        }

        private int getDestroyCalls() {
            return cachingConnectionFactory.getDestroyCalls();
        }
    }

    private static final class DestroyTrackingCachingConnectionFactory extends org.springframework.jms.connection.CachingConnectionFactory {

        private final AtomicInteger destroyCalls = new AtomicInteger();
        private final boolean failFirstDestroy;

        private DestroyTrackingCachingConnectionFactory(final ConnectionFactory targetConnectionFactory, final boolean failFirstDestroy) {
            super(targetConnectionFactory);
            this.failFirstDestroy = failFirstDestroy;
        }

        @Override
        public void destroy() {
            final int destroyCall = destroyCalls.incrementAndGet();
            if (failFirstDestroy && destroyCall == 1) {
                throw new RuntimeException("destroy failed");
            }
        }

        private int getDestroyCalls() {
            return destroyCalls.get();
        }
    }
}
