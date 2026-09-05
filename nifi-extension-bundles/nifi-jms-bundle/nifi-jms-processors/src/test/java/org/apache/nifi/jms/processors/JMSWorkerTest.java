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
import org.apache.nifi.logging.ComponentLog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.jms.connection.CachingConnectionFactory;
import org.springframework.jms.core.JmsTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

public class JMSWorkerTest {

    @Test
    @Timeout(value = 10)
    public void shutdownShouldDestroyConnectionFactoryOnceAcrossConcurrentCallers() throws Exception {
        final DestroyTrackingCachingConnectionFactory cachingConnectionFactory = new DestroyTrackingCachingConnectionFactory(mock(ConnectionFactory.class), false);
        final TestWorker worker = new TestWorker(cachingConnectionFactory);
        final int callers = 8;
        final CountDownLatch ready = new CountDownLatch(callers);
        final CountDownLatch start = new CountDownLatch(1);
        final ExecutorService executorService = Executors.newFixedThreadPool(callers);

        try {
            final List<Future<Void>> futures = new ArrayList<>();
            for (int index = 0; index < callers; index++) {
                futures.add(executorService.submit(() -> {
                    ready.countDown();
                    assertTrue(start.await(5, TimeUnit.SECONDS));
                    worker.shutdown();
                    return null;
                }));
            }

            assertTrue(ready.await(5, TimeUnit.SECONDS));
            start.countDown();

            for (final Future<Void> future : futures) {
                future.get(5, TimeUnit.SECONDS);
            }
        } finally {
            executorService.shutdownNow();
            executorService.awaitTermination(5, TimeUnit.SECONDS);
        }

        assertEquals(1, cachingConnectionFactory.getDestroyCalls());
    }

    @Test
    @Timeout(value = 10)
    public void shutdownShouldNotRetryDestroyAfterInitialFailure() throws Exception {
        final DestroyTrackingCachingConnectionFactory cachingConnectionFactory = new DestroyTrackingCachingConnectionFactory(mock(ConnectionFactory.class), true);
        final TestWorker worker = new TestWorker(cachingConnectionFactory);

        final RuntimeException thrown = getRuntimeException(worker);

        assertNotNull(thrown, "Expected one shutdown caller to receive the destroy failure");
        assertEquals("destroy failed", thrown.getMessage());
        assertEquals(1, cachingConnectionFactory.getDestroyCalls());
        assertDoesNotThrow(worker::shutdown);
        assertEquals(1, cachingConnectionFactory.getDestroyCalls());
    }

    private RuntimeException getRuntimeException(final TestWorker worker) throws Exception {
        final ExecutorService executorService = Executors.newFixedThreadPool(2);
        final CountDownLatch ready = new CountDownLatch(2);
        final CountDownLatch start = new CountDownLatch(1);

        try {
            final Future<Void> firstFuture = executorService.submit(() -> {
                ready.countDown();
                assertTrue(start.await(5, TimeUnit.SECONDS));
                worker.shutdown();
                return null;
            });
            final Future<Void> secondFuture = executorService.submit(() -> {
                ready.countDown();
                assertTrue(start.await(5, TimeUnit.SECONDS));
                worker.shutdown();
                return null;
            });

            assertTrue(ready.await(5, TimeUnit.SECONDS));
            start.countDown();

            RuntimeException runtimeException = null;
            runtimeException = mergeRuntimeException(runtimeException, firstFuture);
            runtimeException = mergeRuntimeException(runtimeException, secondFuture);
            return runtimeException;
        } finally {
            executorService.shutdownNow();
            executorService.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    private RuntimeException mergeRuntimeException(final RuntimeException current, final Future<Void> future) throws Exception {
        try {
            future.get(5, TimeUnit.SECONDS);
            return current;
        } catch (final ExecutionException executionException) {
            assertInstanceOf(RuntimeException.class, executionException.getCause());
            return current == null ? (RuntimeException) executionException.getCause() : current;
        }
    }

    private static final class TestWorker extends JMSWorker {

        private TestWorker(final DestroyTrackingCachingConnectionFactory connectionFactory) {
            super(connectionFactory, createJmsTemplate(connectionFactory), mock(ComponentLog.class));
        }

        private static JmsTemplate createJmsTemplate(final CachingConnectionFactory connectionFactory) {
            final JmsTemplate jmsTemplate = new JmsTemplate();
            jmsTemplate.setConnectionFactory(connectionFactory);
            jmsTemplate.setDefaultDestinationName("test-destination");
            return jmsTemplate;
        }
    }

    private static final class DestroyTrackingCachingConnectionFactory extends CachingConnectionFactory {

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
