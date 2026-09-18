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
package org.apache.nifi.controller;

import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.validation.ValidationTrigger;
import org.apache.nifi.components.validation.VerifiableComponentFactory;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.util.NoOpProcessor;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class StandardProcessorNodeTest {

    @Test
    void testYieldExpiration() {
        final ProcessScheduler processScheduler = mock(ProcessScheduler.class);
        final Processor processor = new NoOpProcessor();
        final StandardProcessorNode processorNode = createProcessorNode(processor, processScheduler);

        processorNode.yield(0L, TimeUnit.MILLISECONDS);
        assertEquals(0L, processorNode.getYieldExpiration());

        processorNode.yield(1L, TimeUnit.DAYS);
        final long expiration = processorNode.getYieldExpiration();
        assertTrue(expiration > System.currentTimeMillis());

        processorNode.yield(1L, TimeUnit.SECONDS);
        assertEquals(expiration, processorNode.getYieldExpiration());
    }

    @Test
    void testGetActiveThreadsIncludesVirtualThread() throws InterruptedException {
        final String threadName = "virtual-processor-task";
        final CountDownLatch invocationStarted = new CountDownLatch(1);
        final CountDownLatch releaseInvocation = new CountDownLatch(1);
        final Processor processor = new NoOpProcessor() {
            @Override
            public void onTrigger(final ProcessContext context, final ProcessSession session) {
                invocationStarted.countDown();
                try {
                    releaseInvocation.await();
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        };
        final StandardProcessorNode processorNode = createProcessorNode(processor, mock(ProcessScheduler.class));
        final ProcessSessionFactory sessionFactory = mock(ProcessSessionFactory.class);
        when(sessionFactory.createSession()).thenReturn(mock(ProcessSession.class));
        final Thread virtualThread = Thread.ofVirtual().name(threadName).start(() -> processorNode.onTrigger(mock(ProcessContext.class), sessionFactory));

        try {
            assertTrue(invocationStarted.await(2, TimeUnit.SECONDS));

            final List<ActiveThreadInfo> activeThreads = processorNode.getActiveThreads(ThreadDetails.capture());

            assertEquals(1, activeThreads.size());
            assertEquals(threadName, activeThreads.getFirst().getThreadName());
            assertTrue(activeThreads.getFirst().getStackTrace().contains(threadName));
            assertTrue(activeThreads.getFirst().getStackTrace().contains(StandardProcessorNodeTest.class.getName()));
        } finally {
            releaseInvocation.countDown();
            virtualThread.join(2_000L);
        }

        assertFalse(virtualThread.isAlive());
    }

    private StandardProcessorNode createProcessorNode(final Processor processor, final ProcessScheduler processScheduler) {
        final LoggableComponent<Processor> loggableProcessor = new LoggableComponent<>(processor, BundleCoordinate.UNKNOWN_COORDINATE, null);
        return new StandardProcessorNode(loggableProcessor, "processor", mock(ValidationContextFactory.class), processScheduler,
                mock(ControllerServiceProvider.class), mock(ReloadComponent.class), mock(VerifiableComponentFactory.class),
                mock(ExtensionManager.class), mock(ValidationTrigger.class));
    }
}
