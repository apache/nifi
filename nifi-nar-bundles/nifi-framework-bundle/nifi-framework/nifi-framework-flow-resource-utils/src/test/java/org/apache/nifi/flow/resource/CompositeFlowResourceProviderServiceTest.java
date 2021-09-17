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
package org.apache.nifi.flow.resource;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

public class CompositeFlowResourceProviderServiceTest {

    @Test
    public void testWorkerThreadIsManagedCorrectly() {
        // given
        final String threadName = Thread.currentThread().getName();
        final TestFlowResourceProviderWorker worker = new TestFlowResourceProviderWorker();
        final Set<FlowResourceProviderWorker> workers = new HashSet<>();
        workers.add(worker);

        // when
        final CompositeFlowResourceProviderService testSubject = new CompositeFlowResourceProviderService("testService", workers, new CountDownLatch(0));
        testSubject.start();

        // then
        Assert.assertTrue(worker.isRunning());

        // when
        testSubject.stop();

        // then
        Assert.assertFalse(worker.isRunning());
        Assert.assertNotEquals(threadName, worker.threadName);
    }

    private static class TestFlowResourceProviderWorker implements FlowResourceProviderWorker {
        volatile String threadName;
        volatile boolean stopped;

        @Override
        public String getName() {
            return "name";
        }

        @Override
        public FlowResourceProvider getProvider() {
            return Mockito.mock(FlowResourceProvider.class);
        }

        @Override
        public boolean isRunning() {
            return !stopped;
        }

        @Override
        public void stop() {
            stopped = true;
        }

        @Override
        public void run() {
            threadName = Thread.currentThread().getName();

            while (!stopped) {
                try {
                    Thread.sleep(10);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    Assert.fail();
                    stopped = true;
                }
            }
        }
    }
}