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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

public class CompositeExternalResourceProviderServiceTest {

    @Test
    public void testWorkerThreadIsManagedCorrectly() {
        final String threadName = Thread.currentThread().getName();
        final TestExternalResourceProviderWorker worker = new TestExternalResourceProviderWorker();
        final Set<ExternalResourceProviderWorker> workers = new HashSet<>();
        workers.add(worker);

        final CompositeExternalResourceProviderService testSubject = new CompositeExternalResourceProviderService("testService", workers, new CountDownLatch(0));
        testSubject.start();

        Assertions.assertTrue(worker.isRunning());

        testSubject.stop();

        Assertions.assertFalse(worker.isRunning());
        Assertions.assertNotEquals(threadName, worker.threadName);
    }

    private static class TestExternalResourceProviderWorker implements ExternalResourceProviderWorker {
        volatile String threadName;
        volatile boolean stopped;

        @Override
        public String getName() {
            return "name";
        }

        @Override
        public ExternalResourceProvider getProvider() {
            return Mockito.mock(ExternalResourceProvider.class);
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
                    Assertions.fail();
                    stopped = true;
                }
            }
        }
    }
}