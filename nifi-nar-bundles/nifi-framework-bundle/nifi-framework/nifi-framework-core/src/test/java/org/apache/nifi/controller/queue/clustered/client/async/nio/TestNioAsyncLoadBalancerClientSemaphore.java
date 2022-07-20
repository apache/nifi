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
package org.apache.nifi.controller.queue.clustered.client.async.nio;

import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;


@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class TestNioAsyncLoadBalancerClientSemaphore {

    @Mock
    RegisteredPartition partition1;

    @Mock
    RegisteredPartition partition2;

    @BeforeEach
    public void setup() {
        Mockito.when(partition1.getNodeIdentifier()).thenReturn(Mockito.mock(NodeIdentifier.class));
        Mockito.when(partition2.getNodeIdentifier()).thenReturn(Mockito.mock(NodeIdentifier.class));
    }

    @Test
    public void testMultipleAcquiresAndReleases() {
        final NioAsyncLoadBalancerClientSemaphore testSubject = new NioAsyncLoadBalancerClientSemaphore();

        final Optional<NioAsyncLoadBalancerClientSemaphore.Reservation> result1 = testSubject.acquire(partition1);
        Assertions.assertTrue(result1.isPresent());
        result1.get().release();

        final Optional<NioAsyncLoadBalancerClientSemaphore.Reservation> result2 = testSubject.acquire(partition1);
        Assertions.assertTrue(result2.isPresent());
        result2.get().release();
    }

    @Test
    public void testAcquireMultipleTimes() {
        final NioAsyncLoadBalancerClientSemaphore testSubject = new NioAsyncLoadBalancerClientSemaphore();

        final Optional<NioAsyncLoadBalancerClientSemaphore.Reservation> result1 = testSubject.acquire(partition1);
        Assertions.assertTrue(result1.isPresent());

        final Optional<NioAsyncLoadBalancerClientSemaphore.Reservation> result2 = testSubject.acquire(partition1);
        Assertions.assertFalse(result2.isPresent());

        result1.get().release();

        final Optional<NioAsyncLoadBalancerClientSemaphore.Reservation> result3 = testSubject.acquire(partition1);
        Assertions.assertTrue(result3.isPresent());
        result3.get().release();
    }

    @Test
    public void testMultiplePartitions() {
        final NioAsyncLoadBalancerClientSemaphore testSubject = new NioAsyncLoadBalancerClientSemaphore();

        final Optional<NioAsyncLoadBalancerClientSemaphore.Reservation> result1 = testSubject.acquire(partition1);
        Assertions.assertTrue(result1.isPresent());

        final Optional<NioAsyncLoadBalancerClientSemaphore.Reservation> result2 = testSubject.acquire(partition2);
        Assertions.assertTrue(result2.isPresent());
    }

    @Test
    public void testReleaseAnAlreadyUnacquiredPartition() {
        final NioAsyncLoadBalancerClientSemaphore testSubject = new NioAsyncLoadBalancerClientSemaphore();
        final Optional<NioAsyncLoadBalancerClientSemaphore.Reservation> result = testSubject.acquire(partition1);
        result.get().release();
        result.get().release();
    }

    @Timeout(value = 1)
    @Test
    public void testConcurrentUsage() throws Exception {
        final NioAsyncLoadBalancerClientSemaphore testSubject = new NioAsyncLoadBalancerClientSemaphore();

        final CountDownLatch step2latch = new CountDownLatch(1);
        final CountDownLatch step3latch = new CountDownLatch(1);
        final CountDownLatch step4latch = new CountDownLatch(1);

        final AtomicReference<Optional<NioAsyncLoadBalancerClientSemaphore.Reservation>> step1result = new AtomicReference<>();
        final AtomicReference<Optional<NioAsyncLoadBalancerClientSemaphore.Reservation>> step2result = new AtomicReference<>();
        final AtomicReference<Optional<NioAsyncLoadBalancerClientSemaphore.Reservation>> step4result = new AtomicReference<>();

        final Runnable client1 = new Runnable() {
            @Override
            public void run() {
                // Step 1
                step1result.set(testSubject.acquire(partition1));

                try {
                    step2latch.await();
                } catch (final InterruptedException e) {
                    Assertions.fail(e);
                }

                // Step 3
                step1result.get().get().release();
                step3latch.countDown();
            }
        };

        final Runnable client2 = new Runnable() {
            @Override
            public void run() {
                // Step 2
                step2result.set(testSubject.acquire(partition1));
                step2latch.countDown();

                try {
                    step3latch.await();
                } catch (final InterruptedException e) {
                    Assertions.fail(e);
                }

                // Step 4
                step4result.set(testSubject.acquire(partition1));
                step4latch.countDown();

            }
        };

        final Thread thread1 = new Thread(client1);
        final Thread thread2 = new Thread(client2);

        thread1.start();
        thread2.start();

        step4latch.await();
        Assertions.assertTrue(step1result.get().isPresent());
        Assertions.assertFalse(step2result.get().isPresent());
        Assertions.assertTrue(step4result.get().isPresent());
    }
}
