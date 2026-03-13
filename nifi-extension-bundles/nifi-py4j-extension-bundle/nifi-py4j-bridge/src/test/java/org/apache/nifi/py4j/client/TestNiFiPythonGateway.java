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

package org.apache.nifi.py4j.client;

import org.apache.nifi.py4j.client.NiFiPythonGateway.InvocationBindings;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import py4j.CallbackClient;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;

public class TestNiFiPythonGateway {

    private static final Method NOP_METHOD;

    static {
        try {
            NOP_METHOD = TestNiFiPythonGateway.class.getMethod("nop", Object[].class);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    private JavaObjectBindings bindings;
    private NiFiPythonGateway gateway;

    @BeforeEach
    public void setup() {
        bindings = new JavaObjectBindings();
        gateway = new NiFiPythonGateway(bindings, this, mock(CallbackClient.class)) {
            @Override
            protected boolean isUnbind(final Method method) {
                return true;
            }
        };
    }

    @Test
    public void testObjectBoundUnboundWithSingleInvocation() {
        final Object[] args = new Object[] {new Object()};
        final InvocationBindings invocationBindings = gateway.beginInvocation("o123", NOP_METHOD, args);
        final String objectId = gateway.putNewObject(args[0]);
        final List<String> objectIds = invocationBindings.getObjectIds();
        assertEquals(List.of(objectId), objectIds);
        assertEquals(args[0], gateway.getObject(objectId));

        gateway.endInvocation(invocationBindings);
        gateway.deleteObject(objectId);
        assertNull(gateway.getObject(objectId));
    }

    @Test
    public void testObjectBoundNotUnboundWhileInvocationActive() {
        final Object[] args = new Object[] {new Object()};
        final InvocationBindings invocationBindings = gateway.beginInvocation("o123", NOP_METHOD, args);
        final String objectId = gateway.putNewObject(args[0]);
        final List<String> objectIds = invocationBindings.getObjectIds();
        assertEquals(List.of(objectId), objectIds);
        assertEquals(args[0], gateway.getObject(objectId));

        gateway.deleteObject(objectId);

        // gateway.deleteObject should not remove the value because the invocation is still active
        assertEquals(args[0], gateway.getObject(objectId));

        // After calling endInvocation, the object should be cleaned up.
        gateway.endInvocation(invocationBindings);
        assertNull(gateway.getObject(objectId));
    }

    @Test
    public void testEndInvocationDifferentThread() throws InterruptedException {
        final Object[] args = new Object[] {new Object()};
        final InvocationBindings invocationBindings = gateway.beginInvocation("o123", NOP_METHOD, args);
        final String objectId = gateway.putNewObject(args[0]);
        final List<String> objectIds = invocationBindings.getObjectIds();
        assertEquals(List.of(objectId), objectIds);
        assertEquals(args[0], gateway.getObject(objectId));

        gateway.deleteObject(objectId);

        // gateway.deleteObject should not remove the value because the invocation is still active
        assertEquals(args[0], gateway.getObject(objectId));

        Thread.ofVirtual().start(() -> {
            gateway.endInvocation(invocationBindings);
        }).join();

        assertNull(gateway.getObject(objectId));
    }

    @Test
    public void testMultipleInvocationsActive() {
        final Object[] args = new Object[] {new Object()};

        // Simulate 5 different threads making invocations into the Python process
        final List<InvocationBindings> bindings = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            final InvocationBindings invocationBindings = gateway.beginInvocation("o123", NOP_METHOD, args);
            bindings.add(invocationBindings);
        }

        // Create an object while there are 5 invocations active. We don't know which invocation caused
        // the object to be created, so we can't unbind it until all invocations are complete.
        final String objectId = gateway.putNewObject(args[0]);

        // Simulate Python process garbage collecting the object after 2 of the invocations are complete
        gateway.endInvocation(bindings.removeFirst());
        gateway.endInvocation(bindings.removeFirst());
        gateway.deleteObject(objectId);

        // We should now be able to add additional invocations, and they should not prevent the already-bound
        // object from being cleaned up.
        for (int i = 0; i < 3; i++) {
            gateway.beginInvocation("o123", NOP_METHOD, args);
        }

        // As each of the invocations complete, we should find the object is still bound
        for (final InvocationBindings invocationBindings : bindings) {
            assertNotNull(gateway.getObject(objectId));
            gateway.endInvocation(invocationBindings);
        }

        // When the final invocation completes, the object should be unbound
        assertNull(gateway.getObject(objectId));
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testConcurrentVirtualThreadsDoNotDeadlock() throws InterruptedException {
        final int threadCount = 200;
        final int iterationsPerThread = 50;
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch doneLatch = new CountDownLatch(threadCount);
        final AtomicInteger completedCount = new AtomicInteger(0);
        final AtomicInteger errorCount = new AtomicInteger(0);

        for (int t = 0; t < threadCount; t++) {
            Thread.ofVirtual().name("gateway-test-" + t).start(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < iterationsPerThread; i++) {
                        final Object[] args = new Object[]{new Object()};
                        final InvocationBindings invocationBindings = gateway.beginInvocation("o" + Thread.currentThread().getName(), NOP_METHOD, args);
                        gateway.putNewObject(args[0]);
                        gateway.endInvocation(invocationBindings);
                    }
                    completedCount.incrementAndGet();
                } catch (final Exception e) {
                    errorCount.incrementAndGet();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        final boolean allFinished = doneLatch.await(25, TimeUnit.SECONDS);

        assertEquals(true, allFinished, "Not all virtual threads completed within the timeout, possible deadlock");
        assertEquals(threadCount, completedCount.get());
        assertEquals(0, errorCount.get());
    }

    public void nop(Object... args) {
    }
}
