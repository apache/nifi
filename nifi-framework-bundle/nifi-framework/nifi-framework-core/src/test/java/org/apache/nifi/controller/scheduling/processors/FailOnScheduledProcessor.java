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

package org.apache.nifi.controller.scheduling.processors;

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.connector.services.CounterService;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class FailOnScheduledProcessor extends AbstractProcessor {

    public static final PropertyDescriptor MANAGED_SERVICE = new PropertyDescriptor.Builder()
        .name("Managed Service")
        .description("Managed Controller Service used to verify lifecycle ordering")
        .identifiesControllerService(CounterService.class)
        .required(false)
        .build();

    private final AtomicInteger invocationCount = new AtomicInteger();
    private final AtomicBoolean succeeded = new AtomicBoolean();
    private volatile int desiredFailureCount = 1;
    private volatile long onScheduledSleepMillis = 0L;
    private volatile int onScheduledSleepIterations = 0;
    private volatile boolean allowSleepInterrupt = true;

    public void setDesiredFailureCount(final int desiredFailureCount) {
        this.desiredFailureCount = desiredFailureCount;
    }

    public void setOnScheduledSleepDuration(final long duration, final TimeUnit unit, final boolean allowInterrupt, final int iterations) {
        this.onScheduledSleepMillis = unit.toMillis(duration);
        this.onScheduledSleepIterations = iterations;
        this.allowSleepInterrupt = allowInterrupt;
    }

    public void setAllowSleepInterrupt(final boolean allow) {
        this.allowSleepInterrupt = allow;
    }

    @OnScheduled
    public void onScheduled() throws InterruptedException {
        final int invocation = invocationCount.incrementAndGet();

        if (invocation <= onScheduledSleepIterations && onScheduledSleepMillis > 0L) {
            final long sleepFinish = System.currentTimeMillis() + onScheduledSleepMillis;

            while (System.currentTimeMillis() < sleepFinish) {
                try {
                    Thread.sleep(Math.max(0, sleepFinish - System.currentTimeMillis()));
                } catch (final InterruptedException e) {
                    if (allowSleepInterrupt) {
                        Thread.currentThread().interrupt();
                        throw e;
                    }
                }
            }
        }

        if (invocation < desiredFailureCount) {
            throw new ProcessException("Intentional failure for unit test");
        } else {
            succeeded.set(true);
        }
    }

    public int getOnScheduledInvocationCount() {
        return invocationCount.get();
    }

    public boolean isSucceeded() {
        return succeeded.get();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return List.of(MANAGED_SERVICE);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
    }
}
