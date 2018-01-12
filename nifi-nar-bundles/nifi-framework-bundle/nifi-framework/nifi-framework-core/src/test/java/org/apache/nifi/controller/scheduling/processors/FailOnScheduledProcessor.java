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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.exception.ProcessException;

public class FailOnScheduledProcessor extends AbstractProcessor {

    private volatile int invocationCount = 0;
    private volatile int desiredFailureCount = 1;
    private volatile long onScheduledSleepMillis = 0L;
    private volatile int onScheduledSleepIterations = 0;
    private volatile boolean allowSleepInterrupt = true;
    private final AtomicBoolean succeeded = new AtomicBoolean();

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
        invocationCount++;

        if (invocationCount <= onScheduledSleepIterations && onScheduledSleepMillis > 0L) {
            final long sleepFinish = System.currentTimeMillis() + onScheduledSleepMillis;

            while (System.currentTimeMillis() < sleepFinish) {
                try {
                    Thread.sleep(Math.max(0, sleepFinish - System.currentTimeMillis()));
                } catch (final InterruptedException ie) {
                    if (allowSleepInterrupt) {
                        Thread.currentThread().interrupt();
                        throw ie;
                    } else {
                        continue;
                    }
                }
            }
        }

        if (invocationCount < desiredFailureCount) {
            throw new ProcessException("Intentional failure for unit test");
        } else {
            succeeded.set(true);
        }
    }

    public int getOnScheduledInvocationCount() {
        return invocationCount;
    }

    public boolean isSucceess() {
        return succeeded.get();
    }

    @Override
    public void onTrigger(org.apache.nifi.processor.ProcessContext context, org.apache.nifi.processor.ProcessSession session) throws ProcessException {
    }
}
