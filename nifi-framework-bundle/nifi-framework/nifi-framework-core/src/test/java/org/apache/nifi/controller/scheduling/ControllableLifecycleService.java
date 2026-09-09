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

package org.apache.nifi.controller.scheduling;

import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A Controller Service whose lifecycle methods can be externally controlled by a test. When configured to block on
 * enable, its @OnEnabled method blocks until released and ignores interruption, recording that it was interrupted. When
 * configured to block on disable, its @OnDisabled method blocks until released. This allows a test to observe the service
 * while it is DISABLING, both while an interruption-resistant @OnEnabled is still running and while @OnDisabled is running.
 */
public class ControllableLifecycleService extends AbstractControllerService {

    private final AtomicInteger enableCounter = new AtomicInteger();
    private final AtomicInteger disableCounter = new AtomicInteger();
    private final AtomicBoolean interrupted = new AtomicBoolean();
    private final CountDownLatch enableRelease = new CountDownLatch(1);
    private final CountDownLatch disableRelease = new CountDownLatch(1);

    private volatile boolean blockOnEnable = false;
    private volatile boolean blockOnDisable = false;

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        enableCounter.incrementAndGet();
        if (blockOnEnable) {
            awaitIgnoringInterruption(enableRelease);
        }
    }

    @OnDisabled
    public void onDisabled() {
        disableCounter.incrementAndGet();
        if (blockOnDisable) {
            awaitIgnoringInterruption(disableRelease);
        }
    }

    private void awaitIgnoringInterruption(final CountDownLatch latch) {
        boolean released = false;
        while (!released) {
            try {
                released = latch.await(30, TimeUnit.SECONDS);
                if (!released) {
                    // Safety limit so that a test which never releases the latch fails on its own timeout rather than hanging.
                    return;
                }
            } catch (final InterruptedException e) {
                interrupted.set(true);
            }
        }
    }

    public void setBlockOnEnable(final boolean blockOnEnable) {
        this.blockOnEnable = blockOnEnable;
    }

    public void setBlockOnDisable(final boolean blockOnDisable) {
        this.blockOnDisable = blockOnDisable;
    }

    public void releaseEnable() {
        enableRelease.countDown();
    }

    public void releaseDisable() {
        disableRelease.countDown();
    }

    public int enableInvocationCount() {
        return enableCounter.get();
    }

    public int disableInvocationCount() {
        return disableCounter.get();
    }

    public boolean wasInterrupted() {
        return interrupted.get();
    }
}
