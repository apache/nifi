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

package org.apache.nifi.components.connector.services.impl;

import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.connector.services.CounterService;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A Controller Service whose @OnEnabled and @OnDisabled methods block until released by the test and ignore interruption.
 * Because @OnEnabled ignores interruption, a disable initiated during enabling leaves the service DISABLING while the
 * first @OnEnabled invocation is still running, which lets a test observe that @OnDisabled has not yet started and that no
 * second @OnEnabled has begun. Releasing @OnEnabled lets the service invoke @OnDisabled, which then blocks until released,
 * keeping the service DISABLING until the test releases it. This models the full enable, disable, re-enable ordering.
 */
public class BlockingEnableDisableCounterService extends AbstractControllerService implements CounterService {

    private final AtomicInteger enableCounter = new AtomicInteger();
    private final AtomicInteger disableCounter = new AtomicInteger();
    private final AtomicBoolean interrupted = new AtomicBoolean();
    private final CountDownLatch enableRelease = new CountDownLatch(1);
    private final CountDownLatch disableRelease = new CountDownLatch(1);

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        enableCounter.incrementAndGet();
        awaitIgnoringInterruption(enableRelease);
    }

    @OnDisabled
    public void onDisabled() {
        disableCounter.incrementAndGet();
        awaitIgnoringInterruption(disableRelease);
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
                // Record the interruption but keep running until the latch is explicitly released.
                interrupted.set(true);
            }
        }
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

    @Override
    public long increment() {
        return 0;
    }

    @Override
    public long getCount() {
        return 0;
    }
}
