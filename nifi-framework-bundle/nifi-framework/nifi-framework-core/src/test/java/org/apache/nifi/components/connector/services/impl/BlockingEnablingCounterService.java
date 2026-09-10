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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class BlockingEnablingCounterService extends AbstractControllerService implements CounterService {

    private final AtomicInteger enableCounter = new AtomicInteger();
    private final AtomicInteger disableCounter = new AtomicInteger();
    private final CountDownLatch firstEnableRelease = new CountDownLatch(1);
    private final CountDownLatch enableInterrupted = new CountDownLatch(1);
    private final CountDownLatch disableStarted = new CountDownLatch(1);
    private final CountDownLatch disableRelease = new CountDownLatch(1);
    private final List<String> lifecycleEvents = new CopyOnWriteArrayList<>();
    private volatile boolean ignoreEnableInterrupt;
    private volatile boolean blockDisable;

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InterruptedException {
        final int invocation = enableCounter.incrementAndGet();
        lifecycleEvents.add("enable-started-" + invocation);
        if (invocation == 1) {
            while (firstEnableRelease.getCount() > 0) {
                try {
                    firstEnableRelease.await();
                } catch (final InterruptedException e) {
                    enableInterrupted.countDown();
                    lifecycleEvents.add("enable-interrupted-" + invocation);
                    if (!ignoreEnableInterrupt) {
                        throw e;
                    }
                }
            }
        }

        lifecycleEvents.add("enable-finished-" + invocation);
    }

    @OnDisabled
    public void onDisabled(final ConfigurationContext context) throws InterruptedException {
        final int invocation = disableCounter.incrementAndGet();
        lifecycleEvents.add("disable-started-" + invocation);
        disableStarted.countDown();
        if (blockDisable) {
            disableRelease.await();
        }

        lifecycleEvents.add("disable-finished-" + invocation);
    }

    public int enableInvocationCount() {
        return enableCounter.get();
    }

    public int disableInvocationCount() {
        return disableCounter.get();
    }

    public void setIgnoreEnableInterrupt(final boolean ignoreEnableInterrupt) {
        this.ignoreEnableInterrupt = ignoreEnableInterrupt;
    }

    public void setBlockDisable(final boolean blockDisable) {
        this.blockDisable = blockDisable;
    }

    public boolean awaitEnableInterrupted(final long timeout, final TimeUnit timeUnit) throws InterruptedException {
        return enableInterrupted.await(timeout, timeUnit);
    }

    public boolean awaitDisableStarted(final long timeout, final TimeUnit timeUnit) throws InterruptedException {
        return disableStarted.await(timeout, timeUnit);
    }

    public List<String> getLifecycleEvents() {
        return List.copyOf(lifecycleEvents);
    }

    public void releaseFirstEnable() {
        firstEnableRelease.countDown();
    }

    public void releaseDisable() {
        disableRelease.countDown();
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
