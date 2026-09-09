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

public class BlockingEnablingCounterService extends AbstractControllerService implements CounterService {

    private final AtomicInteger enableCounter = new AtomicInteger();
    private final AtomicInteger disableCounter = new AtomicInteger();
    private final AtomicBoolean interrupted = new AtomicBoolean();
    private final CountDownLatch enableRelease = new CountDownLatch(1);

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InterruptedException {
        enableCounter.incrementAndGet();
        try {
            enableRelease.await(30, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
            interrupted.set(true);
            Thread.currentThread().interrupt();
            throw e;
        }
    }

    @OnDisabled
    public void onDisabled() {
        disableCounter.incrementAndGet();
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

    public void releaseEnable() {
        enableRelease.countDown();
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
