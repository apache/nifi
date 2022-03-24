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

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;

public class LongEnablingService extends AbstractControllerService {

    private final AtomicInteger enableCounter = new AtomicInteger();
    private final AtomicInteger disableCounter = new AtomicInteger();

    private volatile long limit;

    @OnEnabled
    public void enable(final ConfigurationContext context) throws Exception {
        this.enableCounter.incrementAndGet();
        Thread.sleep(limit);
    }

    @OnDisabled
    public void disable(final ConfigurationContext context) {
        this.disableCounter.incrementAndGet();
    }

    public int enableInvocationCount() {
        return this.enableCounter.get();
    }

    public int disableInvocationCount() {
        return this.disableCounter.get();
    }

    public void setLimit(final long limit) {
        this.limit = limit;
    }
}
