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
package org.apache.nifi.kafka.shared.util;

import java.util.function.Supplier;

/**
 * Abstract away details of {@link Object#wait()} / {@link Object#notify()} mechanics.
 */
public class Notifier {
    private final Object object;

    public Notifier() {
        this.object = new Object();
    }

    public boolean waitForCondition(final Supplier<Boolean> conditionComplete, final long timeout) {
        boolean isInterrupted = false;
        final long millisTimeout = System.currentTimeMillis() + timeout;
        final Supplier<Boolean> conditionTimeout = () -> (System.currentTimeMillis() >= millisTimeout);
        while (!conditionComplete.get() && !conditionTimeout.get() && !isInterrupted) {
            isInterrupted = waitUntil(millisTimeout);
        }
        return conditionComplete.get();
    }

    private boolean waitUntil(final long millisTimeout) {
        boolean interrupted = false;
        final long millisNow = System.currentTimeMillis();
        if (millisNow < millisTimeout) {
            interrupted = waitFor(millisTimeout - millisNow);
        }
        return interrupted;
    }

    private boolean waitFor(final long millis) {
        boolean interrupted = false;
        synchronized (object) {
            try {
                object.wait(millis);
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        return interrupted;
    }

    public void notifyWaiter() {
        synchronized (object) {
            object.notify();
        }
    }
}
