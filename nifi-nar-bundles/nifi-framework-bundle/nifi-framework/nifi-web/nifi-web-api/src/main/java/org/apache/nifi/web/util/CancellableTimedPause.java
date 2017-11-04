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

package org.apache.nifi.web.util;

import java.util.concurrent.TimeUnit;

public class CancellableTimedPause implements Pause {
    private final long expirationNanoTime;
    private final long pauseNanos;
    private volatile boolean cancelled = false;

    public CancellableTimedPause(final long pauseTime, final long expirationPeriod, final TimeUnit timeUnit) {
        final long expirationNanos = TimeUnit.NANOSECONDS.convert(expirationPeriod, timeUnit);
        final long expirationTime = System.nanoTime() + expirationNanos;
        expirationNanoTime = expirationTime < 0 ? Long.MAX_VALUE : expirationTime;
        pauseNanos = Math.max(1L, TimeUnit.NANOSECONDS.convert(pauseTime, timeUnit));
    }

    public void cancel() {
        cancelled = true;
    }

    @Override
    public boolean pause() {
        if (cancelled) {
            return false;
        }

        long sysTime = System.nanoTime();
        final long maxWaitTime = System.nanoTime() + pauseNanos;
        while (sysTime < maxWaitTime) {
            try {
                TimeUnit.NANOSECONDS.sleep(pauseNanos);
            } catch (final InterruptedException ie) {
                Thread.currentThread().interrupt();
                return false;
            }

            sysTime = System.nanoTime();
        }

        return sysTime < expirationNanoTime && !cancelled;
    }

}
