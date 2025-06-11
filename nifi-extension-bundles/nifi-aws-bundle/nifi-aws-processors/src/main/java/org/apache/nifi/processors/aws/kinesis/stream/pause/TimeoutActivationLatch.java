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
package org.apache.nifi.processors.aws.kinesis.stream.pause;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * A utility class that provides a mechanism to block calls to {@link #await()} that happen after a certain timeout has passed since the last call to {@link #pushback()}.
 */
class TimeoutActivationLatch {

    private final Supplier<Long> timestampProvider;
    private final long timeoutMillis;
    private long timeoutBase = 0L;
    private CountDownLatch latch = new CountDownLatch(0);

    TimeoutActivationLatch(Supplier<Long> timestampProvider, TimeUnit unit, long timeout) {
        this.timestampProvider = timestampProvider;
        this.timeoutMillis = unit.toMillis(timeout);
    }

    void await() throws InterruptedException {
        final long nowTimestamp = timestampProvider.get();
        if (!isAfterTimeout(nowTimestamp)) {
            return;
        }
        synchronized (this) {
            final CountDownLatch localTimeoutBlocker = latch;
            if (isAfterTimeout(nowTimestamp) && localTimeoutBlocker.getCount() == 0) {
                localTimeoutBlocker.countDown();
                latch = new CountDownLatch(1);
            }
        }
        latch.await();
    }

    private boolean isAfterTimeout(final long nowMillis) {
        return nowMillis - timeoutBase > timeoutMillis;
    }

    /**
     * Unblocks the latch and pushes back the timeout activation moment.
     */
    void pushback() {
        timeoutBase = timestampProvider.get();
        latch.countDown();
    }

    /**
     * Unblocks the latch and disables the timeout mechanism until next call to {@link #pushback()}.
     */
    void disable() {
        timeoutBase = Long.MAX_VALUE;
        latch.countDown();
    }
}
