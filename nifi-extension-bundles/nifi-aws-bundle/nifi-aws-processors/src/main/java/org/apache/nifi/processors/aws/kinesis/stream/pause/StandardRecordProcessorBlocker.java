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

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.function.Supplier;

/**
 * Standard implementation of {@link RecordProcessorBlocker} that allows blocking calls to {@link #await()} by:
 * <ul>
 *     <li>explicit call to {@link #block()}</li>
 *     <li>timeout since last call to {@link #unblock()}</li>
 * </ul>
 * Unblocking is done by calling {@link #unblock()} or {@link #unblockIndefinitely()}. The latter is used to disable the timeout mechanism to allow shutdown of background threads that could
 * otherwise block indefinitely due to no calls to {@link #unblock()}.
 */
public class StandardRecordProcessorBlocker implements RecordProcessorBlocker {

    static final Duration BLOCK_AFTER_DURATION = Duration.ofSeconds(2);

    private final Supplier<Long> timestampProvider;
    private long timeoutBase;
    private CountDownLatch blocker = new CountDownLatch(0);

    public static StandardRecordProcessorBlocker create() {
        return new StandardRecordProcessorBlocker(System::currentTimeMillis);
    }

    protected StandardRecordProcessorBlocker(final Supplier<Long> timestampProvider) {
        this.timestampProvider = timestampProvider;
        this.timeoutBase = timestampProvider.get();
    }

    @Override
    public void await() throws InterruptedException {
        final long nowTimestamp = timestampProvider.get();
        if (!isAfterTimeout(nowTimestamp)) {
            return;
        }
        synchronized (this) {
            final CountDownLatch localTimeoutBlocker = blocker;
            if (isAfterTimeout(nowTimestamp) && localTimeoutBlocker.getCount() == 0) {
                localTimeoutBlocker.countDown();
                blocker = new CountDownLatch(1);
            }
        }
        blocker.await();
    }

    private boolean isAfterTimeout(final long nowMillis) {
        return nowMillis - timeoutBase > BLOCK_AFTER_DURATION.toMillis();
    }

    /**
     * Blocks subsequent calls to {@link #await()} until {@link #unblock()} or {@link #unblockIndefinitely()} is called.
     */
    public synchronized void block() {
        timeoutBase = 0;
    }

    /**
     * Unblocks the blocker and enables the timeout mechanism.
     */
    public void unblock() {
        timeoutBase = timestampProvider.get();
        blocker.countDown();
    }

    /**
     * Unblocks the blocker and disables the timeout mechanism. Subsequent calls to {@link #unblock()} will re-enable the timeout mechanism.
     */
    public void unblockIndefinitely() {
        timeoutBase = Long.MAX_VALUE;
        blocker.countDown();
    }
}
