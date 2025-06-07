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
 * Standard implementation of {@link RecordProcessorBlocker} that allows blocking calls to {@link #await()} by:
 * <ul>
 *     <li>explicit call to {@link #block()}</li>
 *     <li>timeout since last call to {@link #unblock()}</li>
 * </ul>
 * Unblocking is done by calling {@link #unblock()} or {@link #unblockAndDisableTimeout()}. The latter is used to disable the timeout mechanism to allow shutdown of background threads that could
 * otherwise block indefinitely due to no calls to {@link #unblock()}.
 */
public class StandardRecordProcessorBlocker implements RecordProcessorBlocker {

    private CountDownLatch blocker = new CountDownLatch(0);

    static final long BLOCK_AFTER_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(2);
    private final Supplier<Long> timestampProvider;
    private long timeoutAnchor = 0L;
    private CountDownLatch timeoutBlocker = new CountDownLatch(0);

    public static StandardRecordProcessorBlocker create() {
        return new StandardRecordProcessorBlocker(System::currentTimeMillis);
    }

    protected StandardRecordProcessorBlocker(final Supplier<Long> timestampProvider) {
        this.timestampProvider = timestampProvider;
    }

    public void await() throws InterruptedException {
        blocker.await();
        awaitDueToTimeoutIfNeeded();
    }

    private void awaitDueToTimeoutIfNeeded() throws InterruptedException {
        final long nowTimestamp = timestampProvider.get();
        if (!shouldBlockDueToTimeout(nowTimestamp)) {
            return;
        }
        synchronized (this) {
            final CountDownLatch localTimeoutBlocker = timeoutBlocker;
            if (shouldBlockDueToTimeout(nowTimestamp) && localTimeoutBlocker.getCount() == 0) {
                localTimeoutBlocker.countDown();
                timeoutBlocker = new CountDownLatch(1);
            }
        }
        timeoutBlocker.await();
    }

    private boolean shouldBlockDueToTimeout(final long nowMillis) {
        return nowMillis - timeoutAnchor > BLOCK_AFTER_TIMEOUT_MILLIS;
    }

    public synchronized void block() {
        blocker = blocker.getCount() > 0
                ? blocker
                : new CountDownLatch(1);
    }

    public void unblock() {
        unblockPrivate(timestampProvider.get());
    }

    public void unblockAndDisableTimeout() {
        unblockPrivate(Long.MAX_VALUE);
    }

    private synchronized void unblockPrivate(final long newTimeoutAnchor) {
        blocker.countDown();
        timeoutAnchor = newTimeoutAnchor;
        timeoutBlocker.countDown();
    }
}
