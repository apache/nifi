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
 * Unblocking is done by calling {@link #unblock()} or {@link #unblockInfinitely()}. The latter is used to disable the timeout mechanism to allow shutdown of background threads that could
 * otherwise block indefinitely due to no calls to {@link #unblock()}.
 */
public class StandardRecordProcessorBlocker implements RecordProcessorBlocker {

    static final long BLOCK_AFTER_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(2);

    private final TimeoutActivationLatch timeoutActivationLatch;
    private CountDownLatch countDownLatch = new CountDownLatch(0);

    public static StandardRecordProcessorBlocker create() {
        return new StandardRecordProcessorBlocker(System::currentTimeMillis);
    }

    protected StandardRecordProcessorBlocker(final Supplier<Long> timestampProvider) {
        timeoutActivationLatch = new TimeoutActivationLatch(timestampProvider, TimeUnit.SECONDS, TimeUnit.MILLISECONDS.toSeconds(BLOCK_AFTER_TIMEOUT_MILLIS));
    }

    public void await() throws InterruptedException {
        countDownLatch.await();
        timeoutActivationLatch.await();
    }

    /**
     * Blocks subsequent calls to {@link #await()} until {@link #unblock()} or {@link #unblockInfinitely()} is called.
     */
    public synchronized void block() {
        countDownLatch = countDownLatch.getCount() > 0
                ? countDownLatch
                : new CountDownLatch(1);
    }

    /**
     * Unblocks the blocker and enables the timeout mechanism.
     */
    public void unblock() {
        countDownLatch.countDown();
        timeoutActivationLatch.pushback();
    }

    /**
     * Unblocks the blocker and disables the timeout mechanism. Subsequent calls to {@link #unblock()} will re-enable the timeout mechanism.
     */
    public void unblockInfinitely() {
        countDownLatch.countDown();
        timeoutActivationLatch.disable();
    }
}
