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

package org.apache.nifi.connectable;

import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicReference;

public class ConnectableFlowFileActivity implements FlowFileActivity {
    private volatile long latestActivityTime = -1L;
    private final AtomicReference<FlowFileTransferCounts> transferCounts = new AtomicReference<>(new FlowFileTransferCounts());

    @Override
    public void updateLatestActivityTime() {
        latestActivityTime = System.currentTimeMillis();
    }

    @Override
    public void updateTransferCounts(final int receivedCount, final long receivedBytes, final int sentCount, final long sentBytes) {
        if (receivedCount < 0) {
            throw new IllegalArgumentException("Received count cannot be negative: " + receivedCount);
        }
        if (receivedBytes < 0L) {
            throw new IllegalArgumentException("Received bytes cannot be negative: " + receivedBytes);
        }
        if (sentCount < 0) {
            throw new IllegalArgumentException("Sent count cannot be negative: " + sentCount);
        }
        if (sentBytes < 0L) {
            throw new IllegalArgumentException("Sent bytes cannot be negative: " + sentBytes);
        }

        boolean updated = false;
        while (!updated) {
            final FlowFileTransferCounts currentCounts = transferCounts.get();
            final FlowFileTransferCounts updatedCounts = currentCounts.plus(receivedCount, receivedBytes, sentCount, sentBytes);
            updated = transferCounts.compareAndSet(currentCounts,  updatedCounts);
        }
    }

    @Override
    public void reset() {
        latestActivityTime = -1L;
        transferCounts.set(new FlowFileTransferCounts());
    }

    @Override
    public OptionalLong getLatestActivityTime() {
        return latestActivityTime == -1L ? OptionalLong.empty() : OptionalLong.of(latestActivityTime);
    }

    @Override
    public FlowFileTransferCounts getTransferCounts() {
        return transferCounts.get();
    }
}
