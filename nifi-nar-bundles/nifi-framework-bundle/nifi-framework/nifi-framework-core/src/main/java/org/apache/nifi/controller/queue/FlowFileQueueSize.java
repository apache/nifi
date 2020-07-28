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

package org.apache.nifi.controller.queue;

public class FlowFileQueueSize {
    private final int activeQueueCount;
    private final long activeQueueBytes;
    private final int swappedCount;
    private final long swappedBytes;
    private final int swapFiles;
    private final int unacknowledgedCount;
    private final long unacknowledgedBytes;

    public FlowFileQueueSize(final int activeQueueCount, final long activeQueueBytes, final int swappedCount, final long swappedBytes, final int swapFileCount,
        final int unacknowledgedCount, final long unacknowledgedBytes) {
        this.activeQueueCount = activeQueueCount;
        this.activeQueueBytes = activeQueueBytes;
        this.swappedCount = swappedCount;
        this.swappedBytes = swappedBytes;
        this.swapFiles = swapFileCount;
        this.unacknowledgedCount = unacknowledgedCount;
        this.unacknowledgedBytes = unacknowledgedBytes;
    }

    public int getSwappedCount() {
        return swappedCount;
    }

    public long getSwappedBytes() {
        return swappedBytes;
    }

    public int getSwapFileCount() {
        return swapFiles;
    }

    public int getActiveCount() {
        return activeQueueCount;
    }

    public long getActiveBytes() {
        return activeQueueBytes;
    }

    public int getUnacknowledgedCount() {
        return unacknowledgedCount;
    }

    public long getUnacknowledgedBytes() {
        return unacknowledgedBytes;
    }

    public boolean isEmpty() {
        return activeQueueCount == 0 && swappedCount == 0 && unacknowledgedCount == 0;
    }

    public QueueSize toQueueSize() {
        return new QueueSize(activeQueueCount + swappedCount + unacknowledgedCount, activeQueueBytes + swappedBytes + unacknowledgedBytes);
    }

    public QueueSize activeQueueSize() {
        return new QueueSize(activeQueueCount, activeQueueBytes);
    }

    public QueueSize unacknowledgedQueueSize() {
        return new QueueSize(unacknowledgedCount, unacknowledgedBytes);
    }

    public QueueSize swapQueueSize() {
        return new QueueSize(swappedCount, swappedBytes);
    }

    @Override
    public String toString() {
        return "FlowFile Queue Size[ ActiveQueue=[" + activeQueueCount + ", " + activeQueueBytes +
            " Bytes], Swap Queue=[" + swappedCount + ", " + swappedBytes +
            " Bytes], Swap Files=[" + swapFiles + "], Unacknowledged=[" + unacknowledgedCount + ", " + unacknowledgedBytes + " Bytes] ]";
    }
}
