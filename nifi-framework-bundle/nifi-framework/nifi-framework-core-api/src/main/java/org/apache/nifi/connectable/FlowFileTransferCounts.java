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

public class FlowFileTransferCounts {
    private final long receivedCount;
    private final long receivedBytes;
    private final long sentCount;
    private final long sentBytes;

    public FlowFileTransferCounts() {
        this(0, 0L, 0, 0L);
    }

    public FlowFileTransferCounts(final long receivedCount, final long receivedBytes, final long sentCount, final long sentBytes) {
        this.receivedCount = receivedCount;
        this.receivedBytes = receivedBytes;
        this.sentCount = sentCount;
        this.sentBytes = sentBytes;
    }

    public long getReceivedCount() {
        return receivedCount;
    }

    public long getReceivedBytes() {
        return receivedBytes;
    }

    public long getSentCount() {
        return sentCount;
    }

    public long getSentBytes() {
        return sentBytes;
    }

    public FlowFileTransferCounts plus(final long additionalReceivedCount, final long additionalReceivedBytes, final long additionalSentCount, final long additionalSentBytes) {
        return new FlowFileTransferCounts(
            this.receivedCount + additionalReceivedCount,
            this.receivedBytes + additionalReceivedBytes,
            this.sentCount + additionalSentCount,
            this.sentBytes + additionalSentBytes
        );
    }

    public FlowFileTransferCounts plus(final FlowFileTransferCounts other) {
        return new FlowFileTransferCounts(
            this.receivedCount + other.getReceivedCount(),
            this.receivedBytes + other.getReceivedBytes(),
            this.sentCount + other.getSentCount(),
            this.sentBytes + other.getSentBytes()
        );
    }

    public FlowFileTransferCounts minus(final FlowFileTransferCounts other) {
        return new FlowFileTransferCounts(
            this.receivedCount - other.getReceivedCount(),
            this.receivedBytes - other.getReceivedBytes(),
            this.sentCount - other.getSentCount(),
            this.sentBytes - other.getSentBytes()
        );
    }

    public FlowFileTransferCounts minus(final long additionalReceivedCount, final long additionalReceivedBytes, final long additionalSentCount, final long additionalSentBytes) {
        return new FlowFileTransferCounts(
            this.receivedCount - additionalReceivedCount,
            this.receivedBytes - additionalReceivedBytes,
            this.sentCount - additionalSentCount,
            this.sentBytes - additionalSentBytes
        );
    }
}
