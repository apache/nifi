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

package org.apache.nifi.provenance.store;

import org.apache.nifi.provenance.serialization.RecordWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class RecordWriterLease {
    private final Logger logger = LoggerFactory.getLogger(RecordWriterLease.class);

    private final RecordWriter writer;
    private final long maxBytes;
    private final int maxEvents;
    private final long maxSystemTime;
    private long usageCounter;
    private RolloverState rolloverState = RolloverState.SHOULD_NOT_ROLLOVER;
    private boolean closed = false;

    public RecordWriterLease(final RecordWriter writer, final long maxBytes, final int maxEvents, final long maxMillis) {
        this.writer = writer;
        this.maxBytes = maxBytes;
        this.maxEvents = maxEvents;

        // The max timestamp that we want to write to this lease is X number of milliseconds into the future.
        // We don't want X to be more than the given max millis. However, we also don't want to allow it to get too large. If it
        // becomes >= Integer.MAX_VALUE, we could have some timestamp offsets that rollover into the negative range.
        // To avoid that, we could use a value that is no more than Integer.MAX_VALUE. But since the event may be persisted
        // a bit after the lease has been obtained, we subtract 1 hour from that time to give ourselves a little buffer room.
        this.maxSystemTime = System.currentTimeMillis() + Math.min(maxMillis, Integer.MAX_VALUE - TimeUnit.HOURS.toMillis(1));
    }

    public RecordWriter getWriter() {
        return writer;
    }

    public synchronized boolean tryClaim() {
        if (rolloverState.isRollover()) {
            return false;
        }

        // The previous state did not indicate that we should rollover. We need to check the current state also.
        // It is important that we do not update the rolloverState here because we can do that only if the usageCounter indicates
        // that the writer is no longer in use. This is handled in the getRolloverState() method.
        if (determineRolloverReason().isRollover()) {
            return false;
        }

        usageCounter++;
        return true;
    }

    public synchronized void relinquishClaim() {
        usageCounter--;

        if (closed && usageCounter < 1) {
            try {
                writer.close();
            } catch (final Exception e) {
                logger.warn("Failed to close " + writer, e);
            }
        }
    }

    private synchronized RolloverState determineRolloverReason() {
        if (writer.isClosed()) {
            return RolloverState.WRITER_ALREADY_CLOSED;
        }
        if (writer.isDirty()) {
            return RolloverState.WRITER_IS_DIRTY;
        }
        if (writer.getBytesWritten() >= maxBytes) {
            return RolloverState.MAX_BYTES_REACHED;
        }

        final int recordsWritten = writer.getRecordsWritten();
        if (recordsWritten >= maxEvents) {
            return RolloverState.MAX_EVENTS_REACHED;
        }
        if (recordsWritten > 0 && System.currentTimeMillis() >= maxSystemTime) {
            return RolloverState.MAX_TIME_REACHED;
        }

        return RolloverState.SHOULD_NOT_ROLLOVER;
    }

    public synchronized RolloverState getRolloverState() {
        if (rolloverState.isRollover()) {
            return rolloverState;
        }

        if (usageCounter < 1) {
            rolloverState = determineRolloverReason();
            return rolloverState;
        }

        return RolloverState.SHOULD_NOT_ROLLOVER;
    }

    public synchronized void close() {
        closed = true;

        if (usageCounter < 1) {
            try {
                writer.close();
            } catch (final Exception e) {
                logger.warn("Failed to close " + writer, e);
            }
        }
    }

    public String toString() {
        // Call super.toString() so that we have a unique hash/address added to the toString, but also include the file being written to.
        // When comparing the toString() of two different leases, this helps to compare whether or not the two leases are the same object
        // as well as whether or not they point to the same file.
        return super.toString() + "[" + writer.getFile() + "]";
    }
}
