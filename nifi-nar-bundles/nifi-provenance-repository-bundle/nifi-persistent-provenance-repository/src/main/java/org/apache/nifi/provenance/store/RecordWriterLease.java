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

public class RecordWriterLease {
    private final Logger logger = LoggerFactory.getLogger(RecordWriterLease.class);

    private final RecordWriter writer;
    private final long maxBytes;
    private final int maxEvents;
    private long usageCounter;
    private boolean markedRollable = false;
    private boolean closed = false;

    public RecordWriterLease(final RecordWriter writer, final long maxBytes) {
        this(writer, maxBytes, Integer.MAX_VALUE);
    }

    public RecordWriterLease(final RecordWriter writer, final long maxBytes, final int maxEvents) {
        this.writer = writer;
        this.maxBytes = maxBytes;
        this.maxEvents = maxEvents;
    }

    public RecordWriter getWriter() {
        return writer;
    }

    public synchronized boolean tryClaim() {
        if (markedRollable || writer.isClosed() || writer.isDirty() || writer.getBytesWritten() >= maxBytes || writer.getRecordsWritten() >= maxEvents) {
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

    public synchronized boolean shouldRoll() {
        if (markedRollable) {
            return true;
        }

        if (usageCounter < 1 && (writer.isClosed() || writer.isDirty() || writer.getBytesWritten() >= maxBytes || writer.getRecordsWritten() >= maxEvents)) {
            markedRollable = true;
            return true;
        }

        return false;
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
}
