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
package org.apache.nifi.serialization;

import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSet;

import java.io.IOException;

public abstract class SplitRecordSetHandler {
    final int maximumChunkSize;

    protected SplitRecordSetHandler(int maximumChunkSize) {
        if (maximumChunkSize < 1) {
            throw new IllegalArgumentException("The maximum chunk size must be a positive number");
        }

        this.maximumChunkSize = maximumChunkSize;
    }

    public final RecordHandlerResult handle(final RecordSet recordSet) throws Exception {
        return handle(recordSet, 0);
    }

    public final RecordHandlerResult handle(final RecordSet recordSet, final int alreadyProcessedChunks) throws IOException {
        Record record;
        int currentChunkNumber = 0;
        int currentChunkSize = 0;

        while ((record = recordSet.next()) != null) {
            addToChunk(record);
            currentChunkSize++;

            if (currentChunkSize == maximumChunkSize) {
                try {
                    handleChunk(alreadyProcessedChunks > currentChunkNumber);
                    currentChunkNumber++;
                    currentChunkSize = 0;
                } catch (final SplitRecordSetHandlerException e) {
                    return new RecordHandlerResult(currentChunkNumber, e);
                }
            }
        }

        // Handling the last, not fully filled chunk
        if (currentChunkSize != 0) {
            try {
                handleChunk(alreadyProcessedChunks > currentChunkNumber);
                currentChunkNumber++;
            } catch (final SplitRecordSetHandlerException e) {
                return new RecordHandlerResult(currentChunkNumber, e);
            }
        }

        return new RecordHandlerResult(currentChunkNumber);
    }

    public static class RecordHandlerResult {
        private final int successfulChunks;
        private final Throwable throwable;

        private RecordHandlerResult(final int successfulChunks, final Throwable throwable) {
            this.successfulChunks = successfulChunks;
            this.throwable = throwable;
        }

        private RecordHandlerResult(final int successfulChunks) {
            this.successfulChunks = successfulChunks;
            this.throwable = null;
        }

        public int getSuccessfulChunks() {
            return successfulChunks;
        }

        public boolean isSuccess() {
            return throwable == null;
        }

        public Throwable getThrowable() {
            return throwable;
        }
    }

    protected abstract void handleChunk(boolean wasBatchAlreadyProcessed) throws SplitRecordSetHandlerException;

    protected abstract void addToChunk(Record record);
}
