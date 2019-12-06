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
package org.apache.nifi.controller;

import org.apache.nifi.util.FormatUtils;

import java.text.DateFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.List;

public class StandardGarbageCollectionEvent implements GarbageCollectionEvent {
    private final String gcName;
    private final String action;
    private final String cause;
    private final long startTime;
    private final long endTime;
    private final List<GarbageCollectionHeapSize> heapSizes;
    private final DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss.SSS");

    public StandardGarbageCollectionEvent(final String gcName, final String action, final String cause, final long startTime, final long endTime, final List<GarbageCollectionHeapSize> heapSizes) {
        this.gcName = gcName;
        this.action = action;
        this.cause = cause;
        this.startTime = startTime;
        this.endTime = endTime;
        this.heapSizes = heapSizes;
    }

    @Override
    public String getGarbageCollectorName() {
        return gcName;
    }

    @Override
    public String getAction() {
        return action;
    }

    @Override
    public String getCause() {
        return cause;
    }

    @Override
    public long getStartTime() {
        return startTime;
    }

    @Override
    public long getEndTime() {
        return endTime;
    }

    @Override
    public long getDuration() {
        return endTime - startTime;
    }

    @Override
    public List<GarbageCollectionHeapSize> getHeapSizes() {
        return Collections.unmodifiableList(heapSizes);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("GarbageCollectionEvent[collectorName=").append(gcName)
            .append(", action=").append(action)
            .append(", cause=").append(cause)
            .append(", startTime=").append(dateFormat.format(new Date(startTime)))
            .append(", endTime=").append(dateFormat.format(new Date(endTime)))
            .append(", duration=").append(NumberFormat.getInstance().format(endTime - startTime))
            .append(" ms, heap sizes={");

        for (int i=0; i < heapSizes.size(); i++) {
            final GarbageCollectionHeapSize size = heapSizes.get(i);

            sb.append(size.getMemoryPoolName())
                .append(": ")
                .append(FormatUtils.formatDataSize(size.getUsedBeforeCollection()))
                .append(" => ")
                .append(FormatUtils.formatDataSize(size.getUsedAfterCollection()));

            if (i < heapSizes.size() - 1) {
                sb.append(", ");
            }
        }

        sb.append("}]");
        return sb.toString();
    }

    public static class StandardGarbageCollectionHeapSize implements GarbageCollectionHeapSize {
        private final String memoryPoolName;
        private final long usedBefore;
        private final long usedAfter;

        public StandardGarbageCollectionHeapSize(final String memoryPoolName, final long usedBefore, final long usedAfter) {
            this.memoryPoolName = memoryPoolName;
            this.usedBefore = usedBefore;
            this.usedAfter = usedAfter;
        }

        @Override
        public String getMemoryPoolName() {
            return memoryPoolName;
        }

        @Override
        public long getUsedBeforeCollection() {
            return usedBefore;
        }

        @Override
        public long getUsedAfterCollection() {
            return usedAfter;
        }

        @Override
        public String toString() {
            return "HeapSize[memoryPool=" + memoryPoolName + ", " + FormatUtils.formatDataSize(usedBefore) + " => " + FormatUtils.formatDataSize(usedAfter) + "]";
        }
    }
}
