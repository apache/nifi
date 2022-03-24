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

import java.util.Comparator;

/**
 * Specifies which column to sort on when performing a Listing of FlowFiles via
 * {@link FlowFileQueue#listFlowFiles(String, SortColumn, SortDirection)}
 */
public enum SortColumn implements Comparator<FlowFileSummary> {
    /**
     * Sort based on the current position in the queue
     */
    QUEUE_POSITION(new Comparator<FlowFileSummary>() {
        @Override
        public int compare(final FlowFileSummary o1, final FlowFileSummary o2) {
            return Integer.compare(o1.getPosition(), o2.getPosition());
        }
    }),

    /**
     * Sort based on the UUID of the FlowFile
     */
    FLOWFILE_UUID(new Comparator<FlowFileSummary>() {
        @Override
        public int compare(final FlowFileSummary o1, final FlowFileSummary o2) {
            return o1.getUuid().compareTo(o2.getUuid());
        }
    }),

    /**
     * Sort based on the 'filename' attribute of the FlowFile
     */
    FILENAME(new Comparator<FlowFileSummary>() {
        @Override
        public int compare(final FlowFileSummary o1, final FlowFileSummary o2) {
            return o1.getFilename().compareTo(o2.getFilename());
        }
    }),

    /**
     * Sort based on the size of the FlowFile
     */
    FLOWFILE_SIZE(new Comparator<FlowFileSummary>() {
        @Override
        public int compare(final FlowFileSummary o1, final FlowFileSummary o2) {
            return Long.compare(o1.getSize(), o2.getSize());
        }
    }),

    /**
     * Sort based on how long the FlowFile has been sitting in the queue
     */
    QUEUED_DURATION(new Comparator<FlowFileSummary>() {
        @Override
        public int compare(final FlowFileSummary o1, final FlowFileSummary o2) {
            return -Long.compare(o1.getLastQueuedTime(), o2.getLastQueuedTime());
        }
    }),

    /**
     * Sort based on the age of the FlowFile. I.e., the time at which the FlowFile's
     * "greatest ancestor" entered the flow
     */
    FLOWFILE_AGE(new Comparator<FlowFileSummary>() {
        @Override
        public int compare(final FlowFileSummary o1, final FlowFileSummary o2) {
            return Long.compare(o1.getLineageStartDate(), o2.getLineageStartDate());
        }
    }),

    /**
     * Sort based on when the FlowFile's penalization ends
     */
    PENALIZATION(new Comparator<FlowFileSummary>() {
        @Override
        public int compare(final FlowFileSummary o1, final FlowFileSummary o2) {
            return Boolean.compare(o1.isPenalized(), o2.isPenalized());
        }
    });

    private final Comparator<FlowFileSummary> comparator;

    private SortColumn(final Comparator<FlowFileSummary> comparator) {
        this.comparator = comparator;
    }

    @Override
    public int compare(final FlowFileSummary o1, final FlowFileSummary o2) {
        return comparator.compare(o1, o2);
    }
}
