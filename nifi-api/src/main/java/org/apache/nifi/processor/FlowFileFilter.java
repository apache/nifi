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
package org.apache.nifi.processor;

import org.apache.nifi.flowfile.FlowFile;

/**
 * <p>
 * FlowFileFilter provides a mechanism for selectively choosing which FlowFiles
 * to obtain from a Processor's incoming connections.
 * </p>
 *
 * <p>
 * Implementations of this interface need not be thread-safe.
 * </p>
 */
public interface FlowFileFilter {

    /**
     * Indicates whether or not the given FlowFile should be selected and
     * whether or not the Processor is interested in filtering additional
     * FlowFiles
     *
     * @param flowFile to apply the filter to
     * @return true if the given FlowFile should be selected and
     * if Processor is interested in filtering additional
     * FlowFiles
     */
    FlowFileFilterResult filter(FlowFile flowFile);

    /**
     * Provides a result type to indicate whether or not a FlowFile should be
     * selected
     */
    public static enum FlowFileFilterResult {

        /**
         * Indicates that a FlowFile should be returned to the Processor to be
         * processed and that additional FlowFiles should be processed by this
         * filter.
         */
        ACCEPT_AND_CONTINUE(true, true),
        /**
         * Indicates that a FlowFile should be returned to the Processor to be
         * processed and that this is the last FlowFile that should be processed
         * by this filter.
         */
        ACCEPT_AND_TERMINATE(false, true),
        /**
         * Indicates that a FlowFile should not be processed by the Processor at
         * this time but that additional FlowFiles should be processed by this
         * filter.
         */
        REJECT_AND_CONTINUE(true, false),
        /**
         * Indicates that a FlowFile should not be processed by the Processor at
         * this time and that no additional FlowFiles should be processed
         * either.
         */
        REJECT_AND_TERMINATE(false, false);

        private final boolean continueProcessing;
        private final boolean accept;

        private FlowFileFilterResult(final boolean continueProcessing, final boolean accept) {
            this.continueProcessing = continueProcessing;
            this.accept = accept;
        }

        public boolean isAccept() {
            return accept;
        }

        public boolean isContinue() {
            return continueProcessing;
        }
    }

}
