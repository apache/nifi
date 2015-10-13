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

/**
 * Represents the status of a Drop FlowFile Request that has been issued to
 * a {@link FlowFileQueue}. When a queue is requested to drop its FlowFiles,
 * that process may be rather lengthy in the case of a poorly behaving
 * FlowFileRepository or if the destination Processor is polling from the
 * queue using a filter that is misbehaving. As a result, the dropping of
 * FlowFiles is performed asynchronously.
 *
 * This status object provides information about how far along in the process
 * we currently are and information about the success or failure of the
 * operation.
 */
public interface DropFlowFileStatus {

    /**
     * @return the identifier of the request to drop FlowFiles from the queue
     */
    String getRequestIdentifier();

    /**
     * @return the date/time (in milliseconds since epoch) at which the request to
     *         drop the FlowFiles from a queue was submitted
     */
    long getRequestSubmissionTime();

    /**
     * @return the date/time (in milliseconds since epoch) at which the status of the
     *         request was last updated
     */
    long getLastUpdated();

    /**
     * @return the size of the queue when the drop request was issued or <code>null</code> if
     *         it is not yet known, which can happen if the {@link DropFlowFileState} is
     *         {@link DropFlowFileState#WAITING_FOR_LOCK}.
     */
    QueueSize getOriginalSize();

    /**
     * @return the current size of the queue or <code>null</code> if it is not yet known
     */
    QueueSize getCurrentSize();

    /**
     * @return a QueueSize representing the number of FlowFiles that have been dropped for this request
     *         and the aggregate size of those FlowFiles
     */
    QueueSize getDroppedSize();

    /**
     * @return the current state of the operation
     */
    DropFlowFileState getState();
}
