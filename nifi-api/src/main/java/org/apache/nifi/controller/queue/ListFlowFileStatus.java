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

import java.util.List;

public interface ListFlowFileStatus {

    /**
     * @return the maximum number of FlowFile Summary objects that should be returned
     */
    int getMaxResults();

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
     * @return the column on which the listing is sorted
     */
    SortColumn getSortColumn();

    /**
     * @return the direction in which the FlowFiles are sorted
     */
    SortDirection getSortDirection();

    /**
     * @return the current state of the operation
     */
    ListFlowFileState getState();

    /**
     * @return the reason that the state is set to a Failure state, or <code>null</code> if the state is not {@link ListFlowFileStatus#FAILURE}.
     */
    String getFailureReason();

    /**
     * @return the current size of the queue
     */
    QueueSize getQueueSize();

    /**
     * @return a List of FlowFileSummary objects
     */
    List<FlowFileSummary> getFlowFileSummaries();

    /**
     * @return the percentage (an integer between 0 and 100, inclusive) of how close the request is to being completed
     */
    int getCompletionPercentage();

    /**
     * @return the total number of steps that are required in order to finish the listing
     */
    int getTotalStepCount();

    /**
     * @return the total number of steps that have already been completed. The value returned will be >= 0 and <= the result of calling {@link #getTotalStepCount()}.
     */
    int getCompletedStepCount();
}
