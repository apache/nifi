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

package org.apache.nifi.flowanalysis;

/**
 * Represents the status of an Analyze Flow Request.
 * Such a request may be rather lengthy if there many flow analysis rules and/or their logic is complicated.
 * As a result, the analysis is performed asynchronously.
 * <p>
 * This status object provides information about the success or failure of the
 * operation.
 */
public interface AnalyzeFlowStatus {

    /**
     * @return the id of the process group representing (a part of) the flow being analyzed
     */
    String getProcessGroupId();

    /**
     * @return the date/time (in milliseconds since epoch) at which the flow analysis request
     * was submitted
     */
    long getRequestSubmissionTime();

    /**
     * @return the date/time (in milliseconds since epoch) at which the status of the
     * request was last updated
     */
    long getLastUpdated();

    /**
     * @return the current state of the operation
     */
    AnalyzeFlowState getState();

    /**
     * @return the reason that the state is set to a Failure state, or <code>null</code> if the state is not {@link AnalyzeFlowState#FAILURE}.
     */
    String getFailureReason();
}
