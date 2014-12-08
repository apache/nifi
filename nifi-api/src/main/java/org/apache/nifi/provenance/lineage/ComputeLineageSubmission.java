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
package org.apache.nifi.provenance.lineage;

import java.util.Collection;
import java.util.Date;

public interface ComputeLineageSubmission {

    /**
     * Returns the {@link ComputeLineageResult} that contains the results. The
     * results may be partial if a call to
     * {@link ComputeLineageResult#isFinished()} returns <code>false</code>.
     *
     * @return
     */
    ComputeLineageResult getResult();

    /**
     * Returns the date at which this lineage was submitted
     *
     * @return
     */
    Date getSubmissionTime();

    /**
     * Returns the generated identifier for this lineage result
     *
     * @return
     */
    String getLineageIdentifier();

    /**
     * Cancels the lineage computation
     */
    void cancel();

    /**
     * @return <code>true</code> if {@link #cancel()} has been called,
     * <code>false</code> otherwise
     */
    boolean isCanceled();

    /**
     * Returns the type of Lineage Computation that was submitted
     *
     * @return
     */
    LineageComputationType getLineageComputationType();

    /**
     * If the Lineage Computation Type of this submission is
     * {@link LineageComputationType.EXPAND_CHILDREN} or
     * {@link LineageComputationType.EXPAND_PARENTS}, indicates the ID event
     * that is to be expanded; otherwise, returns <code>null</code>.
     *
     * @return
     */
    Long getExpandedEventId();

    /**
     * Returns all FlowFile UUID's that are encapsulated in this lineage
     * computation submission
     *
     * @return
     */
    Collection<String> getLineageFlowFileUuids();
}
