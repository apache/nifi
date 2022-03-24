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
package org.apache.nifi.provenance;

import java.util.Collection;
import java.util.Date;
import java.util.UUID;

import org.apache.nifi.provenance.lineage.ComputeLineageSubmission;
import org.apache.nifi.provenance.lineage.LineageComputationType;

/**
 *
 */
public class AsyncLineageSubmission implements ComputeLineageSubmission {

    private final String lineageIdentifier = UUID.randomUUID().toString();
    private final Date submissionTime = new Date();

    private final LineageComputationType computationType;
    private final Long eventId;
    private final Collection<String> lineageFlowFileUuids;
    private final String submitterId;

    private volatile boolean canceled = false;

    private final StandardLineageResult result;

    public AsyncLineageSubmission(final LineageComputationType computationType, final Long eventId, final Collection<String> lineageFlowFileUuids, final int numSteps, final String submitterId) {
        this.computationType = computationType;
        this.eventId = eventId;
        this.lineageFlowFileUuids = lineageFlowFileUuids;
        this.submitterId = submitterId;
        this.result = new StandardLineageResult(numSteps, lineageFlowFileUuids);
    }

    @Override
    public String getSubmitterIdentity() {
        return submitterId;
    }

    @Override
    public StandardLineageResult getResult() {
        return result;
    }

    @Override
    public Date getSubmissionTime() {
        return submissionTime;
    }

    @Override
    public String getLineageIdentifier() {
        return lineageIdentifier;
    }

    @Override
    public void cancel() {
        this.canceled = true;
    }

    @Override
    public boolean isCanceled() {
        return canceled;
    }

    @Override
    public LineageComputationType getLineageComputationType() {
        return computationType;
    }

    @Override
    public Long getExpandedEventId() {
        return eventId;
    }

    @Override
    public Collection<String> getLineageFlowFileUuids() {
        return lineageFlowFileUuids;
    }
}
