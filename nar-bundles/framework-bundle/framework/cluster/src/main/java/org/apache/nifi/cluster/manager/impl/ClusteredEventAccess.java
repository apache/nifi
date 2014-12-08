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
package org.apache.nifi.cluster.manager.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.provenance.ProvenanceEventBuilder;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventRepository;
import org.apache.nifi.provenance.lineage.ComputeLineageSubmission;
import org.apache.nifi.provenance.search.Query;
import org.apache.nifi.provenance.search.QuerySubmission;
import org.apache.nifi.provenance.search.SearchableField;
import org.apache.nifi.reporting.EventAccess;

public class ClusteredEventAccess implements EventAccess {

    private final WebClusterManager clusterManager;

    public ClusteredEventAccess(final WebClusterManager clusterManager) {
        this.clusterManager = clusterManager;
    }

    @Override
    public ProcessGroupStatus getControllerStatus() {
        return clusterManager.getProcessGroupStatus(WebClusterManager.ROOT_GROUP_ID_ALIAS);
    }

    @Override
    public List<ProvenanceEventRecord> getProvenanceEvents(long arg0, int arg1) throws IOException {
        return new ArrayList<>();
    }

    @Override
    public ProvenanceEventRepository getProvenanceRepository() {
        // NCM doesn't have provenance events, because it doesn't process FlowFiles.
        // So we just use a Provenance Event Repository that does nothing.
        return new ProvenanceEventRepository() {
            @Override
            public void close() throws IOException {
            }

            @Override
            public ProvenanceEventRecord getEvent(long eventId) throws IOException {
                return null;
            }

            @Override
            public List<ProvenanceEventRecord> getEvents(long startEventId, int maxEvents) throws IOException {
                return new ArrayList<>();
            }

            @Override
            public Long getMaxEventId() {
                return null;
            }

            @Override
            public List<SearchableField> getSearchableAttributes() {
                return new ArrayList<>();
            }

            @Override
            public List<SearchableField> getSearchableFields() {
                return new ArrayList<>();
            }

            @Override
            public void registerEvent(final ProvenanceEventRecord event) {
            }

            @Override
            public void registerEvents(final Iterable<ProvenanceEventRecord> events) {
            }

            @Override
            public ComputeLineageSubmission retrieveLineageSubmission(final String submissionId) {
                return null;
            }

            @Override
            public QuerySubmission retrieveQuerySubmission(final String submissionId) {
                return null;
            }

            @Override
            public ComputeLineageSubmission submitExpandChildren(final long eventId) {
                return null;
            }

            @Override
            public ComputeLineageSubmission submitExpandParents(final long eventId) {
                return null;
            }

            @Override
            public ComputeLineageSubmission submitLineageComputation(final String flowFileUuid) {
                return null;
            }

            @Override
            public QuerySubmission submitQuery(final Query query) {
                return null;
            }

            @Override
            public ProvenanceEventBuilder eventBuilder() {
                return null;
            }

            @Override
            public void initialize(EventReporter eventReporter) throws IOException {

            }
        };
    }
}
