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
package org.apache.nifi.atlas.provenance;

import org.apache.nifi.atlas.NiFiFlow;
import org.apache.nifi.atlas.resolver.ClusterResolver;
import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceRepository;
import org.apache.nifi.provenance.lineage.ComputeLineageResult;
import org.apache.nifi.provenance.lineage.ComputeLineageSubmission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class StandardAnalysisContext implements AnalysisContext {

    private final Logger logger = LoggerFactory.getLogger(StandardAnalysisContext.class);
    private final NiFiFlow nifiFlow;
    private final ClusterResolver clusterResolver;
    private final ProvenanceRepository provenanceRepository;

    public StandardAnalysisContext(NiFiFlow nifiFlow, ClusterResolver clusterResolver,
                                   ProvenanceRepository provenanceRepository) {
        this.nifiFlow = nifiFlow;
        this.clusterResolver = clusterResolver;
        this.provenanceRepository = provenanceRepository;
    }

    @Override
    public List<ConnectionStatus> findConnectionTo(String componentId) {
        return nifiFlow.getIncomingConnections(componentId);
    }

    @Override
    public List<ConnectionStatus> findConnectionFrom(String componentId) {
        return nifiFlow.getOutgoingConnections(componentId);
    }

    @Override
    public String getNiFiClusterName() {
        return nifiFlow.getClusterName();
    }

    @Override
    public ClusterResolver getClusterResolver() {
        return clusterResolver;
    }

    private ComputeLineageResult getLineageResult(long eventId, ComputeLineageSubmission submission) {
        final ComputeLineageResult result = submission.getResult();
        try {
            if (result.awaitCompletion(10, TimeUnit.SECONDS)) {
                return result;
            }
            logger.warn("Lineage query for {} timed out.", new Object[]{eventId});
        } catch (InterruptedException e) {
            logger.warn("Lineage query for {} was interrupted due to {}.", new Object[]{eventId, e}, e);
        } finally {
            submission.cancel();
        }

        return null;
    }

    @Override
    public ComputeLineageResult queryLineage(long eventId) {
        final ComputeLineageSubmission submission = provenanceRepository.submitLineageComputation(eventId, null);
        return getLineageResult(eventId, submission);
    }

    public ComputeLineageResult findParents(long eventId) {
        final ComputeLineageSubmission submission = provenanceRepository.submitExpandParents(eventId, null);
        return getLineageResult(eventId, submission);
    }

    @Override
    public ProvenanceEventRecord getProvenanceEvent(long eventId) {
        try {
            return provenanceRepository.getEvent(eventId);
        } catch (IOException e) {
            logger.error("Failed to get provenance event for {} due to {}", new Object[]{eventId, e}, e);
            return null;
        }
    }

}
