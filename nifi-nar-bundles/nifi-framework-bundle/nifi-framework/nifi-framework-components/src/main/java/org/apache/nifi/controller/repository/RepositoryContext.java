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

package org.apache.nifi.controller.repository;

import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.repository.claim.ContentClaimWriteCache;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.provenance.InternalProvenanceReporter;
import org.apache.nifi.provenance.ProvenanceEventBuilder;
import org.apache.nifi.provenance.ProvenanceEventRepository;

import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;

public interface RepositoryContext {
    Connectable getConnectable();

    Collection<Connection> getConnections(Relationship relationship);

    List<Connection> getPollableConnections();

    ContentRepository getContentRepository();

    FlowFileRepository getFlowFileRepository();

    FlowFileEventRepository getFlowFileEventRepository();

    ProvenanceEventRepository getProvenanceRepository();

    boolean isRelationshipAvailabilitySatisfied(int requiredNumber);

    ContentClaimWriteCache createContentClaimWriteCache();

    InternalProvenanceReporter createProvenanceReporter(Predicate<FlowFile> flowfileKnownCheck, ProvenanceEventEnricher eventEnricher);

    String getConnectableDescription();

    int getNextIncomingConnectionIndex();

    long getNextFlowFileSequence();

    void adjustCounter(String name, long delta);

    ProvenanceEventBuilder createProvenanceEventBuilder();

    StateManager getStateManager();
}
