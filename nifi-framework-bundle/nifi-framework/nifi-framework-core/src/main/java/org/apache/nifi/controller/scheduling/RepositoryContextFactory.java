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
package org.apache.nifi.controller.scheduling;

import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.controller.repository.ContentRepository;
import org.apache.nifi.controller.repository.CounterRepository;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.controller.repository.FlowFileRepository;
import org.apache.nifi.controller.repository.RepositoryContext;
import org.apache.nifi.controller.repository.StandardRepositoryContext;
import org.apache.nifi.provenance.ProvenanceRepository;

import java.util.concurrent.atomic.AtomicLong;

public class RepositoryContextFactory {

    private final ContentRepository contentRepo;
    private final FlowFileRepository flowFileRepo;
    private final FlowFileEventRepository flowFileEventRepo;
    private final CounterRepository counterRepo;
    private final ProvenanceRepository provenanceRepo;
    private final StateManagerProvider stateManagerProvider;

    public RepositoryContextFactory(final ContentRepository contentRepository, final FlowFileRepository flowFileRepository,
            final FlowFileEventRepository flowFileEventRepository, final CounterRepository counterRepository,
            final ProvenanceRepository provenanceRepository, final StateManagerProvider stateManagerProvider) {

        this.contentRepo = contentRepository;
        this.flowFileRepo = flowFileRepository;
        this.flowFileEventRepo = flowFileEventRepository;
        this.counterRepo = counterRepository;
        this.provenanceRepo = provenanceRepository;
        this.stateManagerProvider = stateManagerProvider;
    }

    public RepositoryContext newProcessContext(final Connectable connectable, final AtomicLong connectionIndex) {
        final StateManager stateManager = stateManagerProvider.getStateManager(connectable.getIdentifier());
        return new StandardRepositoryContext(connectable, connectionIndex, contentRepo, flowFileRepo, flowFileEventRepo, counterRepo, provenanceRepo, stateManager);
    }

    public ContentRepository getContentRepository() {
        return contentRepo;
    }

    public FlowFileRepository getFlowFileRepository() {
        return flowFileRepo;
    }

    public ProvenanceRepository getProvenanceRepository() {
        return provenanceRepo;
    }
}
