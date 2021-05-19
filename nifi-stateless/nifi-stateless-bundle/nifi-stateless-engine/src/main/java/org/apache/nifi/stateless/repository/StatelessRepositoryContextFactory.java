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

package org.apache.nifi.stateless.repository;

import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.controller.repository.ContentRepository;
import org.apache.nifi.controller.repository.CounterRepository;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.controller.repository.FlowFileRepository;
import org.apache.nifi.controller.repository.RepositoryContext;
import org.apache.nifi.provenance.ProvenanceEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public class StatelessRepositoryContextFactory implements RepositoryContextFactory {
    private static final Logger logger = LoggerFactory.getLogger(StatelessRepositoryContextFactory.class);

    private final ContentRepository contentRepository;
    private final FlowFileRepository flowFileRepository;
    private final FlowFileEventRepository flowFileEventRepository;
    private final CounterRepository counterRepository;
    private final ProvenanceEventRepository provenanceEventRepository;
    private final StateManagerProvider stateManagerProvider;

    public StatelessRepositoryContextFactory(final ContentRepository contentRepository, final FlowFileRepository flowFileRepository, final FlowFileEventRepository flowFileEventRepository,
                                             final CounterRepository counterRepository, final ProvenanceEventRepository provenanceRepository, final StateManagerProvider stateManagerProvider) {
        this.contentRepository = contentRepository;
        this.flowFileRepository = flowFileRepository;
        this.flowFileEventRepository = flowFileEventRepository;
        this.counterRepository = counterRepository;
        this.provenanceEventRepository = provenanceRepository;
        this.stateManagerProvider = stateManagerProvider;
    }

    @Override
    public RepositoryContext createRepositoryContext(final Connectable connectable) {
        final StateManager stateManager = stateManagerProvider.getStateManager(connectable.getIdentifier());
        return new StatelessRepositoryContext(connectable, new AtomicLong(0L), contentRepository, flowFileRepository,
            flowFileEventRepository, counterRepository, provenanceEventRepository, stateManager);
    }

    @Override
    public ContentRepository getContentRepository() {
        return contentRepository;
    }

    @Override
    public FlowFileEventRepository getFlowFileEventRepository() {
        return flowFileEventRepository;
    }

    @Override
    public void shutdown() {
        contentRepository.shutdown();

        try {
            flowFileRepository.close();
        } catch (final IOException e) {
            logger.warn("Failed to properly shutdown FlowFile Repository", e);
        }

        try {
            flowFileEventRepository.close();
        } catch (final IOException e) {
            logger.warn("Failed to properly shutdown FlowFile Event Repository", e);
        }

        try {
            provenanceEventRepository.close();
        } catch (final IOException e) {
            logger.warn("Failed to properly shutdown Provenance Repository", e);
        }
    }
}
