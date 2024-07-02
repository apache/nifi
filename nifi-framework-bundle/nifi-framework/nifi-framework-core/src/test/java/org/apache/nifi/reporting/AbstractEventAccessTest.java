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
package org.apache.nifi.reporting;

import org.apache.nifi.action.Action;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.controller.repository.RepositoryStatusReport;
import org.apache.nifi.controller.repository.StandardRepositoryStatusReport;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.controller.status.analytics.StatusAnalyticsEngine;
import org.apache.nifi.diagnostics.StorageUsage;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.provenance.ProvenanceEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AbstractEventAccessTest {

    private static final boolean INCLUDE_CONNECTION_DETAILS = true;

    private static final String PROCESS_GROUP_NAME = "Root Group";

    private static final String PROCESS_GROUP_ID = UUID.randomUUID().toString();

    private static final String PROCESSOR_NAME = "Event Processor";

    private static final String PROCESSOR_ID = UUID.randomUUID().toString();

    private static final int ZERO_DEPTH = 0;

    private static final int SINGLE_DEPTH = 1;

    @Mock
    private ProcessScheduler processScheduler;

    @Mock
    private StatusAnalyticsEngine statusAnalyticsEngine;

    @Mock
    private FlowManager flowManager;

    @Mock
    private FlowFileEventRepository flowFileEventRepository;

    @Mock
    private ProcessGroup processGroup;

    @Mock
    private ProcessorNode processorNode;

    private AbstractEventAccess eventAccess;

    @BeforeEach
    void setEventAccess() {
        eventAccess = new ConcreteEventAccess(processScheduler, statusAnalyticsEngine, flowManager, flowFileEventRepository);
    }

    @Test
    void testGetGroupStatusAuthorized() {
        final List<Authorizable> authorizables = new ArrayList<>();

        final RepositoryStatusReport repositoryStatusReport = new StandardRepositoryStatusReport();
        final Predicate<Authorizable> checkAuthorization = authorizables::add;

        when(processGroup.getName()).thenReturn(PROCESS_GROUP_NAME);
        when(processGroup.getIdentifier()).thenReturn(PROCESS_GROUP_ID);

        final ProcessGroupStatus groupStatus = eventAccess.getGroupStatus(processGroup, repositoryStatusReport, checkAuthorization, SINGLE_DEPTH, SINGLE_DEPTH, INCLUDE_CONNECTION_DETAILS);

        assertNotNull(groupStatus);
        assertEquals(PROCESS_GROUP_NAME, groupStatus.getName());
        assertEquals(PROCESS_GROUP_ID, groupStatus.getId());

        assertEquals(1, authorizables.size());
    }

    @Test
    void testGetGroupStatusProcessorAuthorizedNotIncluded() {
        final List<Authorizable> authorizables = new ArrayList<>();

        final RepositoryStatusReport repositoryStatusReport = new StandardRepositoryStatusReport();
        final Predicate<Authorizable> checkAuthorization = authorizables::add;

        when(processorNode.getIdentifier()).thenReturn(PROCESSOR_ID);
        when(processorNode.getProcessGroup()).thenReturn(processGroup);

        when(processGroup.getProcessors()).thenReturn(List.of(processorNode));
        when(processGroup.getName()).thenReturn(PROCESS_GROUP_NAME);
        when(processGroup.getIdentifier()).thenReturn(PROCESS_GROUP_ID);

        final ProcessGroupStatus groupStatus = eventAccess.getGroupStatus(processGroup, repositoryStatusReport, checkAuthorization, ZERO_DEPTH, SINGLE_DEPTH, INCLUDE_CONNECTION_DETAILS);

        assertNotNull(groupStatus);
        assertEquals(PROCESS_GROUP_NAME, groupStatus.getName());
        assertEquals(PROCESS_GROUP_ID, groupStatus.getId());
        assertTrue(groupStatus.getProcessorStatus().isEmpty());

        assertEquals(1, authorizables.size());
    }

    @Test
    void testGetGroupStatusProcessorAuthorized() {
        final List<Authorizable> authorizables = new ArrayList<>();

        final RepositoryStatusReport repositoryStatusReport = new StandardRepositoryStatusReport();
        final Predicate<Authorizable> checkAuthorization = authorizables::add;

        when(processorNode.getName()).thenReturn(PROCESSOR_NAME);
        when(processorNode.getIdentifier()).thenReturn(PROCESSOR_ID);
        when(processorNode.getProcessGroup()).thenReturn(processGroup);
        when(processGroup.getProcessors()).thenReturn(List.of(processorNode));
        when(processGroup.getName()).thenReturn(PROCESS_GROUP_NAME);
        when(processGroup.getIdentifier()).thenReturn(PROCESS_GROUP_ID);

        final ProcessGroupStatus groupStatus = eventAccess.getGroupStatus(processGroup, repositoryStatusReport, checkAuthorization, SINGLE_DEPTH, SINGLE_DEPTH, INCLUDE_CONNECTION_DETAILS);

        assertNotNull(groupStatus);
        assertEquals(PROCESS_GROUP_NAME, groupStatus.getName());
        assertEquals(PROCESS_GROUP_ID, groupStatus.getId());

        final Optional<ProcessorStatus> processorStatusFound = groupStatus.getProcessorStatus().stream().findFirst();
        assertTrue(processorStatusFound.isPresent());
        final ProcessorStatus processorStatus = processorStatusFound.get();
        assertEquals(PROCESSOR_NAME, processorStatus.getName());
        assertEquals(PROCESSOR_ID, processorStatus.getId());

        assertEquals(2, authorizables.size());
    }

    @Test
    void testGetGroupStatusNotAuthorized() {
        final List<Authorizable> authorizables = new ArrayList<>();

        final RepositoryStatusReport repositoryStatusReport = new StandardRepositoryStatusReport();
        final Predicate<Authorizable> checkAuthorization = authorizable -> {
            authorizables.add(authorizable);
            return false;
        };

        when(processorNode.getIdentifier()).thenReturn(PROCESSOR_ID);
        when(processorNode.getProcessGroup()).thenReturn(processGroup);
        when(processGroup.getProcessors()).thenReturn(List.of(processorNode));
        when(processGroup.getIdentifier()).thenReturn(PROCESS_GROUP_ID);

        final ProcessGroupStatus groupStatus = eventAccess.getGroupStatus(processGroup, repositoryStatusReport, checkAuthorization, SINGLE_DEPTH, SINGLE_DEPTH, INCLUDE_CONNECTION_DETAILS);

        assertNotNull(groupStatus);
        assertEquals(PROCESS_GROUP_ID, groupStatus.getName());
        assertEquals(PROCESS_GROUP_ID, groupStatus.getId());

        final Optional<ProcessorStatus> processorStatusFound = groupStatus.getProcessorStatus().stream().findFirst();
        assertTrue(processorStatusFound.isPresent());
        final ProcessorStatus processorStatus = processorStatusFound.get();
        assertEquals(PROCESSOR_ID, processorStatus.getName());
        assertEquals(PROCESSOR_ID, processorStatus.getId());

        assertEquals(2, authorizables.size());
    }

    private static class ConcreteEventAccess extends AbstractEventAccess {

        public ConcreteEventAccess(
                final ProcessScheduler processScheduler,
                final StatusAnalyticsEngine analyticsEngine,
                final FlowManager flowManager,
                final FlowFileEventRepository flowFileEventRepository
        ) {
            super(processScheduler, analyticsEngine, flowManager, flowFileEventRepository);
        }

        @Override
        public ProvenanceEventRepository getProvenanceRepository() {
            return null;
        }

        @Override
        public List<Action> getFlowChanges(int firstActionId, int maxActions) {
            return List.of();
        }

        @Override
        public Map<String, StorageUsage> getProvenanceRepositoryStorageUsage() {
            return Map.of();
        }

        @Override
        public Map<String, StorageUsage> getContentRepositoryStorageUsage() {
            return Map.of();
        }

        @Override
        public StorageUsage getFlowFileRepositoryStorageUsage() {
            return null;
        }
    }
}
