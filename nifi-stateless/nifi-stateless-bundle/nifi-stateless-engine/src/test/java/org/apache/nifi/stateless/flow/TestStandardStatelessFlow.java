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
package org.apache.nifi.stateless.flow;

import org.apache.nifi.components.state.StatelessStateManagerProvider;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.repository.metrics.tracking.StatsTracker;
import org.apache.nifi.controller.scheduling.LifecycleStateManager;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.processor.exception.TerminatedTaskException;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.stateless.engine.ProcessContextFactory;
import org.apache.nifi.stateless.repository.RepositoryContextFactory;
import org.apache.nifi.stateless.session.AsynchronousCommitTracker;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestStandardStatelessFlow {

    @Test
    public void testShutdownWithQueuedFlowFilesTriggersFailure() throws Exception {
        final ProcessGroup rootGroup = mock(ProcessGroup.class);
        when(rootGroup.getName()).thenReturn("root");
        when(rootGroup.getInputPorts()).thenReturn(Collections.emptySet());
        when(rootGroup.getOutputPorts()).thenReturn(Collections.emptySet());
        when(rootGroup.findAllProcessors()).thenReturn(Collections.emptyList());
        when(rootGroup.stopComponents(any())).thenReturn(CompletableFuture.completedFuture(null));
        when(rootGroup.findAllRemoteProcessGroups()).thenReturn(Collections.emptyList());
        when(rootGroup.isDataQueuedForProcessing()).thenReturn(true);

        final ControllerServiceProvider controllerServiceProvider = mock(ControllerServiceProvider.class);
        final ProcessContextFactory processContextFactory = mock(ProcessContextFactory.class);
        final RepositoryContextFactory repositoryContextFactory = mock(RepositoryContextFactory.class);
        final DataflowDefinition dataflowDefinition = mock(DataflowDefinition.class);
        final StatelessStateManagerProvider stateManagerProvider = mock(StatelessStateManagerProvider.class);
        final ProcessScheduler processScheduler = mock(ProcessScheduler.class);
        final BulletinRepository bulletinRepository = mock(BulletinRepository.class);
        final LifecycleStateManager lifecycleStateManager = mock(LifecycleStateManager.class);
        final StatsTracker statsTracker = mock(StatsTracker.class);

        final AsynchronousCommitTracker tracker = spy(new AsynchronousCommitTracker(rootGroup));
        final StandardStatelessFlow flow = new StandardStatelessFlow(rootGroup, List.of(), controllerServiceProvider, processContextFactory,
                repositoryContextFactory, dataflowDefinition, stateManagerProvider, processScheduler, bulletinRepository,
                lifecycleStateManager, Duration.ZERO, statsTracker, tracker);

        flow.shutdown(false, false, Duration.ZERO);

        verify(tracker, times(1)).triggerFailureCallbacks(any(TerminatedTaskException.class));
    }

}
