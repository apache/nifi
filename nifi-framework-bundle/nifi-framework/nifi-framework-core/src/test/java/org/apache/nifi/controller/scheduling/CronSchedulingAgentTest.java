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
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.engine.FlowEngine;
import org.apache.nifi.reporting.ReportingTask;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CronSchedulingAgentTest {

    private static final String DEFAULT_CRON_EXPRESSION = "* * * * * ?";

    private static final String INVALID_CRON_EXPRESSION = "* * * * * * 2023";

    private static final int CONCURRENT_TASKS = 1;

    @Mock
    private FlowController flowController;

    @Mock
    private FlowEngine flowEngine;

    @Mock
    private RepositoryContextFactory repositoryContextFactory;

    @Mock
    private Connectable connectable;

    @Mock
    private ReportingTaskNode reportingTaskNode;

    @Mock
    private ReportingTask reportingTask;

    @Mock
    private StateManagerProvider stateManagerProvider;

    @Mock
    private StateManager stateManager;

    private CronSchedulingAgent schedulingAgent;

    @BeforeEach
    void setSchedulingAgent() {
        schedulingAgent = new CronSchedulingAgent(flowController, flowEngine, repositoryContextFactory);
    }

    @Test
    void testDoScheduleConnectable() {
        final String componentId = UUID.randomUUID().toString();
        final LifecycleState lifecycleState = new LifecycleState(componentId);

        when(connectable.evaluateParameters(eq(DEFAULT_CRON_EXPRESSION))).thenReturn(DEFAULT_CRON_EXPRESSION);
        when(connectable.getSchedulingPeriod()).thenReturn(DEFAULT_CRON_EXPRESSION);
        when(connectable.getMaxConcurrentTasks()).thenReturn(CONCURRENT_TASKS);
        when(connectable.getIdentifier()).thenReturn(componentId);
        when(flowController.getStateManagerProvider()).thenReturn(stateManagerProvider);
        when(stateManagerProvider.getStateManager(eq(componentId))).thenReturn(stateManager);

        schedulingAgent.doSchedule(connectable, lifecycleState);
    }

    @Test
    void testDoScheduleReportingTaskNode() {
        final String componentId = UUID.randomUUID().toString();
        final LifecycleState lifecycleState = new LifecycleState(componentId);

        when(reportingTaskNode.getSchedulingPeriod()).thenReturn(DEFAULT_CRON_EXPRESSION);

        schedulingAgent.doSchedule(reportingTaskNode, lifecycleState);
    }

    @Test
    void testDoScheduleReportingTaskNodeCronExpressionInvalid() {
        final String componentId = UUID.randomUUID().toString();
        final LifecycleState lifecycleState = new LifecycleState(componentId);

        when(reportingTaskNode.getSchedulingPeriod()).thenReturn(INVALID_CRON_EXPRESSION);
        when(reportingTaskNode.getReportingTask()).thenReturn(reportingTask);
        when(reportingTask.getIdentifier()).thenReturn(componentId);

        assertThrows(IllegalStateException.class, () -> schedulingAgent.doSchedule(reportingTaskNode, lifecycleState));
    }
}
