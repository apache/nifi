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

package org.apache.nifi.audit;

import org.apache.nifi.action.Action;
import org.apache.nifi.action.FlowChangeAction;
import org.apache.nifi.action.Operation;
import org.apache.nifi.admin.service.AuditService;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.StandardProcessorNode;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.controller.service.StandardControllerServiceNode;
import org.apache.nifi.controller.service.StandardControllerServiceProvider;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.web.dao.impl.StandardProcessGroupDAO;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("unused")
@ExtendWith({SpringExtension.class, MockitoExtension.class})
@ContextConfiguration(classes = {TestProcessGroupAuditor.AuditorConfiguration.class})
public class TestProcessGroupAuditor {

    private static final String PG_1 = "processGroup1";
    private static final String PROC_1 = "processor1";
    private static final String PROC_2 = "processor2";
    private static final String INPUT_PORT = "inputPort";
    private static final String OUTPUT_PORT = "outputPort";
    private static final String CS_1 = "controllerService1";
    private static final String USER_ID = "user-id";

    @Autowired
    private StandardProcessGroupDAO processGroupDAO;
    @Autowired
    private ProcessGroupAuditor processGroupAuditor;

    @Autowired
    private AuditService auditService;
    @Autowired
    private FlowController flowController;
    @Mock
    private Authentication authentication;
    @Mock
    private FlowManager flowManager;
    @Mock
    private ProcessGroup processGroup;

    @Captor
    private ArgumentCaptor<List<Action>> argumentCaptorActions;

    @BeforeEach
    void setUp() {
        reset(flowController);
        reset(auditService);

        processGroupDAO.setFlowController(flowController);
        processGroupAuditor.setAuditService(auditService);
        processGroupAuditor.setProcessGroupDAO(processGroupDAO);

        final SecurityContext securityContext = SecurityContextHolder.getContext();
        securityContext.setAuthentication(authentication);
        when(authentication.getName()).thenReturn(USER_ID);
    }

    @Test
    void testVerifyStartProcessGroupAuditing() {
        final ProcessorNode processor1 = mock(StandardProcessorNode.class);
        final ProcessorNode processor2 = mock(StandardProcessorNode.class);
        when(processor1.getProcessGroup()).thenReturn(processGroup);
        when(processor2.getProcessGroup()).thenReturn(processGroup);
        when(processor1.getConnectableType()).thenReturn(ConnectableType.PROCESSOR);
        when(processor2.getConnectableType()).thenReturn(ConnectableType.PROCESSOR);

        when(flowManager.getGroup(eq(PG_1))).thenReturn(processGroup);
        when(flowManager.findConnectable(eq(PROC_1))).thenReturn(processor1);
        when(flowManager.findConnectable(eq(PROC_2))).thenReturn(processor2);
        when(flowController.getFlowManager()).thenReturn(flowManager);

        processGroupDAO.scheduleComponents(PG_1, ScheduledState.RUNNING, new HashSet<>(Arrays.asList(PROC_1, PROC_2)));

        verify(auditService).addActions(argumentCaptorActions.capture());
        final List<Action> actions = argumentCaptorActions.getValue();
        assertEquals(1, actions.size());
        final Action action = actions.getFirst();
        assertInstanceOf(FlowChangeAction.class, action);
        assertEquals(USER_ID, action.getUserIdentity());
        assertEquals("ProcessGroup", action.getSourceType().name());
        assertEquals(Operation.Start, action.getOperation());
    }

    @Test
    void testVerifyEnableProcessGroupAuditing() {
        final ProcessorNode processor1 = mock(StandardProcessorNode.class);
        final ProcessorNode processor2 = mock(StandardProcessorNode.class);
        final Port inputPort = mock(Port.class);
        final Port outputPort = mock(Port.class);

        when(processor1.getName()).thenReturn(PROC_1);
        when(processor1.getProcessGroup()).thenReturn(processGroup);
        when(processor1.getConnectableType()).thenReturn(ConnectableType.PROCESSOR);
        when(processor2.getName()).thenReturn(PROC_2);
        when(processor2.getProcessGroup()).thenReturn(processGroup);
        when(processor2.getConnectableType()).thenReturn(ConnectableType.PROCESSOR);

        when(inputPort.getName()).thenReturn(INPUT_PORT);
        when(inputPort.getProcessGroup()).thenReturn(processGroup);
        when(inputPort.getConnectableType()).thenReturn(ConnectableType.INPUT_PORT);

        when(outputPort.getName()).thenReturn(OUTPUT_PORT);
        when(outputPort.getProcessGroup()).thenReturn(processGroup);
        when(outputPort.getConnectableType()).thenReturn(ConnectableType.OUTPUT_PORT);

        when(processGroup.findProcessor(PROC_1)).thenReturn(processor1);
        when(processGroup.findProcessor(PROC_2)).thenReturn(processor2);
        when(processGroup.findProcessor(INPUT_PORT)).thenReturn(null);
        when(processGroup.findProcessor(OUTPUT_PORT)).thenReturn(null);
        when(processGroup.findInputPort(INPUT_PORT)).thenReturn(inputPort);
        when(processGroup.findInputPort(OUTPUT_PORT)).thenReturn(null);
        when(processGroup.findOutputPort(OUTPUT_PORT)).thenReturn(outputPort);

        when(flowManager.getGroup(eq(PG_1))).thenReturn(processGroup);
        when(flowManager.findConnectable(eq(PROC_1))).thenReturn(processor1);
        when(flowManager.findConnectable(eq(PROC_2))).thenReturn(processor2);
        when(flowManager.findConnectable(eq(INPUT_PORT))).thenReturn(inputPort);
        when(flowManager.findConnectable(eq(OUTPUT_PORT))).thenReturn(outputPort);
        when(flowController.getFlowManager()).thenReturn(flowManager);

        processGroupDAO.enableComponents(PG_1, ScheduledState.STOPPED, new HashSet<>(Arrays.asList(PROC_1, PROC_2, INPUT_PORT, OUTPUT_PORT)));

        verify(auditService, times(2)).addActions(argumentCaptorActions.capture());
        final List<List<Action>> actions = argumentCaptorActions.getAllValues();
        assertEquals(2, actions.size());
        final Iterator<List<Action>> actionsIterator = actions.iterator();

        // pg enabled
        final List<Action> pgActions = actionsIterator.next();
        assertEquals(1, pgActions.size());
        final Action pgAction = pgActions.iterator().next();
        assertInstanceOf(FlowChangeAction.class, pgAction);
        assertEquals(USER_ID, pgAction.getUserIdentity());
        assertEquals("ProcessGroup", pgAction.getSourceType().name());
        assertEquals(Operation.Enable, pgAction.getOperation());

        List<Action> componentActions = actionsIterator.next();
        assertEquals(4, componentActions.size());
        componentActions.sort(Comparator.comparing(Action::getSourceName));

        // inputPort enabled
        final Iterator<Action> actionIterator = componentActions.iterator();
        Action componentAction = actionIterator.next();
        assertInstanceOf(FlowChangeAction.class, componentAction);
        assertEquals(USER_ID, componentAction.getUserIdentity());
        assertEquals("InputPort", componentAction.getSourceType().name());
        assertEquals(INPUT_PORT, componentAction.getSourceName());
        assertEquals(Operation.Enable, componentAction.getOperation());

        // outputPort enabled
        componentAction = actionIterator.next();
        assertInstanceOf(FlowChangeAction.class, componentAction);
        assertEquals(USER_ID, componentAction.getUserIdentity());
        assertEquals("OutputPort", componentAction.getSourceType().name());
        assertEquals(OUTPUT_PORT, componentAction.getSourceName());
        assertEquals(Operation.Enable, componentAction.getOperation());

        // processors enabled
        componentAction = actionIterator.next();
        assertInstanceOf(FlowChangeAction.class, componentAction);
        assertEquals(USER_ID, componentAction.getUserIdentity());
        assertEquals("Processor", componentAction.getSourceType().name());
        assertEquals(PROC_1, componentAction.getSourceName());
        assertEquals(Operation.Enable, componentAction.getOperation());

        componentAction = actionIterator.next();
        assertInstanceOf(FlowChangeAction.class, componentAction);
        assertEquals(USER_ID, componentAction.getUserIdentity());
        assertEquals("Processor", componentAction.getSourceType().name());
        assertEquals(PROC_2, componentAction.getSourceName());
        assertEquals(Operation.Enable, componentAction.getOperation());
    }

    @Test
    void testVerifyEnableControllerServicesAuditing() {
        final ControllerServiceNode cs = mock(StandardControllerServiceNode.class);
        final ControllerServiceProvider csProvider = mock(StandardControllerServiceProvider.class);

        when(cs.getName()).thenReturn(CS_1);
        when(processGroup.findControllerService(eq(CS_1), eq(true), eq(true))).thenReturn(cs);

        when(flowManager.getGroup(eq(PG_1))).thenReturn(processGroup);
        when(flowManager.getControllerServiceNode(eq(CS_1))).thenReturn(cs);
        when(flowController.getFlowManager()).thenReturn(flowManager);
        when(flowController.getControllerServiceProvider()).thenReturn(csProvider);

        processGroupDAO.activateControllerServices(PG_1, ControllerServiceState.ENABLED, new HashSet<>(Arrays.asList(CS_1)));

        verify(auditService, times(2)).addActions(argumentCaptorActions.capture());
        final List<List<Action>> actions = argumentCaptorActions.getAllValues();
        assertEquals(2, actions.size());
        final Iterator<List<Action>> actionsIterator = actions.iterator();

        final List<Action> pgActions = actionsIterator.next();
        assertEquals(1, pgActions.size());
        final Action pgAction = pgActions.iterator().next();
        assertInstanceOf(FlowChangeAction.class, pgAction);
        assertEquals(USER_ID, pgAction.getUserIdentity());
        assertEquals("ProcessGroup", pgAction.getSourceType().name());
        assertEquals(Operation.Enable, pgAction.getOperation());

        final List<Action> csActions = actionsIterator.next();
        assertEquals(1, csActions.size());
        final Action csAction = csActions.iterator().next();
        assertInstanceOf(FlowChangeAction.class, csAction);
        assertEquals(USER_ID, csAction.getUserIdentity());
        assertEquals("ControllerService", csAction.getSourceType().name());
        assertEquals(CS_1, csAction.getSourceName());
        assertEquals(Operation.Enable, csAction.getOperation());
    }

    @Configuration
    @EnableAspectJAutoProxy(proxyTargetClass = true)
    public static class AuditorConfiguration {
        @Bean
        public StandardProcessGroupDAO processGroupDAO() {
            return new StandardProcessGroupDAO();
        }

        @Bean
        public ProcessGroupAuditor processGroupAuditor() {
            return new ProcessGroupAuditor();
        }


        @Bean
        public AuditService auditService() {
            return mock(AuditService.class);
        }

        @Bean
        public FlowController flowController() {
            return mock(FlowController.class);
        }
    }
}
