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
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserDetails;
import org.apache.nifi.authorization.user.StandardNiFiUser;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.StandardProcessorNode;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.web.dao.impl.StandardProcessGroupDAO;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("unused")
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = {
        StandardProcessGroupDAO.class,
        ProcessGroupAuditor.class
})
@EnableAspectJAutoProxy(proxyTargetClass = true)
public class TestProcessGroupAuditor {

    @Autowired
    private StandardProcessGroupDAO processGroupDAO;
    @Autowired
    private ProcessGroupAuditor processGroupAuditor;

    @Captor
    private ArgumentCaptor<List<Action>> argumentCaptorActions;

    @Test
    void testVerifyProcessGroupAuditing() {
        final SecurityContext securityContext = SecurityContextHolder.getContext();
        final Authentication authentication = mock(Authentication.class);
        securityContext.setAuthentication(authentication);
        final NiFiUser user = new StandardNiFiUser.Builder().identity("user-id").build();
        final NiFiUserDetails userDetail = new NiFiUserDetails(user);
        when(authentication.getPrincipal()).thenReturn(userDetail);

        final ProcessGroup processGroup = mock(ProcessGroup.class);
        final ProcessorNode processor1 = mock(StandardProcessorNode.class);
        final ProcessorNode processor2 = mock(StandardProcessorNode.class);
        when(processor1.getProcessGroup()).thenReturn(processGroup);
        when(processor2.getProcessGroup()).thenReturn(processGroup);
        when(processor1.getConnectableType()).thenReturn(ConnectableType.PROCESSOR);
        when(processor2.getConnectableType()).thenReturn(ConnectableType.PROCESSOR);

        final FlowManager flowManager = mock(FlowManager.class);
        when(flowManager.getGroup(eq("pg1"))).thenReturn(processGroup);
        when(flowManager.findConnectable(eq("proc1"))).thenReturn(processor1);
        when(flowManager.findConnectable(eq("proc2"))).thenReturn(processor2);
        final FlowController flowController = mock(FlowController.class);
        when(flowController.getFlowManager()).thenReturn(flowManager);

        processGroupAuditor.setProcessGroupDAO(processGroupDAO);
        final AuditService auditServiceMock = mock(AuditService.class);
        processGroupAuditor.setAuditService(auditServiceMock);
        processGroupDAO.setFlowController(flowController);
        processGroupDAO.scheduleComponents("pg1", ScheduledState.RUNNING, new HashSet<>(Arrays.asList("proc1", "proc2")));

        verify(auditServiceMock).addActions(argumentCaptorActions.capture());
        final List<Action> actions = argumentCaptorActions.getValue();
        assertEquals(1, actions.size());
        final Action action = actions.iterator().next();
        assertInstanceOf(FlowChangeAction.class, action);
        assertEquals(user.getIdentity(), action.getUserIdentity());
        assertEquals("ProcessGroup", action.getSourceType().name());
        assertEquals(Operation.Start, action.getOperation());
    }
}
