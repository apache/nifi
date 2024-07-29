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
import org.apache.nifi.action.Component;
import org.apache.nifi.action.Operation;
import org.apache.nifi.action.details.ActionDetails;
import org.apache.nifi.action.details.FlowChangeConfigureDetails;
import org.apache.nifi.admin.service.AuditService;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserDetails;
import org.apache.nifi.authorization.user.StandardNiFiUser;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.label.StandardLabel;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.web.api.dto.LabelDTO;
import org.apache.nifi.web.dao.ProcessGroupDAO;
import org.apache.nifi.web.dao.impl.StandardLabelDAO;
import org.apache.nifi.controller.label.Label;
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

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith({SpringExtension.class, MockitoExtension.class})
@ContextConfiguration(classes = {TestLabelAuditor.AuditorConfiguration.class})
public class TestLabelAuditor {

    private static final String USER_IDENTITY = "username";

    private static final String GROUP_ID = "group-1";

    private static final String LABEL_ID = "label-1";

    private static final String LABEL = "label";

    private static final String UPDATED_LABEL = "updated-label";

    @Autowired
    AuditService auditService;

    @Mock
    Authentication authentication;

    @Autowired
    FlowController flowController;

    @Mock
    FlowManager flowManager;

    @Mock
    ProcessGroup processGroup;

    @Captor
    ArgumentCaptor<List<Action>> actionsCaptor;

    @Autowired
    StandardLabelDAO labelDao;

    @Autowired
    LabelAuditor labelAuditor;

    @BeforeEach
    void setAuditor() {
        reset(flowController);
        reset(auditService);

        final SecurityContext securityContext = SecurityContextHolder.getContext();
        securityContext.setAuthentication(authentication);
        final NiFiUser user = new StandardNiFiUser.Builder().identity(USER_IDENTITY).build();
        final NiFiUserDetails userDetail = new NiFiUserDetails(user);
        when(authentication.getPrincipal()).thenReturn(userDetail);

        when(flowController.getFlowManager()).thenReturn(flowManager);
        labelDao.setFlowController(flowController);
        labelAuditor.setAuditService(auditService);
    }

    @Test
    void testCreateLabelAdvice() {
        final LabelDTO labelDto = getLabelDto();
        when(flowManager.getGroup(eq(GROUP_ID))).thenReturn(processGroup);
        when(flowManager.createLabel(eq(LABEL_ID), eq(LABEL))).thenReturn(new StandardLabel(LABEL_ID, LABEL));

        final Label label = labelDao.createLabel(GROUP_ID, labelDto);

        assertNotNull(label);
        verify(auditService).addActions(actionsCaptor.capture());
        final List<Action> actions = actionsCaptor.getValue();
        assertActionFound(actions, Operation.Add);
    }

    @Test
    void testRemoveLabelAdvice() {
        setFindLabel();

        labelDao.deleteLabel(LABEL_ID);

        verify(auditService).addActions(actionsCaptor.capture());
        final List<Action> actions = actionsCaptor.getValue();
        assertActionFound(actions, Operation.Remove);
    }

    @Test
    void testUpdateLabelAdvice() {
        setFindLabel();

        final LabelDTO labelDto = getLabelDto();
        labelDto.setLabel(UPDATED_LABEL);

        labelDao.updateLabel(labelDto);

        verify(auditService).addActions(actionsCaptor.capture());
        final List<Action> actions = actionsCaptor.getValue();
        final Action action = assertActionFound(actions, Operation.Configure);

        final ActionDetails actionDetails = action.getActionDetails();
        assertActionDetailsFound(actionDetails);
    }

    @Test
    void testUpdateLabelAdviceLabelUnchanged() {
        setFindLabel();

        final LabelDTO labelDto = getLabelDto();

        labelDao.updateLabel(labelDto);

        verifyNoInteractions(auditService);
    }

    private void setFindLabel() {
        when(flowManager.getRootGroup()).thenReturn(processGroup);
        final Label label = new StandardLabel(LABEL_ID, LABEL);
        label.setProcessGroup(processGroup);
        when(processGroup.findLabel(eq(LABEL_ID))).thenReturn(label);
    }

    private void assertActionDetailsFound(final ActionDetails actionDetails) {
        assertInstanceOf(FlowChangeConfigureDetails.class, actionDetails);
        final FlowChangeConfigureDetails flowChangeConfigureDetails = (FlowChangeConfigureDetails) actionDetails;

        assertEquals(LABEL_ID, flowChangeConfigureDetails.getName());
        assertEquals(LABEL, flowChangeConfigureDetails.getPreviousValue());
        assertEquals(UPDATED_LABEL, flowChangeConfigureDetails.getValue());
    }

    private Action assertActionFound(final List<Action> actions, final Operation operation) {
        assertNotNull(actions);

        final Optional<Action> actionFound = actions.stream().findFirst();
        assertTrue(actionFound.isPresent());

        final Action action = actionFound.get();
        assertEquals(USER_IDENTITY, action.getUserIdentity());
        assertEquals(operation, action.getOperation());
        assertEquals(LABEL_ID, action.getSourceId());
        assertEquals(LABEL_ID, action.getSourceName());
        assertEquals(Component.Label, action.getSourceType());
        assertNotNull(action.getTimestamp());

        return action;
    }

    private LabelDTO getLabelDto() {
        final LabelDTO labelDto = new LabelDTO();
        labelDto.setLabel(LABEL);
        labelDto.setId(LABEL_ID);
        labelDto.setStyle(Collections.emptyMap());
        return labelDto;
    }

    @Configuration
    @EnableAspectJAutoProxy(proxyTargetClass = true)
    public static class AuditorConfiguration {

        @Bean
        public LabelAuditor labelAuditor() {
            return new LabelAuditor();
        }

        @Bean
        public StandardLabelDAO labelDAO() {
            return new StandardLabelDAO();
        }

        @Bean
        public ProcessGroupDAO processGroupDAO() {
            return new StandardProcessGroupDAO();
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
