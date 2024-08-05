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
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.bundle.BundleDetails;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.web.api.dto.BundleDTO;
import org.apache.nifi.web.api.dto.ProcessorConfigDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.dao.ComponentStateDAO;
import org.apache.nifi.web.dao.ProcessGroupDAO;
import org.apache.nifi.web.dao.impl.StandardComponentStateDAO;
import org.apache.nifi.web.dao.impl.StandardProcessGroupDAO;
import org.apache.nifi.web.dao.impl.StandardProcessorDAO;
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
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;


@ExtendWith({SpringExtension.class, MockitoExtension.class})
@ContextConfiguration(classes = {TestProcessorAuditor.AuditorConfiguration.class})
class TestProcessorAuditor {

    private static final String PROC_1 = "processor1";
    private static final String PN_SOURCE_NAME = "sourceName1";
    private static final String PN_ID = "processorNodeId1";
    private static final String GROUP_ID = "group-1";
    private static final String USER_IDENTITY = "user-id";
    private static final BundleCoordinate BUNDLE_COORDINATE = new BundleCoordinate("org.apache.nifi", "nifi-processor-nar", "0.0.0");

    @Autowired
    private StandardProcessorDAO processorDao;
    @Autowired
    private ProcessorAuditor processorAuditor;

    @Autowired
    private AuditService auditService;
    @Mock
    private Authentication authentication;
    @Mock
    private Processor processor;
    @Mock
    private ExtensionManager extensionManager;
    @Mock
    private FlowController flowController;
    @Autowired
    private FlowManager flowManager;
    @Mock
    private ProcessGroup processGroup;
    @Mock
    private ProcessorNode mockProcessorNode;
    @Mock
    private StateManagerProvider mockStateManagerProvider;
    @Mock
    private StateManager mockStateManager;
    @Mock
    private NiFiUserDetails userDetail;

    @Captor
    private ArgumentCaptor<List<Action>> actionsArgumentCaptor;

    @BeforeEach
    void init() {
        reset(flowController);
        reset(auditService);

        SecurityContextHolder.getContext().setAuthentication(authentication);
        final NiFiUser user = new StandardNiFiUser.Builder().identity(USER_IDENTITY).build();
        userDetail = new NiFiUserDetails(user);
        when(authentication.getPrincipal()).thenReturn(userDetail);

        when(flowController.getFlowManager()).thenReturn(flowManager);

        processorDao.setFlowController(flowController);
        processorAuditor.setAuditService(auditService);
    }

    @Test
    void testCreateProcessorAdvice() {
        final ProcessorDTO processorDto = getProcessorDto();

        when(mockProcessorNode.getIdentifier()).thenReturn(PN_ID);
        when(mockProcessorNode.getName()).thenReturn(PN_SOURCE_NAME);
        when(mockProcessorNode.getProcessor()).thenReturn(processor);
        when(mockProcessorNode.getCanonicalClassName()).thenReturn(mockProcessorNode.getClass().getCanonicalName());

        when(processor.getIdentifier()).thenReturn(PN_ID);

        when(flowController.getExtensionManager()).thenReturn(extensionManager);
        when(flowController.getControllerServiceProvider()).thenReturn(mock(ControllerServiceProvider.class));
        when(flowController.getStateManagerProvider()).thenReturn(mockStateManagerProvider);

        when(flowManager.getGroup(GROUP_ID)).thenReturn(processGroup);
        when(flowManager.createProcessor(anyString(), anyString(), any())).thenReturn(mockProcessorNode);

        final Bundle bundle = getBundle();

        when(extensionManager.getBundle(any(BundleCoordinate.class))).thenReturn(bundle);
        when(extensionManager.getBundles(anyString())).thenReturn(Collections.singletonList(bundle));


        when(mockStateManagerProvider.getStateManager(PN_ID)).thenReturn(mockStateManager);

        final ProcessorNode processor = processorDao.createProcessor(GROUP_ID, processorDto);

        assertNotNull(processor);
        verify(auditService).addActions(actionsArgumentCaptor.capture());
        final List<Action> actions = actionsArgumentCaptor.getValue();
        assertActionFound(actions, Operation.Add);
    }

    @Test
    void testRemoveProcessorAdvice() {
        when(flowManager.getRootGroup()).thenReturn(processGroup);

        ProcessorNode mockProcessorNode = mock(ProcessorNode.class);
        when(mockProcessorNode.getIdentifier()).thenReturn(PN_ID);
        when(mockProcessorNode.getName()).thenReturn(PN_SOURCE_NAME);
        when(mockProcessorNode.getProcessGroup()).thenReturn(processGroup);

        when(processGroup.findProcessor(PN_ID)).thenReturn(mockProcessorNode);

        processorDao.deleteProcessor(PN_ID);

        verify(auditService).addActions(actionsArgumentCaptor.capture());
        final List<Action> actions = actionsArgumentCaptor.getValue();
        assertActionFound(actions, Operation.Remove);
    }

    @Test
    void testUpdateProcessorAdvice() {
        when(flowController.getExtensionManager()).thenReturn(extensionManager);
        when(flowManager.getRootGroup()).thenReturn(processGroup);

        final Bundle bundle = getBundle();

        when(extensionManager.getBundle(any(BundleCoordinate.class))).thenReturn(bundle);
        when(extensionManager.getBundles(anyString())).thenReturn(Collections.singletonList(bundle));

        when(mockProcessorNode.getIdentifier()).thenReturn(PN_ID);
        when(mockProcessorNode.getName()).thenReturn(PN_SOURCE_NAME);
        when(mockProcessorNode.getBundleCoordinate()).thenReturn(BUNDLE_COORDINATE);
        when(mockProcessorNode.getCanonicalClassName()).thenReturn(ProcessorNode.class.getCanonicalName());
        when(mockProcessorNode.getProcessGroup()).thenReturn(processGroup);

        when(processGroup.findProcessor(PN_ID)).thenReturn(mockProcessorNode);

        final ProcessorDTO processorDto = getProcessorDto();
        ProcessorConfigDTO config = new ProcessorConfigDTO();
        config.setComments("comment1");
        config.setRunDurationMillis(100L);

        processorDto.setConfig(config);
        processorDto.setId(PN_ID);

        processorDao.updateProcessor(processorDto);

        verify(auditService).addActions(actionsArgumentCaptor.capture());
        final List<Action> actions = actionsArgumentCaptor.getValue();
        final Action action = assertActionFound(actions, Operation.Configure);

        final ActionDetails actionDetails = action.getActionDetails();
        assertUpdateActionDetailsFound(actionDetails);
    }

    @Test
    void testSensitivePropertyDeletesHistory() {
        when(flowController.getExtensionManager()).thenReturn(extensionManager);
        when(flowManager.getRootGroup()).thenReturn(processGroup);

        final Bundle bundle = getBundle();

        when(extensionManager.getBundle(any(BundleCoordinate.class))).thenReturn(bundle);
        when(extensionManager.getBundles(anyString())).thenReturn(Collections.singletonList(bundle));

        when(mockProcessorNode.getIdentifier()).thenReturn(PN_ID);
        when(mockProcessorNode.getName()).thenReturn(PN_SOURCE_NAME);
        when(mockProcessorNode.getBundleCoordinate()).thenReturn(BUNDLE_COORDINATE);
        when(mockProcessorNode.getCanonicalClassName()).thenReturn(ProcessorNode.class.getCanonicalName());
        when(mockProcessorNode.getProcessGroup()).thenReturn(processGroup);

        final String propertyName = "sensitive-property-descriptor-1";

        PropertyDescriptor propertyDescriptor = new PropertyDescriptor.Builder()
                .name(propertyName)
                .sensitive(true)
                .build();

        when(mockProcessorNode.getPropertyDescriptor("dynamicSensitiveProperty1")).thenReturn(propertyDescriptor);

        when(processGroup.findProcessor(PN_ID)).thenReturn(mockProcessorNode);

        final ProcessorDTO processorDto = getProcessorDto();
        ProcessorConfigDTO config = new ProcessorConfigDTO();
        config.setProperties(Collections.singletonMap("dynamicSensitiveProperty1", "asd"));
        config.setSensitiveDynamicPropertyNames(Collections.singleton("dynamicSensitiveProperty1"));
        config.setRunDurationMillis(100L);

        processorDto.setConfig(config);
        processorDto.setId(PN_ID);

        processorDao.updateProcessor(processorDto);

        ArgumentCaptor<String> propertyNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> componentIdCaptor = ArgumentCaptor.forClass(String.class);

        verify(auditService).deletePreviousValues(propertyNameCaptor.capture(), componentIdCaptor.capture());

        assertEquals(propertyName, propertyNameCaptor.getValue());
        assertEquals(PN_ID, componentIdCaptor.getValue());
    }

    @Test
    void testUpdateProcessorAdviceProcessorUnchanged() {
        when(flowController.getExtensionManager()).thenReturn(extensionManager);
        when(flowManager.getRootGroup()).thenReturn(processGroup);

        final Bundle bundle = getBundle();

        when(extensionManager.getBundle(any(BundleCoordinate.class))).thenReturn(bundle);
        when(extensionManager.getBundles(anyString())).thenReturn(Collections.singletonList(bundle));

        when(mockProcessorNode.getIdentifier()).thenReturn(PN_ID);
        when(mockProcessorNode.getBundleCoordinate()).thenReturn(BUNDLE_COORDINATE);
        when(mockProcessorNode.getCanonicalClassName()).thenReturn(ProcessorNode.class.getCanonicalName());
        when(mockProcessorNode.getProcessGroup()).thenReturn(processGroup);

        when(processGroup.findProcessor(PN_ID)).thenReturn(mockProcessorNode);

        final ProcessorDTO processorDto = getProcessorDto();
        processorDto.setId(PN_ID);

        processorDao.updateProcessor(processorDto);

        verifyNoInteractions(auditService);
    }

    private void assertUpdateActionDetailsFound(final ActionDetails actionDetails) {
        assertInstanceOf(FlowChangeConfigureDetails.class, actionDetails);
        final FlowChangeConfigureDetails flowChangeConfigureDetails = (FlowChangeConfigureDetails) actionDetails;

        assertEquals("Comments", flowChangeConfigureDetails.getName());
        assertNotEquals("Comments", flowChangeConfigureDetails.getPreviousValue());
    }

    private Action assertActionFound(final List<Action> actions, final Operation operation) {
        assertNotNull(actions);

        final Optional<Action> actionFound = actions.stream().findFirst();
        assertTrue(actionFound.isPresent());

        final Action action = actionFound.get();
        assertEquals(USER_IDENTITY, action.getUserIdentity());
        assertEquals(operation, action.getOperation());
        assertEquals(PN_ID, action.getSourceId());
        assertEquals(PN_SOURCE_NAME, action.getSourceName());
        assertEquals(Component.Processor, action.getSourceType());
        assertNotNull(action.getTimestamp());

        return action;
    }

    private ProcessorDTO getProcessorDto() {
        final ProcessorDTO processorDto = new ProcessorDTO();
        processorDto.setId(PROC_1);
        processorDto.setType("Processor");
        final BundleDTO bundleDto = new BundleDTO();
        bundleDto.setArtifact(BUNDLE_COORDINATE.getId());
        bundleDto.setGroup(BUNDLE_COORDINATE.getGroup());
        bundleDto.setVersion(BUNDLE_COORDINATE.getVersion());
        processorDto.setBundle(bundleDto);
        processorDto.setExtensionMissing(false);
        processorDto.setStyle(Collections.emptyMap());

        return processorDto;
    }

    private Bundle getBundle() {
        final BundleDetails bundleDetails = new BundleDetails.Builder()
                .coordinate(BUNDLE_COORDINATE)
                .workingDir(new File("."))
                .build();
        return new Bundle(bundleDetails, this.getClass().getClassLoader());
    }

    @Configuration
    @EnableAspectJAutoProxy(proxyTargetClass = true)
    public static class AuditorConfiguration {

        @Bean
        public ProcessorAuditor processorAuditor() {
            return new ProcessorAuditor();
        }

        @Bean
        public StandardProcessorDAO processorDAO() {
            return new StandardProcessorDAO();
        }

        @Bean
        public ProcessGroupDAO processGroupDAO() {
            return new StandardProcessGroupDAO();
        }

        @Bean
        public ComponentStateDAO componentStateDAO() {
            return new StandardComponentStateDAO();
        }

        @Bean
        public StateManagerProvider stateManagerProvider() {
            return mock(StateManagerProvider.class);
        }

        @Bean
        public FlowManager flowManager() {
            return mock(FlowManager.class);
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
