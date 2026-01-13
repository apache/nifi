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
package org.apache.nifi.web;

import org.apache.nifi.action.Component;
import org.apache.nifi.action.FlowChangeAction;
import org.apache.nifi.action.Operation;
import org.apache.nifi.admin.service.AuditService;
import org.apache.nifi.authorization.AccessDeniedException;
import org.apache.nifi.authorization.AuthorizableLookup;
import org.apache.nifi.authorization.AuthorizationRequest;
import org.apache.nifi.authorization.AuthorizationResult;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.ComponentAuthorizable;
import org.apache.nifi.authorization.Group;
import org.apache.nifi.authorization.Resource;
import org.apache.nifi.authorization.User;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.resource.ResourceFactory;
import org.apache.nifi.authorization.resource.ResourceType;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserDetails;
import org.apache.nifi.authorization.user.StandardNiFiUser.Builder;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.Counter;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.PropertyConfiguration;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.flow.ExecutionEngine;
import org.apache.nifi.flow.ExternalControllerServiceReference;
import org.apache.nifi.flow.ParameterProviderReference;
import org.apache.nifi.flow.VersionedComponent;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.flow.VersionedPropertyDescriptor;
import org.apache.nifi.flow.VersionedReportingTask;
import org.apache.nifi.flow.VersionedReportingTaskSnapshot;
import org.apache.nifi.flowanalysis.EnforcementPolicy;
import org.apache.nifi.groups.ComponentAdditions;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.groups.VersionedComponentAdditions;
import org.apache.nifi.history.History;
import org.apache.nifi.history.HistoryQuery;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.registry.flow.FlowRegistryUtil;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshot;
import org.apache.nifi.registry.flow.VersionControlInformation;
import org.apache.nifi.registry.flow.diff.ComparableDataFlow;
import org.apache.nifi.registry.flow.diff.DifferenceType;
import org.apache.nifi.registry.flow.diff.FlowComparator;
import org.apache.nifi.registry.flow.diff.FlowComparatorVersionedStrategy;
import org.apache.nifi.registry.flow.diff.StandardComparableDataFlow;
import org.apache.nifi.registry.flow.diff.StandardFlowComparator;
import org.apache.nifi.registry.flow.diff.StaticDifferenceDescriptor;
import org.apache.nifi.registry.flow.mapping.FlowMappingOptions;
import org.apache.nifi.registry.flow.mapping.InstantiatedVersionedProcessGroup;
import org.apache.nifi.registry.flow.mapping.NiFiRegistryFlowMapper;
import org.apache.nifi.reporting.Bulletin;
import org.apache.nifi.reporting.BulletinFactory;
import org.apache.nifi.reporting.ComponentType;
import org.apache.nifi.reporting.UserAwareEventAccess;
import org.apache.nifi.services.FlowService;
import org.apache.nifi.util.MockBulletinRepository;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.validation.RuleViolation;
import org.apache.nifi.validation.RuleViolationsManager;
import org.apache.nifi.web.api.dto.CounterDTO;
import org.apache.nifi.web.api.dto.CountersDTO;
import org.apache.nifi.web.api.dto.CountersSnapshotDTO;
import org.apache.nifi.web.api.dto.DtoFactory;
import org.apache.nifi.web.api.dto.EntityFactory;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupDTO;
import org.apache.nifi.web.api.dto.action.HistoryDTO;
import org.apache.nifi.web.api.dto.action.HistoryQueryDTO;
import org.apache.nifi.web.api.dto.status.StatusHistoryDTO;
import org.apache.nifi.web.api.entity.ActionEntity;
import org.apache.nifi.web.api.entity.AffectedComponentEntity;
import org.apache.nifi.web.api.entity.ClearBulletinsForGroupResultsEntity;
import org.apache.nifi.web.api.entity.ClearBulletinsResultEntity;
import org.apache.nifi.web.api.entity.CopyRequestEntity;
import org.apache.nifi.web.api.entity.CopyResponseEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.StatusHistoryEntity;
import org.apache.nifi.web.api.entity.TenantEntity;
import org.apache.nifi.web.api.entity.TenantsEntity;
import org.apache.nifi.web.controller.ControllerFacade;
import org.apache.nifi.web.dao.ProcessGroupDAO;
import org.apache.nifi.web.dao.RemoteProcessGroupDAO;
import org.apache.nifi.web.dao.UserDAO;
import org.apache.nifi.web.dao.UserGroupDAO;
import org.apache.nifi.web.revision.NaiveRevisionManager;
import org.apache.nifi.web.revision.RevisionManager;
import org.apache.nifi.web.revision.RevisionUpdate;
import org.apache.nifi.web.revision.StandardRevisionUpdate;
import org.apache.nifi.web.security.token.NiFiAuthenticationToken;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StandardNiFiServiceFacadeTest {

    private static final String USER_PREFIX = "user";
    private static final String USER_1 = String.format("%s-1", USER_PREFIX);
    private static final String USER_1_ID = UUID.nameUUIDFromBytes(USER_1.getBytes(StandardCharsets.UTF_8)).toString();
    private static final String USER_2 = String.format("%s-2", USER_PREFIX);
    private static final String USER_GROUP_1 = String.format("%s-group-1", USER_PREFIX);
    private static final String USER_GROUP_1_ID = UUID.nameUUIDFromBytes(USER_GROUP_1.getBytes(StandardCharsets.UTF_8)).toString();

    private static final Integer UNKNOWN_ACTION_ID = 0;

    private static final Integer ACTION_ID_1 = 1;
    private static final String PROCESSOR_ID_1 = "processor-1";

    private static final Integer ACTION_ID_2 = 2;
    private static final String PROCESSOR_ID_2 = "processor-2";

    private static final String GROUP_NAME_1 = "group-name-1";
    private static final String GROUP_NAME_2 = "group-name-2";
    private static final String PROCESSOR_NAME_1 = "Processor1";
    private static final String PROCESSOR_NAME_2 = "Processor2";
    private static final String BULLETIN_CATEGORY = "Log Message";
    private static final String BULLETIN_SEVERITY = "ERROR";
    private static final String BULLETIN_MESSAGE_1 = "Error1";
    private static final String BULLETIN_MESSAGE_2 = "Error2";
    private static final String PATH_TO_GROUP_1 = "Path1";
    private static final String PATH_TO_GROUP_2 = "Path2";
    private static final String RANDOM_GROUP_ID = "randomGroupId";

    private StandardNiFiServiceFacade serviceFacade;
    private Authorizer authorizer;
    private FlowController flowController;
    private ProcessGroupDAO processGroupDAO;
    private RuleViolationsManager ruleViolationsManager;

    @BeforeEach
    public void setUp() throws Exception {
        // audit service
        final AuditService auditService = mock(AuditService.class);
        when(auditService.getAction(anyInt())).then(invocation -> {
            final Integer actionId = invocation.getArgument(0);

            FlowChangeAction action = null;
            if (ACTION_ID_1.equals(actionId)) {
                action = getAction(actionId, PROCESSOR_ID_1);
            } else if (ACTION_ID_2.equals(actionId)) {
                action = getAction(actionId, PROCESSOR_ID_2);
            }

            return action;
        });
        when(auditService.getActions(any(HistoryQuery.class))).then(invocation -> {
            final History history = new History();
            history.setActions(Arrays.asList(getAction(ACTION_ID_1, PROCESSOR_ID_1), getAction(ACTION_ID_2, PROCESSOR_ID_2)));
            return history;
        });


        // authorizable lookup
        final AuthorizableLookup authorizableLookup = mock(AuthorizableLookup.class);
        when(authorizableLookup.getProcessor(Mockito.anyString())).then(getProcessorInvocation -> {
            final String processorId = getProcessorInvocation.getArgument(0);

            // processor-2 is no longer part of the flow
            if (processorId.equals(PROCESSOR_ID_2)) {
                throw new ResourceNotFoundException("");
            }

            // component authorizable
            final ComponentAuthorizable componentAuthorizable = mock(ComponentAuthorizable.class);
            when(componentAuthorizable.getAuthorizable()).then(getAuthorizableInvocation -> {

                // authorizable
                return new Authorizable() {
                    @Override
                    public Authorizable getParentAuthorizable() {
                        return null;
                    }

                    @Override
                    public Resource getResource() {
                        return ResourceFactory.getComponentResource(ResourceType.Processor, processorId, processorId);
                    }
                };
            });

            return componentAuthorizable;
        });

        // authorizer
        authorizer = mock(Authorizer.class);
        when(authorizer.authorize(any(AuthorizationRequest.class))).then(invocation -> {
            final AuthorizationRequest request = invocation.getArgument(0);

            AuthorizationResult result = AuthorizationResult.denied();
            if (request.getResource().getIdentifier().endsWith(PROCESSOR_ID_1)) {
                if (USER_1.equals(request.getIdentity())) {
                    result = AuthorizationResult.approved();
                }
            } else if (request.getResource().equals(ResourceFactory.getControllerResource())) {
                if (USER_2.equals(request.getIdentity())) {
                    result = AuthorizationResult.approved();
                }
            }

            return result;
        });

        // flow controller
        flowController = mock(FlowController.class);
        when(flowController.getResource()).thenCallRealMethod();
        when(flowController.getParentAuthorizable()).thenCallRealMethod();

        final UserAwareEventAccess eventAccess = mock(UserAwareEventAccess.class);
        when(flowController.getEventAccess()).thenReturn(eventAccess);
        when(eventAccess.getGroupStatus(anyString(), any(NiFiUser.class), anyInt())).thenReturn(mock(ProcessGroupStatus.class));

        // props
        final NiFiProperties properties = mock(NiFiProperties.class);
        when(properties.getFlowServiceWriteDelay()).thenReturn("0 sec");

        // flow service
        final FlowService flowService = mock(FlowService.class);
        doNothing().when(flowService).saveFlowChanges(any(TimeUnit.class), anyLong());

        // controller facade
        final ControllerFacade controllerFacade = new ControllerFacade();
        controllerFacade.setFlowController(flowController);
        controllerFacade.setProperties(properties);
        controllerFacade.setFlowService(flowService);

        processGroupDAO = mock(ProcessGroupDAO.class, Answers.RETURNS_DEEP_STUBS);
        ruleViolationsManager = mock(RuleViolationsManager.class);

        serviceFacade = new StandardNiFiServiceFacade();
        serviceFacade.setAuditService(auditService);
        serviceFacade.setAuthorizableLookup(authorizableLookup);
        serviceFacade.setAuthorizer(authorizer);
        serviceFacade.setEntityFactory(new EntityFactory());
        serviceFacade.setDtoFactory(new DtoFactory());
        serviceFacade.setControllerFacade(controllerFacade);
        serviceFacade.setProcessGroupDAO(processGroupDAO);
        serviceFacade.setRuleViolationsManager(ruleViolationsManager);

    }

    @Test
    public void testGetComponentsAffectedByFlowUpdate_WithNewStatelessProcessGroup_ReproducesNPE() {
        final String groupId = UUID.randomUUID().toString();
        final ProcessGroup processGroup = mock(ProcessGroup.class);
        when(processGroupDAO.getProcessGroup(groupId)).thenReturn(processGroup);
        when(processGroup.getAncestorServiceIds()).thenReturn(Collections.emptySet());

        final FlowManager flowManager = mock(FlowManager.class);
        final ExtensionManager extensionManager = mock(ExtensionManager.class);
        when(flowController.getFlowManager()).thenReturn(flowManager);
        when(flowController.getExtensionManager()).thenReturn(extensionManager);

        final StandardNiFiServiceFacade serviceFacadeSpy = spy(serviceFacade);
        final NiFiRegistryFlowMapper flowMapper = mock(NiFiRegistryFlowMapper.class);
        doReturn(flowMapper).when(serviceFacadeSpy).makeNiFiRegistryFlowMapper(extensionManager);

        final InstantiatedVersionedProcessGroup localRoot = new InstantiatedVersionedProcessGroup("local-root-instance", groupId);
        when(flowMapper.mapProcessGroup(any(ProcessGroup.class), any(ControllerServiceProvider.class), any(FlowManager.class), eq(true)))
                .thenReturn(localRoot);

        // Build proposed (updated) flow with a NEW child Process Group configured with Stateless Execution Engine
        final VersionedProcessGroup proposedRoot = new VersionedProcessGroup();
        proposedRoot.setIdentifier("root");
        proposedRoot.setName("root");

        final VersionedProcessGroup newChild = new VersionedProcessGroup();
        newChild.setIdentifier("child");
        newChild.setName("child");
        newChild.setGroupIdentifier("root");
        newChild.setExecutionEngine(ExecutionEngine.STATELESS);
        proposedRoot.getProcessGroups().add(newChild);

        final RegisteredFlowSnapshot updatedSnapshot = new RegisteredFlowSnapshot();
        updatedSnapshot.setFlowContents(proposedRoot);

        final ComparableDataFlow localFlow = new StandardComparableDataFlow("Current Flow", localRoot);
        final ComparableDataFlow proposedFlow = new StandardComparableDataFlow("New Flow", proposedRoot);
        final FlowComparator flowComparator = new StandardFlowComparator(
                localFlow,
                proposedFlow,
                Collections.emptySet(),
                new StaticDifferenceDescriptor(),
                Function.identity(),
                VersionedComponent::getIdentifier,
                FlowComparatorVersionedStrategy.DEEP);

        final org.apache.nifi.registry.flow.diff.FlowComparison comparison = flowComparator.compare();
        final boolean hasExecEngineChange = comparison.getDifferences().stream()
                .anyMatch(d -> d.getDifferenceType() == DifferenceType.EXECUTION_ENGINE_CHANGED
                        && d.getComponentA() == null
                        && d.getComponentB() instanceof VersionedProcessGroup
                        && "child".equals(d.getComponentB().getIdentifier()));
        assertTrue(hasExecEngineChange, "Expected EXECUTION_ENGINE_CHANGED difference for Stateless child group");

        // Act: Should not throw after fix; no local components are affected by a new Stateless child group
        final Set<AffectedComponentEntity> affected = serviceFacadeSpy.getComponentsAffectedByFlowUpdate(groupId, updatedSnapshot);
        assertNotNull(affected);
        assertTrue(affected.isEmpty(), "No local components should be affected for added Stateless group");
    }

    private FlowChangeAction getAction(final Integer actionId, final String processorId) {
        final FlowChangeAction action = new FlowChangeAction();
        action.setId(actionId);
        action.setSourceId(processorId);
        action.setSourceType(Component.Processor);
        action.setOperation(Operation.Add);
        return action;
    }

    @Test
    public void testGetUnknownAction() {
        assertThrows(ResourceNotFoundException.class, () -> serviceFacade.getAction(UNKNOWN_ACTION_ID));
    }

    @Test
    public void testGetActionApprovedThroughAction() {
        // set the user
        final Authentication authentication = new NiFiAuthenticationToken(new NiFiUserDetails(new Builder().identity(USER_1).build()));
        SecurityContextHolder.getContext().setAuthentication(authentication);

        // get the action
        final ActionEntity entity = serviceFacade.getAction(ACTION_ID_1);

        // verify
        assertEquals(ACTION_ID_1, entity.getId());
        assertTrue(entity.getCanRead());

        // resource exists and is approved, no need to check the controller
        verify(authorizer, times(1)).authorize(argThat(o -> o.getResource().getIdentifier().endsWith(PROCESSOR_ID_1)));
        verify(authorizer, times(0)).authorize(argThat(o -> o.getResource().equals(ResourceFactory.getControllerResource())));
    }

    @Test
    public void testGetActionDeniedDespiteControllerAccess() {
        // set the user
        final Authentication authentication = new NiFiAuthenticationToken(new NiFiUserDetails(new Builder().identity(USER_2).build()));
        SecurityContextHolder.getContext().setAuthentication(authentication);

        assertThrows(AccessDeniedException.class, () -> serviceFacade.getAction(ACTION_ID_1));
        // resource exists, but should trigger access denied and will not check the controller
        verify(authorizer, times(1)).authorize(argThat(o -> o.getResource().getIdentifier().endsWith(PROCESSOR_ID_1)));
        verify(authorizer, times(0)).authorize(argThat(o -> o.getResource().equals(ResourceFactory.getControllerResource())));
    }

    @Test
    public void testGetStatusHistory() {
        // given
        final Date generated = new Date();
        final StatusHistoryDTO dto = new StatusHistoryDTO();
        dto.setGenerated(generated);
        final ControllerFacade controllerFacade = mock(ControllerFacade.class);
        Mockito.when(controllerFacade.getNodeStatusHistory()).thenReturn(dto);
        serviceFacade.setControllerFacade(controllerFacade);

        // when
        final StatusHistoryEntity result = serviceFacade.getNodeStatusHistory();

        // then
        Mockito.verify(controllerFacade).getNodeStatusHistory();
        assertNotNull(result);
        assertEquals(generated, result.getStatusHistory().getGenerated());
    }

    @Test
    public void testGetActionApprovedThroughController() {
        // set the user
        final Authentication authentication = new NiFiAuthenticationToken(new NiFiUserDetails(new Builder().identity(USER_2).build()));
        SecurityContextHolder.getContext().setAuthentication(authentication);

        // get the action
        final ActionEntity entity = serviceFacade.getAction(ACTION_ID_2);

        // verify
        assertEquals(ACTION_ID_2, entity.getId());
        assertTrue(entity.getCanRead());

        // component does not exists, so only checks against the controller
        verify(authorizer, times(0)).authorize(argThat(o -> o.getResource().getIdentifier().endsWith(PROCESSOR_ID_2)));
        verify(authorizer, times(1)).authorize(argThat(o -> o.getResource().equals(ResourceFactory.getControllerResource())));
    }

    @Test
    public void testGetActionsForUser1() {
        // set the user
        final Authentication authentication = new NiFiAuthenticationToken(new NiFiUserDetails(new Builder().identity(USER_1).build()));
        SecurityContextHolder.getContext().setAuthentication(authentication);

        final HistoryDTO dto = serviceFacade.getActions(new HistoryQueryDTO());

        // verify user 1 only has access to actions for processor 1
        dto.getActions().forEach(action -> {
            if (PROCESSOR_ID_1.equals(action.getSourceId())) {
                assertTrue(action.getCanRead());
            } else if (PROCESSOR_ID_2.equals(action.getSourceId())) {
                assertFalse(action.getCanRead());
                assertNull(action.getAction());
            }
        });
    }

    @Test
    public void testGetActionsForUser2() {
        // set the user
        final Authentication authentication = new NiFiAuthenticationToken(new NiFiUserDetails(new Builder().identity(USER_2).build()));
        SecurityContextHolder.getContext().setAuthentication(authentication);

        final HistoryDTO dto = serviceFacade.getActions(new HistoryQueryDTO());

        // verify user 2 only has access to actions for processor 2
        dto.getActions().forEach(action -> {
            if (PROCESSOR_ID_1.equals(action.getSourceId())) {
                assertFalse(action.getCanRead());
                assertNull(action.getAction());
            } else if (PROCESSOR_ID_2.equals(action.getSourceId())) {
                assertTrue(action.getCanRead());
            }
        });
    }

    @Test
    public void testCopyComponents() {
        final String groupId = UUID.randomUUID().toString();
        final ProcessGroup processGroup = mock(ProcessGroup.class);

        when(processGroupDAO.getProcessGroup(groupId)).thenReturn(processGroup);

        final FlowManager flowManager = mock(FlowManager.class);
        final ExtensionManager extensionManager = mock(ExtensionManager.class);
        when(flowController.getFlowManager()).thenReturn(flowManager);
        when(flowController.getExtensionManager()).thenReturn(extensionManager);

        final ControllerServiceProvider controllerServiceProvider = mock(ControllerServiceProvider.class);
        when(flowController.getControllerServiceProvider()).thenReturn(controllerServiceProvider);

        final VersionControlInformation versionControlInformation = mock(VersionControlInformation.class);
        when(processGroup.getVersionControlInformation()).thenReturn(versionControlInformation);

        // use spy to mock the make() method for generating a new flow mapper to make this testable
        final StandardNiFiServiceFacade serviceFacadeSpy = spy(serviceFacade);
        final NiFiRegistryFlowMapper flowMapper = mock(NiFiRegistryFlowMapper.class);
        doReturn(flowMapper).when(serviceFacadeSpy).makeNiFiRegistryFlowMapper(eq(extensionManager), any(FlowMappingOptions.class));

        final InstantiatedVersionedProcessGroup nonVersionedProcessGroup = mock(InstantiatedVersionedProcessGroup.class);
        when(flowMapper.mapProcessGroup(processGroup, controllerServiceProvider, flowManager, true)).thenReturn(nonVersionedProcessGroup);

        final String controllerServiceId = "controllerServiceId";
        final String processorOneId = "processorOneId";
        final String processorTwoId = "processorTwoId";
        final VersionedProcessor one = mock(VersionedProcessor.class);
        when(one.getInstanceIdentifier()).thenReturn(processorOneId);
        final VersionedProcessor two = mock(VersionedProcessor.class);
        when(two.getInstanceIdentifier()).thenReturn(processorTwoId);
        when(two.getProperties()).thenReturn(Map.of("CS Property", controllerServiceId));

        final Set<VersionedProcessor> versionedProcessors = Set.of(one, two);
        when(nonVersionedProcessGroup.getProcessors()).thenReturn(versionedProcessors);

        final ExternalControllerServiceReference externalControllerServiceReference = mock(ExternalControllerServiceReference.class);
        final Map<String, ExternalControllerServiceReference> externalControllerServiceReferences = new LinkedHashMap<>();
        externalControllerServiceReferences.put(controllerServiceId, externalControllerServiceReference);
        when(nonVersionedProcessGroup.getExternalControllerServiceReferences()).thenReturn(externalControllerServiceReferences);

        final CopyRequestEntity copyRequestEntity = new CopyRequestEntity();
        copyRequestEntity.setProcessors(Set.of(processorOneId));
        CopyResponseEntity copyResponseEntity = serviceFacadeSpy.copyComponents(groupId, copyRequestEntity);

        assertNotNull(copyResponseEntity);
        assertEquals(1, copyResponseEntity.getProcessors().size());
        assertEquals(processorOneId, copyResponseEntity.getProcessors().iterator().next().getInstanceIdentifier());
        assertTrue(copyResponseEntity.getExternalControllerServiceReferences().isEmpty());
        assertTrue(copyResponseEntity.getParameterContexts().isEmpty());
        assertTrue(copyResponseEntity.getParameterProviders().isEmpty());

        final CopyRequestEntity copyRequestEntityTwo = new CopyRequestEntity();
        copyRequestEntityTwo.setProcessors(Set.of(processorTwoId));
        copyResponseEntity = serviceFacadeSpy.copyComponents(groupId, copyRequestEntityTwo);

        assertNotNull(copyResponseEntity);
        assertEquals(1, copyResponseEntity.getProcessors().size());
        assertEquals(processorTwoId, copyResponseEntity.getProcessors().iterator().next().getInstanceIdentifier());
        assertEquals(1, copyResponseEntity.getExternalControllerServiceReferences().size());
        assertTrue(copyResponseEntity.getParameterContexts().isEmpty());
        assertTrue(copyResponseEntity.getParameterProviders().isEmpty());
    }

    @Test
    public void testPasteComponents() {
        // set the user
        final Authentication authentication = new NiFiAuthenticationToken(new NiFiUserDetails(new Builder().identity(USER_1).build()));
        SecurityContextHolder.getContext().setAuthentication(authentication);

        final String groupId = "groupId";
        final String seed = "seed";

        final String sensitiveValue = "sensitiveValue";
        final String sensitiveProperty = "sensitiveProperty";

        final FlowManager flowManager = mock(FlowManager.class);
        when(flowController.getFlowManager()).thenReturn(flowManager);

        final RevisionManager revisionManager = new NaiveRevisionManager();
        serviceFacade.setRevisionManager(revisionManager);

        final VersionedPropertyDescriptor versionedPropertyDescriptor = new VersionedPropertyDescriptor();
        versionedPropertyDescriptor.setName(sensitiveProperty);
        versionedPropertyDescriptor.setSensitive(true);

        final Map<String, VersionedPropertyDescriptor> propertyDescriptors = new HashMap<>();
        propertyDescriptors.put(sensitiveProperty, versionedPropertyDescriptor);

        final Map<String, String> properties = new HashMap<>();
        properties.put(sensitiveProperty, null);

        final String instanceId = "67890";
        final VersionedProcessor versionedProcessor = new VersionedProcessor();
        versionedProcessor.setIdentifier("12345");
        versionedProcessor.setInstanceIdentifier(instanceId);
        versionedProcessor.setPropertyDescriptors(propertyDescriptors);
        versionedProcessor.setProperties(properties);

        final PropertyDescriptor propertyDescriptor = mock(PropertyDescriptor.class);
        when(propertyDescriptor.getName()).thenReturn(sensitiveProperty);
        when(propertyDescriptor.isSensitive()).thenReturn(true);

        final Map<PropertyDescriptor, PropertyConfiguration> copiedInstanceProperties = new HashMap<>();
        copiedInstanceProperties.put(propertyDescriptor, null);

        final ProcessorNode copiedInstance = mock(ProcessorNode.class);
        when(copiedInstance.getProperties()).thenReturn(copiedInstanceProperties);
        when(copiedInstance.getRawPropertyValue(propertyDescriptor)).thenReturn(sensitiveValue);
        when(flowManager.getProcessorNode(eq(instanceId))).thenReturn(copiedInstance);

        final VersionedComponentAdditions additions = new VersionedComponentAdditions.Builder()
                .setProcessors(Set.of(versionedProcessor))
                .build();

        when(processGroupDAO.addVersionedComponents(groupId, additions, seed)).thenReturn(new ComponentAdditions.Builder().build());

        final ArgumentCaptor<VersionedComponentAdditions> additionsCaptor = ArgumentCaptor.forClass(VersionedComponentAdditions.class);

        serviceFacade.pasteComponents(new Revision(0L, "", groupId), groupId, additions, seed);

        verify(processGroupDAO).addVersionedComponents(eq(groupId), additionsCaptor.capture(), eq(seed));
        final VersionedComponentAdditions capturedAdditions = additionsCaptor.getValue();

        // verify the sensitive value was mapped to the local instance
        assertEquals(sensitiveValue, capturedAdditions.getProcessors().iterator().next().getProperties().get(propertyDescriptor.getName()));
    }

    @Test
    public void testGetCurrentFlowSnapshotByGroupId() {
        final String groupId = UUID.randomUUID().toString();
        final ProcessGroup processGroup = mock(ProcessGroup.class);

        when(processGroupDAO.getProcessGroup(groupId)).thenReturn(processGroup);

        final FlowManager flowManager = mock(FlowManager.class);
        final ExtensionManager extensionManager = mock(ExtensionManager.class);
        when(flowController.getFlowManager()).thenReturn(flowManager);
        when(flowController.getExtensionManager()).thenReturn(extensionManager);

        final ControllerServiceProvider controllerServiceProvider = mock(ControllerServiceProvider.class);
        when(flowController.getControllerServiceProvider()).thenReturn(controllerServiceProvider);

        final VersionControlInformation versionControlInformation = mock(VersionControlInformation.class);
        when(processGroup.getVersionControlInformation()).thenReturn(versionControlInformation);

        // use spy to mock the make() method for generating a new flow mapper to make this testable
        final StandardNiFiServiceFacade serviceFacadeSpy = spy(serviceFacade);
        final NiFiRegistryFlowMapper flowMapper = mock(NiFiRegistryFlowMapper.class);
        when(serviceFacadeSpy.makeNiFiRegistryFlowMapper(extensionManager)).thenReturn(flowMapper);

        final InstantiatedVersionedProcessGroup nonVersionedProcessGroup = mock(InstantiatedVersionedProcessGroup.class);
        when(flowMapper.mapNonVersionedProcessGroup(processGroup, controllerServiceProvider)).thenReturn(nonVersionedProcessGroup);

        final Map<String, ParameterProviderReference> parameterProviderReferences = new HashMap<>();
        final String parameterName = "foo";
        final VersionedParameterContext versionedParameterContext = mock(VersionedParameterContext.class);
        when(versionedParameterContext.getName()).thenReturn(parameterName);
        final Map<String, VersionedParameterContext> parameterContexts = new LinkedHashMap<>();
        parameterContexts.put(parameterName, versionedParameterContext);
        when(flowMapper.mapParameterContexts(processGroup, true, parameterProviderReferences)).thenReturn(parameterContexts);

        final ExternalControllerServiceReference externalControllerServiceReference = mock(ExternalControllerServiceReference.class);
        final Map<String, ExternalControllerServiceReference> externalControllerServiceReferences = new LinkedHashMap<>();
        externalControllerServiceReferences.put("test", externalControllerServiceReference);
        when(nonVersionedProcessGroup.getExternalControllerServiceReferences()).thenReturn(externalControllerServiceReferences);

        final RegisteredFlowSnapshot versionedFlowSnapshot = serviceFacadeSpy.getCurrentFlowSnapshotByGroupId(groupId);

        assertEquals(nonVersionedProcessGroup, versionedFlowSnapshot.getFlowContents());
        assertEquals(1, versionedFlowSnapshot.getParameterContexts().size());
        assertEquals(versionedParameterContext, versionedFlowSnapshot.getParameterContexts().get(parameterName));
        assertEquals(externalControllerServiceReferences, versionedFlowSnapshot.getExternalControllerServices());
        assertEquals(FlowRegistryUtil.FLOW_ENCODING_VERSION, versionedFlowSnapshot.getFlowEncodingVersion());
        assertNull(versionedFlowSnapshot.getFlow());
        assertNull(versionedFlowSnapshot.getBucket());
        assertNull(versionedFlowSnapshot.getSnapshotMetadata());
    }

    @Test
    public void testGetCurrentFlowSnapshotByGroupIdWithReferencedControllerServices() {
        final String groupId = UUID.randomUUID().toString();
        final ProcessGroup processGroup = mock(ProcessGroup.class);
        final ProcessGroup parentProcessGroup = mock(ProcessGroup.class);

        final Set<ControllerServiceNode> parentControllerServices = new HashSet<>();
        final ControllerServiceNode parentControllerService1 = mock(ControllerServiceNode.class);
        final ControllerServiceNode parentControllerService2 = mock(ControllerServiceNode.class);
        parentControllerServices.add(parentControllerService1);
        parentControllerServices.add(parentControllerService2);

        when(processGroupDAO.getProcessGroup(groupId)).thenReturn(processGroup);
        when(processGroup.getParent()).thenReturn(parentProcessGroup);
        when(parentProcessGroup.getControllerServices(anyBoolean())).thenReturn(parentControllerServices);

        final FlowManager flowManager = mock(FlowManager.class);
        final ExtensionManager extensionManager = mock(ExtensionManager.class);
        when(flowController.getFlowManager()).thenReturn(flowManager);
        when(flowController.getExtensionManager()).thenReturn(extensionManager);

        final ControllerServiceProvider controllerServiceProvider = mock(ControllerServiceProvider.class);
        when(flowController.getControllerServiceProvider()).thenReturn(controllerServiceProvider);

        final VersionControlInformation versionControlInformation = mock(VersionControlInformation.class);
        when(processGroup.getVersionControlInformation()).thenReturn(versionControlInformation);

        // use spy to mock the make() method for generating a new flow mapper to make this testable
        final StandardNiFiServiceFacade serviceFacadeSpy = spy(serviceFacade);
        final NiFiRegistryFlowMapper flowMapper = mock(NiFiRegistryFlowMapper.class);
        when(serviceFacadeSpy.makeNiFiRegistryFlowMapper(extensionManager)).thenReturn(flowMapper);

        final InstantiatedVersionedProcessGroup nonVersionedProcessGroup = spy(new InstantiatedVersionedProcessGroup(UUID.randomUUID().toString(), UUID.randomUUID().toString()));
        when(flowMapper.mapNonVersionedProcessGroup(processGroup, controllerServiceProvider)).thenReturn(nonVersionedProcessGroup);

        final VersionedControllerService versionedControllerService1 = mock(VersionedControllerService.class);
        final VersionedControllerService versionedControllerService2 = mock(VersionedControllerService.class);

        Mockito.when(versionedControllerService1.getIdentifier()).thenReturn("test");
        Mockito.when(versionedControllerService2.getIdentifier()).thenReturn("test2");

        when(flowMapper.mapControllerService(same(parentControllerService1), same(controllerServiceProvider), anySet(), anyMap())).thenReturn(versionedControllerService1);
        when(flowMapper.mapControllerService(same(parentControllerService2), same(controllerServiceProvider), anySet(), anyMap())).thenReturn(versionedControllerService2);
        when(flowMapper.mapParameterContexts(processGroup, true, null)).thenReturn(new HashMap<>());

        final ExternalControllerServiceReference externalControllerServiceReference = mock(ExternalControllerServiceReference.class);
        final Map<String, ExternalControllerServiceReference> externalControllerServiceReferences = new LinkedHashMap<>();
        externalControllerServiceReferences.put("test", externalControllerServiceReference);
        when(nonVersionedProcessGroup.getExternalControllerServiceReferences()).thenReturn(externalControllerServiceReferences);

        final RegisteredFlowSnapshot versionedFlowSnapshot = serviceFacadeSpy.getCurrentFlowSnapshotByGroupIdWithReferencedControllerServices(groupId);

        assertEquals(1, versionedFlowSnapshot.getFlowContents().getControllerServices().size());
        assertEquals("test", versionedFlowSnapshot.getFlowContents().getControllerServices().iterator().next().getIdentifier());
    }

    @Test
    public void testIsAnyProcessGroupUnderVersionControl_None() {
        final String groupId = UUID.randomUUID().toString();
        final ProcessGroup processGroup = mock(ProcessGroup.class);
        final ProcessGroup childProcessGroup = mock(ProcessGroup.class);

        when(processGroupDAO.getProcessGroup(groupId)).thenReturn(processGroup);

        when(processGroup.getVersionControlInformation()).thenReturn(null);
        when(processGroup.getProcessGroups()).thenReturn(Collections.singleton(childProcessGroup));
        when(childProcessGroup.getVersionControlInformation()).thenReturn(null);

        assertFalse(serviceFacade.isAnyProcessGroupUnderVersionControl(groupId));
    }

    @Test
    public void testIsAnyProcessGroupUnderVersionControl_PrimaryGroup() {
        final String groupId = UUID.randomUUID().toString();
        final ProcessGroup processGroup = mock(ProcessGroup.class);

        when(processGroupDAO.getProcessGroup(groupId)).thenReturn(processGroup);

        final VersionControlInformation vci = mock(VersionControlInformation.class);
        when(processGroup.getVersionControlInformation()).thenReturn(vci);
        when(processGroup.getProcessGroups()).thenReturn(new HashSet<>());

        assertTrue(serviceFacade.isAnyProcessGroupUnderVersionControl(groupId));
    }

    @Test
    public void testIsAnyProcessGroupUnderVersionControl_ChildGroup() {
        final String groupId = UUID.randomUUID().toString();
        final ProcessGroup processGroup = mock(ProcessGroup.class);
        final ProcessGroup childProcessGroup = mock(ProcessGroup.class);

        when(processGroupDAO.getProcessGroup(groupId)).thenReturn(processGroup);

        final VersionControlInformation vci = mock(VersionControlInformation.class);
        when(processGroup.getVersionControlInformation()).thenReturn(null);
        when(processGroup.getProcessGroups()).thenReturn(Collections.singleton(childProcessGroup));
        when(childProcessGroup.getVersionControlInformation()).thenReturn(vci);

        assertTrue(serviceFacade.isAnyProcessGroupUnderVersionControl(groupId));
    }

    @Test
    public void testVerifyUpdateRemoteProcessGroups() {
        // GIVEN
        RemoteProcessGroupDAO remoteProcessGroupDAO = mock(RemoteProcessGroupDAO.class);
        serviceFacade.setRemoteProcessGroupDAO(remoteProcessGroupDAO);

        String groupId = "groupId";
        boolean shouldTransmit = true;

        String remoteProcessGroupId1 = "remoteProcessGroupId1";
        String remoteProcessGroupId2 = "remoteProcessGroupId2";

        List<RemoteProcessGroup> remoteProcessGroups = Arrays.asList(
                // Current 'transmitting' status should not influence the verification, which should be solely based on the 'shouldTransmitting' value
                mockRemoteProcessGroup(remoteProcessGroupId1, true),
                mockRemoteProcessGroup(remoteProcessGroupId2, false)
        );

        List<RemoteProcessGroupDTO> expected = Arrays.asList(
                createRemoteProcessGroupDTO(remoteProcessGroupId1, shouldTransmit),
                createRemoteProcessGroupDTO(remoteProcessGroupId2, shouldTransmit)
        );

        when(processGroupDAO.getProcessGroup(groupId).findAllRemoteProcessGroups()).thenReturn(remoteProcessGroups);
        expected.stream()
                .map(RemoteProcessGroupDTO::getId)
                .forEach(remoteProcessGroupId -> when(remoteProcessGroupDAO.hasRemoteProcessGroup(remoteProcessGroupId)).thenReturn(true));


        // WHEN
        serviceFacade.verifyUpdateRemoteProcessGroups(groupId, shouldTransmit);

        // THEN
        ArgumentCaptor<RemoteProcessGroupDTO> remoteProcessGroupDTOArgumentCaptor = ArgumentCaptor.forClass(RemoteProcessGroupDTO.class);

        verify(remoteProcessGroupDAO, times(remoteProcessGroups.size())).verifyUpdate(remoteProcessGroupDTOArgumentCaptor.capture());

        List<RemoteProcessGroupDTO> actual = remoteProcessGroupDTOArgumentCaptor.getAllValues();

        assertEquals(toMap(expected), toMap(actual));
    }

    private Map<String, Boolean> toMap(List<RemoteProcessGroupDTO> list) {
        return list.stream().collect(Collectors.toMap(RemoteProcessGroupDTO::getId, RemoteProcessGroupDTO::isTransmitting));
    }

    private RemoteProcessGroup mockRemoteProcessGroup(String identifier, boolean transmitting) {
        RemoteProcessGroup remoteProcessGroup = mock(RemoteProcessGroup.class);

        when(remoteProcessGroup.getIdentifier()).thenReturn(identifier);
        when(remoteProcessGroup.isTransmitting()).thenReturn(transmitting);

        return remoteProcessGroup;
    }

    private RemoteProcessGroupDTO createRemoteProcessGroupDTO(String id, boolean transmitting) {
        RemoteProcessGroupDTO remoteProcessGroup = new RemoteProcessGroupDTO();

        remoteProcessGroup.setId(id);
        remoteProcessGroup.setTransmitting(transmitting);

        return remoteProcessGroup;
    }

    @Test
    public void testUpdateProcessGroup_WithProcessorBulletin() {
        //GIVEN
        final String groupId = UUID.randomUUID().toString();
        final ProcessGroup processGroup = mock(ProcessGroup.class);
        when(processGroup.getIdentifier()).thenReturn(groupId);

        ProcessGroupStatus processGroupStatus = new ProcessGroupStatus();
        processGroupStatus.setId(groupId);
        processGroupStatus.setName(GROUP_NAME_1);
        processGroupStatus.setStatelessActiveThreadCount(1);

        final ControllerFacade controllerFacade = mock(ControllerFacade.class);
        when(controllerFacade.getProcessGroupStatus(any())).thenReturn(processGroupStatus);

        final StandardNiFiServiceFacade serviceFacadeSpy = spy(serviceFacade);
        serviceFacadeSpy.setControllerFacade(controllerFacade);
        ProcessGroupDTO processGroupDTO = new ProcessGroupDTO();
        processGroupDTO.setId(groupId);
        when(processGroupDAO.getProcessGroup(groupId)).thenReturn(processGroup);
        when(processGroupDAO.updateProcessGroup(processGroupDTO)).thenReturn(processGroup);

        final RevisionManager revisionManager = mock(RevisionManager.class);
        Revision revision = new Revision(1L, "a", "b");
        final FlowModification lastModification = new FlowModification(revision, "a");
        RevisionUpdate<Object> snapshot = new StandardRevisionUpdate<>(processGroupDTO, lastModification);
        when(revisionManager.updateRevision(any(), any(), any())).thenReturn(snapshot);
        serviceFacadeSpy.setRevisionManager(revisionManager);

        MockTestBulletinRepository bulletinRepository = new MockTestBulletinRepository();
        serviceFacadeSpy.setBulletinRepository(bulletinRepository);

        //add a bulletin for a processor in the current processor group
        bulletinRepository.addBulletin(
                BulletinFactory.createBulletin(groupId, GROUP_NAME_1, PROCESSOR_ID_1,
                        ComponentType.PROCESSOR, PROCESSOR_NAME_1,
                        BULLETIN_CATEGORY, BULLETIN_SEVERITY, BULLETIN_MESSAGE_1, PATH_TO_GROUP_1));

        //add a bulletin for a processor in a different processor group
        bulletinRepository.addBulletin(
                BulletinFactory.createBulletin(RANDOM_GROUP_ID, GROUP_NAME_2, PROCESSOR_ID_2,
                        ComponentType.PROCESSOR, PROCESSOR_NAME_2,
                        BULLETIN_CATEGORY, BULLETIN_SEVERITY, BULLETIN_MESSAGE_2, PATH_TO_GROUP_2));

        //WHEN
        ProcessGroupEntity result = serviceFacadeSpy.updateProcessGroup(revision, processGroupDTO);

        //THEN
        assertNotNull(result);
        assertEquals(1, result.getBulletins().size());
        assertEquals(groupId, result.getBulletins().get(0).getGroupId());
    }

    @Test
    public void testUpdateProcessGroup_WithNoBulletinForProcessGroup() {
        //GIVEN
        final String groupId = UUID.randomUUID().toString();
        final ProcessGroup processGroup = mock(ProcessGroup.class);
        when(processGroup.getIdentifier()).thenReturn(groupId);

        ProcessGroupStatus processGroupStatus = new ProcessGroupStatus();
        processGroupStatus.setId(groupId);
        processGroupStatus.setName(GROUP_NAME_1);
        processGroupStatus.setStatelessActiveThreadCount(1);

        final ControllerFacade controllerFacade = mock(ControllerFacade.class);
        when(controllerFacade.getProcessGroupStatus(any())).thenReturn(processGroupStatus);

        final StandardNiFiServiceFacade serviceFacadeSpy = spy(serviceFacade);
        serviceFacadeSpy.setControllerFacade(controllerFacade);

        ProcessGroupDTO processGroupDTO = new ProcessGroupDTO();
        processGroupDTO.setId(groupId);
        when(processGroupDAO.getProcessGroup(groupId)).thenReturn(processGroup);
        when(processGroupDAO.updateProcessGroup(processGroupDTO)).thenReturn(processGroup);

        final RevisionManager revisionManager = mock(RevisionManager.class);
        Revision revision = new Revision(1L, "a", "b");
        final FlowModification lastModification = new FlowModification(revision, "a");

        RevisionUpdate<Object> snapshot = new StandardRevisionUpdate<>(processGroupDTO, lastModification);
        when(revisionManager.updateRevision(any(), any(), any())).thenReturn(snapshot);
        serviceFacadeSpy.setRevisionManager(revisionManager);

        MockTestBulletinRepository bulletinRepository = new MockTestBulletinRepository();
        serviceFacadeSpy.setBulletinRepository(bulletinRepository);

        //add a bulletin for a processor in a different processor group
        bulletinRepository.addBulletin(
                BulletinFactory.createBulletin(RANDOM_GROUP_ID, GROUP_NAME_2, PROCESSOR_ID_2,
                        ComponentType.PROCESSOR, PROCESSOR_NAME_2,
                        BULLETIN_CATEGORY, BULLETIN_SEVERITY, BULLETIN_MESSAGE_2, PATH_TO_GROUP_2));

        //WHEN
        ProcessGroupEntity result = serviceFacadeSpy.updateProcessGroup(revision, processGroupDTO);

        //THEN
        assertNotNull(result);
        assertEquals(0, result.getBulletins().size());
    }

    @Test
    public void testUpdateProcessGroup_WithProcessorGroupBulletin() {
        //GIVEN
        final String groupId = UUID.randomUUID().toString();
        final ProcessGroup processGroup = mock(ProcessGroup.class);
        when(processGroup.getIdentifier()).thenReturn(groupId);

        ProcessGroupStatus processGroupStatus = new ProcessGroupStatus();
        processGroupStatus.setId(groupId);
        processGroupStatus.setName(GROUP_NAME_1);
        processGroupStatus.setStatelessActiveThreadCount(0);

        final ControllerFacade controllerFacade = mock(ControllerFacade.class);
        when(controllerFacade.getProcessGroupStatus(any())).thenReturn(processGroupStatus);

        final StandardNiFiServiceFacade serviceFacadeSpy = spy(serviceFacade);
        serviceFacadeSpy.setControllerFacade(controllerFacade);

        ProcessGroupDTO processGroupDTO = new ProcessGroupDTO();
        processGroupDTO.setId(groupId);
        when(processGroupDAO.getProcessGroup(groupId)).thenReturn(processGroup);
        when(processGroupDAO.updateProcessGroup(processGroupDTO)).thenReturn(processGroup);

        final RevisionManager revisionManager = mock(RevisionManager.class);
        Revision revision = new Revision(1L, "a", "b");
        final FlowModification lastModification = new FlowModification(revision, "a");

        RevisionUpdate<Object> snapshot = new StandardRevisionUpdate<>(processGroupDTO, lastModification);
        when(revisionManager.updateRevision(any(), any(), any())).thenReturn(snapshot);
        serviceFacadeSpy.setRevisionManager(revisionManager);

        MockTestBulletinRepository bulletinRepository = new MockTestBulletinRepository();
        serviceFacadeSpy.setBulletinRepository(bulletinRepository);

        //add a bulletin for current processor group, meaning the source is also the process group
        bulletinRepository.addBulletin(
                BulletinFactory.createBulletin(groupId, GROUP_NAME_1, groupId,
                        ComponentType.PROCESSOR, GROUP_NAME_1,
                        BULLETIN_CATEGORY, BULLETIN_SEVERITY, BULLETIN_MESSAGE_1, PATH_TO_GROUP_1));

        //add a bulletin for a processor in a different processor group
        bulletinRepository.addBulletin(
                BulletinFactory.createBulletin(RANDOM_GROUP_ID, GROUP_NAME_2, PROCESSOR_ID_2,
                        ComponentType.PROCESSOR, PROCESSOR_NAME_2,
                        BULLETIN_CATEGORY, BULLETIN_SEVERITY, BULLETIN_MESSAGE_2, PATH_TO_GROUP_2));

        //WHEN
        ProcessGroupEntity result = serviceFacadeSpy.updateProcessGroup(revision, processGroupDTO);

        //THEN
        assertNotNull(result);
        assertEquals(1, result.getBulletins().size());
        assertEquals(groupId, result.getBulletins().get(0).getGroupId());
    }

    @Test
    public void testSearchTenantsNullQuery() {
        setupSearchTenants();

        final TenantsEntity tenantsEntity = serviceFacade.searchTenants(null);

        assertUserFound(tenantsEntity);
        assertUserGroupFound(tenantsEntity);
    }

    @Test
    public void testSearchTenantsMatchedQuery() {
        setupSearchTenants();

        final TenantsEntity tenantsEntity = serviceFacade.searchTenants(USER_PREFIX);

        assertUserFound(tenantsEntity);
        assertUserGroupFound(tenantsEntity);
    }

    @Test
    public void testSearchTenantsGroupMatchedQuery() {
        setupSearchTenants();

        final TenantsEntity tenantsEntity = serviceFacade.searchTenants(USER_GROUP_1);

        assertUserGroupFound(tenantsEntity);

        final Collection<TenantEntity> usersFound = tenantsEntity.getUsers();
        assertTrue(usersFound.isEmpty());
    }

    @Test
    public void testSearchTenantsNotMatchedQuery() {
        setupSearchTenants();

        final TenantsEntity tenantsEntity = serviceFacade.searchTenants(String.class.getSimpleName());

        assertNotNull(tenantsEntity);

        final Collection<TenantEntity> usersFound = tenantsEntity.getUsers();
        assertTrue(usersFound.isEmpty());

        final Collection<TenantEntity> userGroupsFound = tenantsEntity.getUserGroups();
        assertTrue(userGroupsFound.isEmpty());
    }

    private void setupSearchTenants() {
        final Authentication authentication = new NiFiAuthenticationToken(new NiFiUserDetails(new Builder().identity(USER_1).build()));
        SecurityContextHolder.getContext().setAuthentication(authentication);

        authorizer = mock(Authorizer.class);
        serviceFacade.setAuthorizer(authorizer);
        final AuthorizableLookup authorizableLookup = mock(AuthorizableLookup.class);
        serviceFacade.setAuthorizableLookup(authorizableLookup);

        final Authorizable authorizable = mock(Authorizable.class);
        when(authorizableLookup.getTenant()).thenReturn(authorizable);

        final RevisionManager revisionManager = mock(RevisionManager.class);
        serviceFacade.setRevisionManager(revisionManager);
        final Revision revision = new Revision(1L, USER_1_ID, USER_1_ID);
        when(revisionManager.getRevision(anyString())).thenReturn(revision);

        final UserDAO userDAO = mock(UserDAO.class);
        serviceFacade.setUserDAO(userDAO);
        final UserGroupDAO userGroupDAO = mock(UserGroupDAO.class);
        serviceFacade.setUserGroupDAO(userGroupDAO);

        final User user = new User.Builder().identity(USER_1).identifier(USER_1_ID).build();
        final Set<User> users = Collections.singleton(user);
        when(userDAO.getUsers()).thenReturn(users);

        final Group group = new Group.Builder().name(USER_GROUP_1).identifier(USER_GROUP_1_ID).build();
        final Set<Group> groups = Collections.singleton(group);
        when(userGroupDAO.getUserGroups()).thenReturn(groups);
    }

    private void assertUserFound(final TenantsEntity tenantsEntity) {
        assertNotNull(tenantsEntity);
        final Collection<TenantEntity> usersFound = tenantsEntity.getUsers();
        assertFalse(usersFound.isEmpty());
        final TenantEntity firstUserFound = usersFound.iterator().next();
        assertEquals(USER_1_ID, firstUserFound.getId());
    }

    private void assertUserGroupFound(final TenantsEntity tenantsEntity) {
        assertNotNull(tenantsEntity);
        final Collection<TenantEntity> userGroupsFound = tenantsEntity.getUserGroups();
        assertFalse(userGroupsFound.isEmpty());
        final TenantEntity firstUserGroup = userGroupsFound.iterator().next();
        assertEquals(USER_GROUP_1_ID, firstUserGroup.getId());
    }

    private static class MockTestBulletinRepository extends MockBulletinRepository {

        List<Bulletin> bulletinList;

        public MockTestBulletinRepository() {
            bulletinList = new ArrayList<>();
        }

        @Override
        public void addBulletin(Bulletin bulletin) {
            bulletinList.add(bulletin);
        }

        @Override
        public List<Bulletin> findBulletinsForSource(String sourceId) {
            List<Bulletin> ans = new ArrayList<>();
            for (Bulletin b : bulletinList) {
                if (sourceId.equals(b.getSourceId())) {
                    ans.add(b);
                }
            }
            return ans;
        }

        @Override
        public List<Bulletin> findBulletinsForGroupBySource(String groupId) {
            List<Bulletin> ans = new ArrayList<>();
            for (Bulletin b : bulletinList) {
                if (b.getGroupId().equals(groupId)) {
                    ans.add(b);
                }
            }
            return ans;
        }

        @Override
        public int clearBulletinsForComponent(String sourceId, Instant fromTimestamp) {
            int cleared = 0;
            final Iterator<Bulletin> iterator = bulletinList.iterator();
            while (iterator.hasNext()) {
                final Bulletin bulletin = iterator.next();
                if (sourceId.equals(bulletin.getSourceId())
                        && bulletin.getTimestamp() != null
                        && !bulletin.getTimestamp().toInstant().isAfter(fromTimestamp)) {
                    iterator.remove();
                    cleared++;
                }
            }
            return cleared;
        }

        @Override
        public int clearBulletinsForComponents(Collection<String> sourceIds, Instant fromTimestamp) {
            if (sourceIds == null || sourceIds.isEmpty()) {
                throw new IllegalArgumentException("Source ID cannot be null or empty");
            }

            int cleared = 0;
            final Iterator<Bulletin> iterator = bulletinList.iterator();
            while (iterator.hasNext()) {
                final Bulletin bulletin = iterator.next();
                if (sourceIds.contains(bulletin.getSourceId())
                        && bulletin.getTimestamp() != null
                        && !bulletin.getTimestamp().toInstant().isAfter(fromTimestamp)) {
                    iterator.remove();
                    cleared++;
                }
            }
            return cleared;
        }

        public void addTestBulletin(String sourceId, String message, Instant timestamp) {
            TestBulletin bulletin = new TestBulletin(sourceId, message, Date.from(timestamp));
            bulletinList.add(bulletin);
        }

    }

    private static class TestBulletin extends Bulletin {
        private final String sourceId;
        private final String message;
        private final Date timestamp;

        public TestBulletin(String sourceId, String message, Date timestamp) {
            super(System.nanoTime());
            this.sourceId = sourceId;
            this.message = message;
            this.timestamp = timestamp;
        }

        @Override
        public String getSourceId() {
            return sourceId;
        }

        @Override
        public String getMessage() {
            return message;
        }

        @Override
        public Date getTimestamp() {
            return timestamp;
        }

        @Override
        public String getCategory() {
            return "Test";
        }

        @Override
        public String getLevel() {
            return "INFO";
        }

        @Override
        public ComponentType getSourceType() {
            return ComponentType.PROCESSOR;
        }

        @Override
        public String getSourceName() {
            return "Test Component";
        }

        @Override
        public String getGroupId() {
            return "test-group";
        }

        @Override
        public String getGroupName() {
            return "Test Group";
        }

        @Override
        public String getGroupPath() {
            return "/";
        }

        @Override
        public String getNodeAddress() {
            return null;
        }

        @Override
        public String getFlowFileUuid() {
            return null;
        }
    }

    @Test
    public void testGetRuleViolationsForRoot() {
        // GIVEN
        int ruleViolationCounter = 0;

        String groupId = "groupId";
        String childGroupId = "childGroupId";
        String grandChildGroupId = "grandChildGroupId";

        RuleViolation ruleViolation1 = createRuleViolation(groupId, ruleViolationCounter++);
        RuleViolation ruleViolation2 = createRuleViolation(groupId, ruleViolationCounter++);

        RuleViolation childRuleViolation1 = createRuleViolation(childGroupId, ruleViolationCounter++);
        RuleViolation childRuleViolation2 = createRuleViolation(childGroupId, ruleViolationCounter++);

        RuleViolation grandChildRuleViolation1 = createRuleViolation(grandChildGroupId, ruleViolationCounter++);
        RuleViolation grandChildRuleViolation2 = createRuleViolation(grandChildGroupId, ruleViolationCounter++);
        RuleViolation grandChildRuleViolation3 = createRuleViolation(grandChildGroupId, ruleViolationCounter++);

        ProcessGroup grandChildProcessGroup = mockProcessGroup(
                grandChildGroupId,
                Collections.emptyList(),
                Arrays.asList(grandChildRuleViolation1, grandChildRuleViolation2, grandChildRuleViolation3)
        );
        ProcessGroup childProcessGroup = mockProcessGroup(
                childGroupId,
                Arrays.asList(grandChildProcessGroup),
                Arrays.asList(childRuleViolation1, childRuleViolation2)
        );
        ProcessGroup processGroup = mockProcessGroup(
                groupId,
                Arrays.asList(childProcessGroup),
                Arrays.asList(ruleViolation1, ruleViolation2)
        );

        Collection<RuleViolation> expected = new HashSet<>(Arrays.asList(
                ruleViolation1, ruleViolation2,
                childRuleViolation1, childRuleViolation2,
                grandChildRuleViolation1, grandChildRuleViolation2, grandChildRuleViolation3
        ));

        when(processGroupDAO.getProcessGroup(FlowManager.ROOT_GROUP_ID_ALIAS)).thenReturn(processGroup);
        when(ruleViolationsManager.getAllRuleViolations()).thenReturn(expected);

        // WHEN
        Collection<RuleViolation> actual = serviceFacade.getRuleViolationStream(processGroup.getIdentifier()).collect(Collectors.toSet());

        // THEN
        assertEquals(expected, actual);
    }

    @Test
    public void testGetRuleViolationsForRootWithAlias() {
        // GIVEN
        int ruleViolationCounter = 0;

        String groupId = "groupId";
        String childGroupId = "childGroupId";
        String grandChildGroupId = "grandChildGroupId";

        RuleViolation ruleViolation1 = createRuleViolation(groupId, ruleViolationCounter++);
        RuleViolation ruleViolation2 = createRuleViolation(groupId, ruleViolationCounter++);

        RuleViolation childRuleViolation1 = createRuleViolation(childGroupId, ruleViolationCounter++);
        RuleViolation childRuleViolation2 = createRuleViolation(childGroupId, ruleViolationCounter++);

        RuleViolation grandChildRuleViolation1 = createRuleViolation(grandChildGroupId, ruleViolationCounter++);
        RuleViolation grandChildRuleViolation2 = createRuleViolation(grandChildGroupId, ruleViolationCounter++);
        RuleViolation grandChildRuleViolation3 = createRuleViolation(grandChildGroupId, ruleViolationCounter++);

        ProcessGroup grandChildProcessGroup = mockProcessGroup(
                grandChildGroupId,
                Collections.emptyList(),
                Arrays.asList(grandChildRuleViolation1, grandChildRuleViolation2, grandChildRuleViolation3)
        );
        ProcessGroup childProcessGroup = mockProcessGroup(
                childGroupId,
                Arrays.asList(grandChildProcessGroup),
                Arrays.asList(childRuleViolation1, childRuleViolation2)
        );
        ProcessGroup processGroup = mockProcessGroup(
                groupId,
                Arrays.asList(childProcessGroup),
                Arrays.asList(ruleViolation1, ruleViolation2)
        );

        Collection<RuleViolation> expected = new HashSet<>(Arrays.asList(
                ruleViolation1, ruleViolation2,
                childRuleViolation1, childRuleViolation2,
                grandChildRuleViolation1, grandChildRuleViolation2, grandChildRuleViolation3
        ));

        when(processGroupDAO.getProcessGroup(FlowManager.ROOT_GROUP_ID_ALIAS)).thenReturn(processGroup);
        when(ruleViolationsManager.getAllRuleViolations()).thenReturn(expected);

        // WHEN
        Collection<RuleViolation> actual = serviceFacade.getRuleViolationStream(FlowManager.ROOT_GROUP_ID_ALIAS).collect(Collectors.toSet());

        // THEN
        assertEquals(expected, actual);
    }


    @Test
    public void testGetRuleViolationsEmpty() {
        // GIVEN
        String groupId = "groupId";

        ProcessGroup processGroup = mockProcessGroup(
                groupId,
                Arrays.asList(),
                Arrays.asList()
        );

        Collection<RuleViolation> expected = new HashSet<>(List.of());

        when(ruleViolationsManager.isEmpty()).thenReturn(true);

        // WHEN
        Collection<RuleViolation> actual = serviceFacade.getRuleViolationStream(processGroup.getIdentifier()).collect(Collectors.toSet());

        // THEN
        assertEquals(expected, actual);
    }

    @Test
    public void testGetRuleViolationsForGroupIsRecursive() {
        // GIVEN
        int ruleViolationCounter = 0;

        String rootGroupId = "groupId";
        String childGroupId = "childGroupId";
        String grandChildGroupId = "grandChildGroupId";

        RuleViolation ruleViolation1 = createRuleViolation(rootGroupId, ruleViolationCounter++);
        RuleViolation ruleViolation2 = createRuleViolation(rootGroupId, ruleViolationCounter++);

        RuleViolation childRuleViolation1 = createRuleViolation(childGroupId, ruleViolationCounter++);
        RuleViolation childRuleViolation2 = createRuleViolation(childGroupId, ruleViolationCounter++);

        RuleViolation grandChildRuleViolation1 = createRuleViolation(grandChildGroupId, ruleViolationCounter++);
        RuleViolation grandChildRuleViolation2 = createRuleViolation(grandChildGroupId, ruleViolationCounter++);
        RuleViolation grandChildRuleViolation3 = createRuleViolation(grandChildGroupId, ruleViolationCounter);

        ProcessGroup grandChildProcessGroup = mockProcessGroup(
                grandChildGroupId,
                Collections.emptyList(),
                Arrays.asList(grandChildRuleViolation1, grandChildRuleViolation2, grandChildRuleViolation3)
        );
        ProcessGroup childProcessGroup = mockProcessGroup(
                childGroupId,
                Arrays.asList(grandChildProcessGroup),
                Arrays.asList(childRuleViolation1, childRuleViolation2)
        );
        mockProcessGroup(
                rootGroupId,
                Arrays.asList(childProcessGroup),
                Arrays.asList(ruleViolation1, ruleViolation2)
        );

        when(ruleViolationsManager.getRuleViolationsForGroups(Set.of(childGroupId, grandChildGroupId))).thenReturn(
                Arrays.asList(
                        childRuleViolation1, childRuleViolation2,
                        grandChildRuleViolation1, grandChildRuleViolation2, grandChildRuleViolation3)
        );

        Collection<RuleViolation> expected = new HashSet<>(Arrays.asList(
                childRuleViolation1, childRuleViolation2,
                grandChildRuleViolation1, grandChildRuleViolation2, grandChildRuleViolation3
        ));

        // WHEN
        Collection<RuleViolation> actual = serviceFacade.getRuleViolationStream(childProcessGroup.getIdentifier()).collect(Collectors.toSet());

        // THEN
        assertEquals(expected, actual);
    }

    private RuleViolation createRuleViolation(String groupId, int ruleViolationCounter) {
        return new RuleViolation(
                EnforcementPolicy.WARN,
                "scope" + ruleViolationCounter,
                "subjectId" + ruleViolationCounter,
                "subjectDisplayName" + ruleViolationCounter,
                null,
                groupId,
                "ruleId" + ruleViolationCounter,
                "issueId" + ruleViolationCounter,
                "violationMessage" + ruleViolationCounter,
                "violationExplanation" + ruleViolationCounter
        );
    }

    private ProcessGroup mockProcessGroup(String groupId, Collection<ProcessGroup> children, Collection<RuleViolation> violations) {
        ProcessGroup processGroup = mock(ProcessGroup.class, groupId);

        when(processGroup.getIdentifier()).thenReturn(groupId);
        when(processGroup.getProcessGroups()).thenReturn(new HashSet<>(children));

        when(processGroupDAO.getProcessGroup(groupId)).thenReturn(processGroup);

        when(ruleViolationsManager.getRuleViolationsForGroup(groupId)).thenReturn(violations);

        return processGroup;
    }

    @Test
    public void testGenerateIdsForImportingReportingTaskSnapshot() {
        final String originalServiceId = "s1";
        final VersionedControllerService service = new VersionedControllerService();
        service.setIdentifier(originalServiceId);

        final VersionedPropertyDescriptor serviceDescriptor = new VersionedPropertyDescriptor();
        serviceDescriptor.setName("My Service");
        serviceDescriptor.setIdentifiesControllerService(true);

        final Map<String, VersionedPropertyDescriptor> reportingTaskDescriptors = new HashMap<>();
        reportingTaskDescriptors.put(serviceDescriptor.getName(), serviceDescriptor);

        final Map<String, String> reportingTaskPropertyValues = new HashMap<>();
        reportingTaskPropertyValues.put(serviceDescriptor.getName(), service.getIdentifier());

        final String originalReportingTaskId = "r1";
        final VersionedReportingTask reportingTask = new VersionedReportingTask();
        reportingTask.setIdentifier(originalReportingTaskId);
        reportingTask.setPropertyDescriptors(reportingTaskDescriptors);
        reportingTask.setProperties(reportingTaskPropertyValues);

        final VersionedReportingTaskSnapshot reportingTaskSnapshot = new VersionedReportingTaskSnapshot();
        reportingTaskSnapshot.setReportingTasks(Collections.singletonList(reportingTask));
        reportingTaskSnapshot.setControllerServices(Collections.singletonList(service));

        serviceFacade.generateIdentifiersForImport(reportingTaskSnapshot, () -> UUID.randomUUID().toString());

        assertNotNull(service.getIdentifier());
        assertNotNull(service.getInstanceIdentifier());
        assertNotEquals(originalServiceId, service.getIdentifier());

        assertNotNull(reportingTask.getIdentifier());
        assertNotNull(reportingTask.getInstanceIdentifier());
        assertNotEquals(originalReportingTaskId, reportingTask.getIdentifier());

        assertEquals(service.getInstanceIdentifier(), reportingTask.getProperties().get(serviceDescriptor.getName()));
    }

    @Test
    public void testUpdateAllCounters() {
        // Mock ControllerFacade to return a list of reset counters
        final ControllerFacade controllerFacade = mock(ControllerFacade.class);
        final List<Counter> mockCounters = new ArrayList<>();

        // Create mock counters that would be returned after reset
        final Counter counter1 = mock(Counter.class);
        when(counter1.getIdentifier()).thenReturn("counter1-id");
        when(counter1.getName()).thenReturn("counter1");
        when(counter1.getContext()).thenReturn("context1");
        when(counter1.getValue()).thenReturn(0L); // Should be 0 after reset

        final Counter counter2 = mock(Counter.class);
        when(counter2.getIdentifier()).thenReturn("counter2-id");
        when(counter2.getName()).thenReturn("counter2");
        when(counter2.getContext()).thenReturn("context2");
        when(counter2.getValue()).thenReturn(0L); // Should be 0 after reset

        mockCounters.add(counter1);
        mockCounters.add(counter2);

        when(controllerFacade.resetAllCounters()).thenReturn(mockCounters);

        // Mock DtoFactory to create CounterDTOs
        final DtoFactory dtoFactory = mock(DtoFactory.class);
        final CounterDTO counterDto1 = new CounterDTO();
        counterDto1.setId("counter1-id");
        counterDto1.setName("counter1");
        counterDto1.setContext("context1");
        counterDto1.setValue("0");

        final CounterDTO counterDto2 = new CounterDTO();
        counterDto2.setId("counter2-id");
        counterDto2.setName("counter2");
        counterDto2.setContext("context2");
        counterDto2.setValue("0");

        when(dtoFactory.createCounterDto(counter1)).thenReturn(counterDto1);
        when(dtoFactory.createCounterDto(counter2)).thenReturn(counterDto2);

        final CountersSnapshotDTO snapshotDto = new CountersSnapshotDTO();
        snapshotDto.setCounters(Set.of(counterDto1, counterDto2));
        when(dtoFactory.createCountersDto(any())).thenReturn(snapshotDto);

        // Set up the facade
        serviceFacade.setControllerFacade(controllerFacade);
        serviceFacade.setDtoFactory(dtoFactory);

        // Test the updateAllCounters method
        final CountersDTO result = serviceFacade.updateAllCounters();

        // Verify the result
        assertNotNull(result);
        assertNotNull(result.getAggregateSnapshot());
        assertEquals(2, result.getAggregateSnapshot().getCounters().size());

        // Verify that resetAllCounters was called on the controller facade
        verify(controllerFacade, times(1)).resetAllCounters();

        // Verify that DTOs were created for each counter
        verify(dtoFactory, times(1)).createCounterDto(counter1);
        verify(dtoFactory, times(1)).createCounterDto(counter2);
        verify(dtoFactory, times(1)).createCountersDto(any());
    }

    @Test
    public void testUpdateAllCountersWithEmptyCounters() {
        // Mock ControllerFacade to return empty list
        final ControllerFacade controllerFacade = mock(ControllerFacade.class);
        when(controllerFacade.resetAllCounters()).thenReturn(new ArrayList<>());

        // Mock DtoFactory
        final DtoFactory dtoFactory = mock(DtoFactory.class);
        final CountersSnapshotDTO snapshotDto = new CountersSnapshotDTO();
        snapshotDto.setCounters(Collections.emptySet());
        when(dtoFactory.createCountersDto(any())).thenReturn(snapshotDto);

        // Set up the facade
        serviceFacade.setControllerFacade(controllerFacade);
        serviceFacade.setDtoFactory(dtoFactory);

        // Test the updateAllCounters method
        final CountersDTO result = serviceFacade.updateAllCounters();

        // Verify the result
        assertNotNull(result);
        assertNotNull(result.getAggregateSnapshot());
        assertTrue(result.getAggregateSnapshot().getCounters().isEmpty());

        // Verify that resetAllCounters was called
        verify(controllerFacade, times(1)).resetAllCounters();
        verify(dtoFactory, times(1)).createCountersDto(any());
    }

    @Test
    public void testClearBulletinsForComponent() {
        final String componentId = "test-component-123";
        final Instant fromTimestamp = Instant.now();

        MockTestBulletinRepository bulletinRepository = new MockTestBulletinRepository();

        // Add some test bulletins with different timestamps
        Instant beforeTime = fromTimestamp.minusSeconds(10);
        Instant afterTime = fromTimestamp.plusSeconds(10);

        bulletinRepository.addTestBulletin(componentId, "Before message", beforeTime);
        bulletinRepository.addTestBulletin(componentId, "At time message", fromTimestamp);
        bulletinRepository.addTestBulletin(componentId, "After message", afterTime);
        bulletinRepository.addTestBulletin("other-component", "Other component message", afterTime);

        serviceFacade.setBulletinRepository(bulletinRepository);

        ClearBulletinsResultEntity result = serviceFacade.clearBulletinsForComponent(componentId, fromTimestamp);

        assertNotNull(result);
        assertEquals(componentId, result.getComponentId());
        assertEquals(2, result.getBulletinsCleared()); // Should clear bulletins older than or equal to fromTimestamp
        assertEquals(2, bulletinRepository.bulletinList.size()); // 2 bulletins should remain (after time + other component)
    }

    @Test
    public void testClearBulletinsForComponents() {
        final String processGroupId = "test-process-group-123";
        final Instant fromTimestamp = Instant.now();
        final Set<String> componentIds = Set.of("component-1", "component-2");

        MockTestBulletinRepository bulletinRepository = new MockTestBulletinRepository();

        // Add test bulletins for multiple components
        Instant beforeTime = fromTimestamp.minusSeconds(10);
        Instant afterTime = fromTimestamp.plusSeconds(10);

        bulletinRepository.addTestBulletin("component-1", "Component 1 before", beforeTime);
        bulletinRepository.addTestBulletin("component-1", "Component 1 after", afterTime);
        bulletinRepository.addTestBulletin("component-2", "Component 2 after", afterTime);
        bulletinRepository.addTestBulletin("component-3", "Component 3 after", afterTime);

        serviceFacade.setBulletinRepository(bulletinRepository);

        ClearBulletinsForGroupResultsEntity result = serviceFacade.clearBulletinsForComponents(
                processGroupId, fromTimestamp, componentIds);

        assertNotNull(result);
        assertEquals(1, result.getBulletinsCleared()); // Should clear 1 bulletin (component-1 before only)
        assertEquals(3, bulletinRepository.bulletinList.size()); // 3 bulletins should remain (all "after" bulletins)
    }

    @Test
    public void testClearBulletinsForComponentWithZeroResult() {
        final String componentId = "non-existent-component";
        final Instant fromTimestamp = Instant.now();

        MockTestBulletinRepository bulletinRepository = new MockTestBulletinRepository();

        // Add a bulletin for a different component
        bulletinRepository.addTestBulletin("other-component", "Other message", Instant.now());

        serviceFacade.setBulletinRepository(bulletinRepository);

        ClearBulletinsResultEntity result = serviceFacade.clearBulletinsForComponent(componentId, fromTimestamp);

        assertNotNull(result);
        assertEquals(componentId, result.getComponentId());
        assertEquals(0, result.getBulletinsCleared());
        assertEquals(1, bulletinRepository.bulletinList.size()); // Original bulletin should remain
    }

    @Test
    public void testClearBulletinsForComponentsWithEmptySet() {
        final String processGroupId = "test-process-group-123";
        final Instant fromTimestamp = Instant.now();
        final Set<String> emptyComponentIds = Collections.emptySet();

        MockTestBulletinRepository bulletinRepository = new MockTestBulletinRepository();
        serviceFacade.setBulletinRepository(bulletinRepository);

        ClearBulletinsForGroupResultsEntity result = serviceFacade.clearBulletinsForComponents(
                processGroupId, fromTimestamp, emptyComponentIds);

        assertNotNull(result);
        assertEquals(0, result.getBulletinsCleared());
    }
}
