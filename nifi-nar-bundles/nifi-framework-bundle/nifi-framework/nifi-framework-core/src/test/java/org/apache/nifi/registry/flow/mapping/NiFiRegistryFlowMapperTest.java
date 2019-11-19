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

package org.apache.nifi.registry.flow.mapping;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.nifi.authorization.resource.ComponentAuthorizable;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Funnel;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.connectable.Position;
import org.apache.nifi.connectable.Positionable;
import org.apache.nifi.connectable.Size;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.PropertyConfiguration;
import org.apache.nifi.controller.label.Label;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.queue.LoadBalanceCompression;
import org.apache.nifi.controller.queue.LoadBalanceStrategy;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterDescriptor;
import org.apache.nifi.registry.ComponentVariableRegistry;
import org.apache.nifi.registry.VariableDescriptor;
import org.apache.nifi.registry.flow.ComponentType;
import org.apache.nifi.registry.flow.ExternalControllerServiceReference;
import org.apache.nifi.registry.flow.FlowRegistry;
import org.apache.nifi.registry.flow.FlowRegistryClient;
import org.apache.nifi.registry.flow.PortType;
import org.apache.nifi.registry.flow.VersionControlInformation;
import org.apache.nifi.registry.flow.VersionedConnection;
import org.apache.nifi.registry.flow.VersionedControllerService;
import org.apache.nifi.registry.flow.VersionedFlowCoordinates;
import org.apache.nifi.registry.flow.VersionedFunnel;
import org.apache.nifi.registry.flow.VersionedLabel;
import org.apache.nifi.registry.flow.VersionedParameter;
import org.apache.nifi.registry.flow.VersionedParameterContext;
import org.apache.nifi.registry.flow.VersionedPort;
import org.apache.nifi.registry.flow.VersionedProcessGroup;
import org.apache.nifi.registry.flow.VersionedProcessor;
import org.apache.nifi.registry.flow.VersionedPropertyDescriptor;
import org.apache.nifi.registry.flow.VersionedRemoteGroupPort;
import org.apache.nifi.registry.flow.VersionedRemoteProcessGroup;
import org.apache.nifi.remote.RemoteGroupPort;
import org.apache.nifi.remote.protocol.SiteToSiteTransportProtocol;
import org.apache.nifi.scheduling.ExecutionNode;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class NiFiRegistryFlowMapperTest {

    @Mock
    private ExtensionManager extensionManager;
    @Mock
    private ControllerServiceProvider controllerServiceProvider;
    @Mock
    private FlowRegistryClient flowRegistryClient;

    private NiFiRegistryFlowMapper flowMapper = new NiFiRegistryFlowMapper(extensionManager);

    private int counter = 1;

    @Before
    public void setup() {
        final FlowRegistry flowRegistry = mock(FlowRegistry.class);
        when(flowRegistryClient.getFlowRegistry(anyString())).thenReturn(flowRegistry);
        when(flowRegistry.getURL()).thenReturn("url");
    }

    /**
     * Test mapping versioned process group's parameter contexts excluding descendant versioned process groups
     */
    @Test
    public void testMapParameterContextsExcludingVersionedDescendants() {
        final ProcessGroup innerInnerProcessGroup =
                prepareProcessGroupWithParameterContext(Collections.emptyList(),
                        true, true);
        final ProcessGroup innerProcessGroup =
                prepareProcessGroupWithParameterContext(Lists.newArrayList(innerInnerProcessGroup),
                        true, false);
        final ProcessGroup processGroup =
                prepareProcessGroupWithParameterContext(Lists.newArrayList(innerProcessGroup),
                        false, false);

        // first nesting should be traversed because child is not version controlled, but deeper nesting should be ignored
        // because map versioned descendants indicator is false
        final List<VersionedParameterContext> versionedParameterContexts =
                Lists.newArrayList(flowMapper.mapParameterContexts(processGroup, false));

        // verify single parameter context
        assertEquals(1, versionedParameterContexts.size());
        verifyParameterContext(versionedParameterContexts.get(0), innerProcessGroup.getParameterContext());
    }

    /**
     * Test mapping nested process group's parameter contexts
     */
    @Test
    public void testMapNestedParameterContexts() {
        final ProcessGroup innerInnerProcessGroup =
                prepareProcessGroupWithParameterContext(Collections.emptyList(),
                        true, true);
        final ProcessGroup innerProcessGroup =
                prepareProcessGroupWithParameterContext(Lists.newArrayList(innerInnerProcessGroup),
                        false, true);
        final ProcessGroup processGroup =
                prepareProcessGroupWithParameterContext(Lists.newArrayList(innerProcessGroup),
                        true, true);

        // include nested parameter contexts even though they are version controlled because map descendant indicator is true
        final List<VersionedParameterContext> versionedParameterContexts =
                Lists.newArrayList(flowMapper.mapParameterContexts(processGroup, true));

        // verify parameter contexts (collection order should be deterministic, copied to list for ease of testing)
        assertEquals(2, versionedParameterContexts.size());
        verifyParameterContext(versionedParameterContexts.get(0), processGroup.getParameterContext());
        verifyParameterContext(versionedParameterContexts.get(1), innerInnerProcessGroup.getParameterContext());
    }

    /**
     * Test mapping a versioned ProcessGroup model to a versioned VersionedProcessGroup excluding descendant versioned flows.
     * VersionControlInformation should be mapped to the versioned inner process group instead of the group contents
     */
    @Test
    public void testMapVersionedProcessGroupsExcludingVersionedDescendants() {
        // prepare a versioned process group with a nested versioned process group, each with 1 processor
        final ProcessGroup innerProcessGroup =
                prepareProcessGroup(1,false, false, false,
                        false, false,null,
                        false, true, Collections.emptyList());
        final ProcessGroup processGroup =
                prepareProcessGroup(1,false,false, false,
                        false, false, null,
                        false, true, Lists.newArrayList(innerProcessGroup));

        final List<ProcessGroup> allProcessGroups = Lists.newArrayList(innerProcessGroup);
        when(processGroup.findAllProcessGroups()).thenReturn(allProcessGroups);

        // perform the mapping, excluding descendant versioned flows
        final InstantiatedVersionedProcessGroup versionedProcessGroup =
                flowMapper.mapProcessGroup(processGroup, controllerServiceProvider, flowRegistryClient,
                        false);
        final VersionedProcessGroup innerVersionedProcessGroup =
                versionedProcessGroup.getProcessGroups().iterator().next();

        // verify root versioned process group contents only
        verifyVersionedProcessGroup(processGroup, versionedProcessGroup,false,false);

        // verify versioned descendant is present with VersionControlInfo only
        verifyVersionedProcessGroup(innerProcessGroup, innerVersionedProcessGroup,true,false);
    }

    /**
     * Test mapping a versioned ProcessGroup model to a non-versioned VersionedProcessGroup. Version info is ignored.
     * Most elements are exercised here... including labels, ports, processors, connections, funnels, nested process groups,
     * remote process groups, congtroller services, external controller service references and variable registries.
     */
    @Test
    public void testMapNonVersionedProcessGroups() {
        // create a controller service with a different process group id so it's treated as external
        final ControllerServiceNode externalControllerServiceNode = prepareControllerService(UUID.randomUUID().toString());

        // prepare a process group with nested process groups
        final ProcessGroup innerInnerProcessGroup =
                prepareProcessGroup(0,false, true, false,
                        true, false,null,
                        true, false, Collections.emptyList());
        final ProcessGroup innerProcessGroup =
                prepareProcessGroup(1,true, false, false,
                        true, true, externalControllerServiceNode,
                        true, true, Lists.newArrayList(innerInnerProcessGroup));
        final ProcessGroup processGroup =
                prepareProcessGroup(2,false,false, true,
                        false, true, null,
                        false, true, Lists.newArrayList(innerProcessGroup));

        final List<ProcessGroup> allProcessGroups = Lists.newArrayList(innerProcessGroup, innerInnerProcessGroup);
        when(processGroup.findAllProcessGroups()).thenReturn(allProcessGroups);

        // perform the mapping
        final InstantiatedVersionedProcessGroup versionedProcessGroup =
                flowMapper.mapNonVersionedProcessGroup(processGroup, controllerServiceProvider);

        // recursively verify versioned process group contents
        verifyVersionedProcessGroup(processGroup, versionedProcessGroup, false,true);

        // verify external controller service reference
        final Map<String, ExternalControllerServiceReference> externalControllerServiceReferences =
                versionedProcessGroup.getExternalControllerServiceReferences();
        final String expectedExternalControllerServiceReferenceKey = flowMapper.getGroupId(externalControllerServiceNode.getIdentifier());
        final ExternalControllerServiceReference externalControllerServiceReference =
                externalControllerServiceReferences.get(expectedExternalControllerServiceReferenceKey);
        assertNotNull(externalControllerServiceReference);
        assertEquals(externalControllerServiceReference.getIdentifier(), expectedExternalControllerServiceReferenceKey);
        assertEquals(externalControllerServiceReference.getName(), externalControllerServiceNode.getName());
    }

    private ProcessGroup prepareProcessGroupWithParameterContext(final List<ProcessGroup> childProcessGroups,
                                                                 final boolean includeParameterContext,
                                                                 final boolean isVersionControlled) {
        final ProcessGroup processGroup = mock(ProcessGroup.class);

        if (includeParameterContext) {
            final ParameterContext parameterContext = mock(ParameterContext.class);
            when(processGroup.getParameterContext()).thenReturn(parameterContext);
            when(parameterContext.getName()).thenReturn("context" + (counter++));
            final Map<ParameterDescriptor, Parameter> parametersMap = Maps.newHashMap();
            when(parameterContext.getParameters()).thenReturn(parametersMap);

            addParameter(parametersMap, "value" + (counter++), false);
            addParameter(parametersMap, "value" + (counter++), true);
            addParameter(parametersMap, null, true);
        }

        if (isVersionControlled) {
            when(processGroup.getVersionControlInformation()).thenReturn(mock(VersionControlInformation.class));
        }

        when(processGroup.getProcessGroups()).thenReturn(Sets.newLinkedHashSet(childProcessGroups));

        return processGroup;
    }

    private void addParameter(final Map<ParameterDescriptor, Parameter> parametersMap, final String value, final boolean isSensitive) {

        final ParameterDescriptor parameterDescriptor =
                new ParameterDescriptor.Builder().name("param" + (counter++)).description("description" + (counter++)).sensitive(isSensitive).build();
        final Parameter parameter = mock(Parameter.class);
        when(parameter.getDescriptor()).thenReturn(parameterDescriptor);
        when(parameter.getValue()).thenReturn(value);
        parametersMap.put(parameterDescriptor, parameter);
    }

    private void verifyParameterContext(final VersionedParameterContext versionedParameterContext, final ParameterContext parameterContext) {
        assertEquals(versionedParameterContext.getName(), parameterContext.getName());

        final Collection<Parameter> parameters = parameterContext.getParameters().values();
        final Set<VersionedParameter> versionedParameters = versionedParameterContext.getParameters();
        // parameter order is not deterministic - use unique names to map up matching parameters
        final Iterator<Parameter> parametersIterator = parameters.iterator();
        while (parametersIterator.hasNext()) {
            final Parameter parameter = parametersIterator.next();
            final Iterator<VersionedParameter> versionedParameterIterator = versionedParameters.iterator();
            while (versionedParameterIterator.hasNext()) {
                final VersionedParameter versionedParameter = versionedParameterIterator.next();
                if (versionedParameter.getName().equals(parameter.getDescriptor().getName())) {
                    verifyParameter(versionedParameter, parameter);
                    versionedParameterIterator.remove();
                    break;
                }
            }
        }
        assertTrue("Failed to match parameters by unique name", versionedParameters.isEmpty());

    }

    private void verifyParameter(final VersionedParameter versionedParameter, final Parameter parameter) {
        final ParameterDescriptor parameterDescriptor = parameter.getDescriptor();

        assertEquals(versionedParameter.getName(), parameterDescriptor.getName());
        assertEquals(versionedParameter.getDescription(), parameterDescriptor.getDescription());
        assertEquals(versionedParameter.isSensitive(), parameterDescriptor.isSensitive());
        if (parameterDescriptor.isSensitive()) {
            // verify parameter value is null for sensitive parameters
            assertNull(versionedParameter.getValue());
        } else {
            assertEquals(versionedParameter.getValue(), parameter.getValue());
        }
    }

    private ProcessGroup prepareProcessGroup(final int numProcessors, final boolean includeFunnel,final boolean includePorts,
                                             final boolean includeLabels, final boolean includeVariableRegistry,
                                             final boolean includeControllerService,
                                             final ControllerServiceNode externalControllerServiceNode,
                                             final boolean includeRemoteProcessGroup, final boolean includeVersionControlInfo,
                                             final List<ProcessGroup> childProcessGroups) {
        final String processGroupId = UUID.randomUUID().toString();
        final ProcessGroup processGroup = mock(ProcessGroup.class);
        when(processGroup.getIdentifier()).thenReturn(processGroupId);
        when(processGroup.getProcessGroupIdentifier()).thenReturn(processGroupId);
        when(processGroup.getName()).thenReturn("group"+(counter++));
        when(processGroup.getComments()).thenReturn("comments"+(counter++));
        when(processGroup.getPosition()).thenReturn(new Position(counter++, counter++));
        final ParameterContext parameterContext = mock(ParameterContext.class);
        when(processGroup.getParameterContext()).thenReturn(parameterContext);
        when(parameterContext.getName()).thenReturn("context"+(counter++));

        // prep funnels
        final Set<Funnel> funnels = Sets.newHashSet();
        if (includeFunnel) {
            funnels.add(prepareFunnel(processGroupId));
        }
        when(processGroup.getFunnels()).thenReturn(funnels);

        // prep ports
        final Set<Port> inputPorts = Sets.newHashSet();
        final Set<Port> outputPorts = Sets.newHashSet();
        if (includePorts) {
            inputPorts.add(preparePort(processGroupId, PortType.INPUT_PORT));
            outputPorts.add(preparePort(processGroupId, PortType.OUTPUT_PORT));
        }
        when(processGroup.getInputPorts()).thenReturn(inputPorts);
        when(processGroup.getOutputPorts()).thenReturn(outputPorts);

        // prep labels
        final Set<Label> labels = Sets.newHashSet();
        if (includeLabels) {
            labels.add(prepareLabel(processGroupId));
        }
        when(processGroup.getLabels()).thenReturn(labels);

        // prep connections and processors
        final Set<ProcessorNode> processorNodes = Sets.newLinkedHashSet();
        final Set<Connection> connections = Sets.newHashSet();
        if (numProcessors == 2) {
            // 2 processors connected together
            final ProcessorNode processorNode1 = prepareProcessor(processGroup, externalControllerServiceNode);
            final ProcessorNode processorNode2 = prepareProcessor(processGroup, externalControllerServiceNode);
            processorNodes.add(processorNode1);
            processorNodes.add(processorNode2);
            connections.add(prepareConnection(processorNode1, processorNode2, processGroup));
        } else if (numProcessors == 1) {
            // a processor connected to itself
            final ProcessorNode processorNode1 = prepareProcessor(processGroup, externalControllerServiceNode);
            processorNodes.add(processorNode1);
            connections.add(prepareConnection(processorNode1, processorNode1, processGroup));
        }
        when(processGroup.getProcessors()).thenReturn(processorNodes);
        when(processGroup.getConnections()).thenReturn(connections);

        // prep controller services
        final Set<ControllerServiceNode> controllerServiceNodes = Sets.newHashSet();
        if (includeControllerService) {
            controllerServiceNodes.add(prepareControllerService(processGroupId));
        }
        when(processGroup.getControllerServices(false)).thenReturn(controllerServiceNodes);

        // prep variable registry
        final ComponentVariableRegistry componentVariableRegistry = mock(ComponentVariableRegistry.class);
        when(processGroup.getVariableRegistry()).thenReturn(componentVariableRegistry);
        final Map<VariableDescriptor, String> registryVariableMap = Maps.newHashMap();
        if (includeVariableRegistry) {
            registryVariableMap.putAll(prepareVariableRegistry());
        }
        when(componentVariableRegistry.getVariableMap()).thenReturn(registryVariableMap);

        // prepare remote process group
        final Set<RemoteProcessGroup> remoteProcessGroups = Sets.newHashSet();
        if (includeRemoteProcessGroup) {
            remoteProcessGroups.add(prepareRemoteProcessGroup(processGroupId));
        }
        when(processGroup.getRemoteProcessGroups()).thenReturn(remoteProcessGroups);

        // prep version control info
        if (includeVersionControlInfo) {
            final VersionControlInformation versionControlInformation = prepareVersionControlInfo();
            when(processGroup.getVersionControlInformation()).thenReturn(versionControlInformation);
        }

        // prep nested process groups
        when(processGroup.getProcessGroups()).thenReturn(Sets.newLinkedHashSet(childProcessGroups));

        return processGroup;
    }

    private Funnel prepareFunnel(final String processGroupId) {
        final Funnel funnel = mock(Funnel.class);
        prepareComponentAuthorizable(funnel, processGroupId);
        preparePositionable(funnel);
        return funnel;
    }

    private Port preparePort(final String processGroupId, final PortType portType) {
        final Port port = mock(Port.class);
        prepareComponentAuthorizable(port, processGroupId);
        preparePositionable(port);
        prepareConnectable(port, ConnectableType.valueOf(portType.name()));
        return port;
    }

    private Label prepareLabel(final String processGroupId) {
        final Label label = mock(Label.class);
        prepareComponentAuthorizable(label, processGroupId);
        preparePositionable(label);
        when(label.getSize()).thenReturn(new Size(counter++, counter++));
        return label;
    }

    private ProcessorNode prepareProcessor(final ProcessGroup processGroup, final ControllerServiceNode externalControllerServiceNode) {
        final ProcessorNode processorNode = mock(ProcessorNode.class);
        prepareComponentAuthorizable(processorNode, processGroup.getIdentifier());
        preparePositionable(processorNode);
        prepareConnectable(processorNode, ConnectableType.PROCESSOR);
        when(processorNode.getProcessGroup()).thenReturn(processGroup);
        when(processorNode.getAutoTerminatedRelationships()).thenReturn(Collections.emptySet());
        when(processorNode.getBulletinLevel()).thenReturn(LogLevel.INFO);
        when(processorNode.getExecutionNode()).thenReturn(ExecutionNode.ALL);
        when(processorNode.getSchedulingStrategy()).thenReturn(SchedulingStrategy.TIMER_DRIVEN);
        when(processorNode.getBundleCoordinate()).thenReturn(mock(BundleCoordinate.class));

        final String rawPropertyValue = "propValue";
        final PropertyDescriptor.Builder propertyDescriptorBuilder =
                new PropertyDescriptor.Builder().name("propName").sensitive(false).displayName("displayName");
        if (externalControllerServiceNode != null) {
            propertyDescriptorBuilder.identifiesControllerService(ControllerService.class);
            when(controllerServiceProvider.getControllerServiceNode(rawPropertyValue)).thenReturn(externalControllerServiceNode);
        }
        final PropertyDescriptor propertyDescriptor = propertyDescriptorBuilder.build();
        final PropertyConfiguration propertyConfiguration = mock(PropertyConfiguration.class);
        final Map<PropertyDescriptor, PropertyConfiguration> properties = Maps.newHashMap();
        properties.put(propertyDescriptor, propertyConfiguration);
        when(processorNode.getProperties()).thenReturn(properties);
        when(processorNode.getProperty(propertyDescriptor)).thenReturn(propertyConfiguration);
        when(propertyConfiguration.getRawValue()).thenReturn(rawPropertyValue);

        return processorNode;
    }

    private Connection prepareConnection(final ProcessorNode sourceProcessorNode, final ProcessorNode destProcessorNode,
                                         final ProcessGroup processGroup) {
        final Connection connection = mock(Connection.class);
        when(connection.getIdentifier()).thenReturn(UUID.randomUUID().toString());
        when(connection.getProcessGroup()).thenReturn(processGroup);
        when(connection.getBendPoints()).thenReturn(Lists.newArrayList(new Position(counter++, counter++)));
        when(connection.getRelationships()).thenReturn(Lists.newArrayList());
        final FlowFileQueue flowFileQueue = mock(FlowFileQueue.class);
        when(connection.getFlowFileQueue()).thenReturn(flowFileQueue);
        when(flowFileQueue.getPriorities()).thenReturn(Collections.emptyList());
        when(flowFileQueue.getLoadBalanceStrategy()).thenReturn(LoadBalanceStrategy.DO_NOT_LOAD_BALANCE);
        when(flowFileQueue.getLoadBalanceCompression()).thenReturn(LoadBalanceCompression.DO_NOT_COMPRESS);
        when(sourceProcessorNode.getConnectableType()).thenReturn(ConnectableType.PROCESSOR);
        when(connection.getSource()).thenReturn(sourceProcessorNode);
        when(destProcessorNode.getConnectableType()).thenReturn(ConnectableType.PROCESSOR);
        when(connection.getDestination()).thenReturn(destProcessorNode);
        return connection;
    }

    private Map<VariableDescriptor, String> prepareVariableRegistry() {
        final VariableDescriptor variableDescriptor =
                new VariableDescriptor.Builder("variable"+(counter++)).build();
        final Map<VariableDescriptor, String> variableRegistryMap = Maps.newHashMap();
        variableRegistryMap.put(variableDescriptor, "value"+(counter++));
        return variableRegistryMap;
    }

    private ControllerServiceNode prepareControllerService(final String processGroupId) {
        final ControllerServiceNode controllerServiceNode = mock(ControllerServiceNode.class);
        prepareComponentAuthorizable(controllerServiceNode, processGroupId);
        when(controllerServiceNode.getName()).thenReturn("service" + (counter++));
        when(controllerServiceNode.getBundleCoordinate()).thenReturn(mock(BundleCoordinate.class));
        when(controllerServiceNode.getControllerServiceImplementation()).thenReturn(mock(ControllerService.class));
        when(controllerServiceNode.getProperties()).thenReturn(Collections.emptyMap());
        return controllerServiceNode;
    }

    private RemoteProcessGroup prepareRemoteProcessGroup(final String processGroupId) {
        final RemoteProcessGroup remoteProcessGroup = mock(RemoteProcessGroup.class);
        prepareComponentAuthorizable(remoteProcessGroup, processGroupId);
        preparePositionable(remoteProcessGroup);
        when(remoteProcessGroup.getName()).thenReturn("remote" + (counter++));
        when(remoteProcessGroup.getTransportProtocol()).thenReturn(SiteToSiteTransportProtocol.HTTP);
        final RemoteGroupPort remoteGroupInputPort = prepareRemoteGroupPort(remoteProcessGroup);
        when(remoteProcessGroup.getInputPorts()).thenReturn(Sets.newHashSet(remoteGroupInputPort));
        final RemoteGroupPort remoteGroupOutputPort = prepareRemoteGroupPort(remoteProcessGroup);
        when(remoteProcessGroup.getOutputPorts()).thenReturn(Sets.newHashSet(remoteGroupOutputPort));
        return remoteProcessGroup;
    }

    private RemoteGroupPort prepareRemoteGroupPort(final RemoteProcessGroup remoteProcessGroup) {
        final RemoteGroupPort remoteGroupPort = mock(RemoteGroupPort.class);
        prepareComponentAuthorizable(remoteGroupPort, remoteProcessGroup.getIdentifier());
        when(remoteGroupPort.getName()).thenReturn("remotePort" + (counter++));
        when(remoteGroupPort.getRemoteProcessGroup()).thenReturn(remoteProcessGroup);
        return remoteGroupPort;
    }

    private VersionControlInformation prepareVersionControlInfo() {
        final VersionControlInformation versionControlInformation = mock(VersionControlInformation.class);
        when(versionControlInformation.getRegistryIdentifier()).thenReturn(UUID.randomUUID().toString());
        when(versionControlInformation.getBucketIdentifier()).thenReturn(UUID.randomUUID().toString());
        when(versionControlInformation.getFlowIdentifier()).thenReturn(UUID.randomUUID().toString());
        when(versionControlInformation.getVersion()).thenReturn(counter++);
        return versionControlInformation;
    }

    private void prepareComponentAuthorizable(final ComponentAuthorizable authorizable, final String processGroupId) {
        when(authorizable.getIdentifier()).thenReturn(UUID.randomUUID().toString());
        when(authorizable.getProcessGroupIdentifier()).thenReturn(processGroupId);
    }

    private void preparePositionable(final Positionable positionable) {
        when(positionable.getPosition()).thenReturn(new Position(counter++, counter++));
    }

    private void prepareConnectable(final Connectable connectable, final ConnectableType connectableType) {
        when(connectable.getName()).thenReturn("connectable" + (counter++));
        when(connectable.getConnectableType()).thenReturn(connectableType);
    }

    /**
     * Verify the given VersionedProcessGroup was correctly mapped from the given ProcessGroup, including child groups
     * when boolean indicator is true. If expectVersionControlInfo boolean indicator is true, the mapped group should
     * only contain VersionControlInfo, otherwise it should instead contain its full contents.
     */
    private void verifyVersionedProcessGroup(final ProcessGroup processGroup, final VersionedProcessGroup versionedProcessGroup,
                                             final boolean expectVersionControlInfo, final boolean verifyChildProcessGroups) {
        final String expectedGroupIdentifier = flowMapper.getGroupId(processGroup.getProcessGroupIdentifier());

        // verify process group fields
        assertEquals(versionedProcessGroup.getName(), processGroup.getName());
        assertEquals(versionedProcessGroup.getIdentifier(), flowMapper.getGroupId(processGroup.getIdentifier()));
        assertEquals(versionedProcessGroup.getGroupIdentifier(), expectedGroupIdentifier);
        assertEquals(versionedProcessGroup.getComments(), processGroup.getComments());
        assertEquals(versionedProcessGroup.getPosition().getX(), processGroup.getPosition().getX(), 0);
        assertEquals(versionedProcessGroup.getPosition().getY(), processGroup.getPosition().getY(), 0);

        final String expectedParameterContextName =
                (processGroup.getParameterContext() != null ? processGroup.getParameterContext().getName() : null);
        assertEquals(versionedProcessGroup.getParameterContextName(), expectedParameterContextName);

        // verify either version control info or full group contents
        if (expectVersionControlInfo) {
            final VersionControlInformation versionControlInfo = processGroup.getVersionControlInformation();
            final VersionedFlowCoordinates versionedFlowCoordinates = versionedProcessGroup.getVersionedFlowCoordinates();
            assertNotNull(versionedFlowCoordinates.getRegistryUrl());
            assertEquals(versionedFlowCoordinates.getBucketId(), versionControlInfo.getBucketIdentifier());
            assertEquals(versionedFlowCoordinates.getFlowId(), versionControlInfo.getFlowIdentifier());
            assertEquals(versionedFlowCoordinates.getVersion(), versionControlInfo.getVersion());
        } else {
            assertNull(versionedProcessGroup.getVersionedFlowCoordinates());

            // verify funnels
            final Set<Funnel> funnels = processGroup.getFunnels();
            final Set<VersionedFunnel> versionedFunnels = versionedProcessGroup.getFunnels();
            if (funnels.isEmpty()) {
                assertTrue(versionedFunnels.isEmpty());
            } else {
                final Funnel funnel = funnels.iterator().next();
                final VersionedFunnel versionedFunnel = versionedFunnels.iterator().next();

                assertEquals(versionedFunnel.getIdentifier(), flowMapper.getGroupId(funnel.getIdentifier()));
                assertEquals(versionedFunnel.getGroupIdentifier(), expectedGroupIdentifier);
                assertEquals(versionedFunnel.getPosition().getX(), funnel.getPosition().getX(), 0);
                assertEquals(versionedFunnel.getPosition().getY(), funnel.getPosition().getY(), 0);
            }

            // verify ports
            verifyPorts(processGroup.getInputPorts(), versionedProcessGroup.getInputPorts(), PortType.INPUT_PORT, expectedGroupIdentifier);
            verifyPorts(processGroup.getOutputPorts(), versionedProcessGroup.getOutputPorts(), PortType.OUTPUT_PORT, expectedGroupIdentifier);

            // verify labels
            final Set<Label> labels = processGroup.getLabels();
            final Set<VersionedLabel> versionedLabels = versionedProcessGroup.getLabels();
            if (labels.isEmpty()) {
                assertTrue(versionedLabels.isEmpty());
            } else {
                final Label label = labels.iterator().next();
                final VersionedLabel versionedLabel = versionedLabels.iterator().next();

                assertEquals(versionedLabel.getIdentifier(), flowMapper.getGroupId(label.getIdentifier()));
                assertEquals(versionedLabel.getGroupIdentifier(), expectedGroupIdentifier);
                assertEquals(versionedLabel.getPosition().getX(), label.getPosition().getX(), 0);
                assertEquals(versionedLabel.getPosition().getY(), label.getPosition().getY(), 0);
                assertEquals(versionedLabel.getHeight(), label.getSize().getHeight(), 0);
                assertEquals(versionedLabel.getWidth(), label.getSize().getWidth(), 0);
            }

            // verify processors
            final Collection<ProcessorNode> processorNodes = processGroup.getProcessors();
            final Set<VersionedProcessor> versionedProcessors = versionedProcessGroup.getProcessors();
            // first verify the number of processors matches
            assertEquals(versionedProcessors.size(), processorNodes.size());
            // processor order is not deterministic - use unique names to map up matching processors
            final Iterator<ProcessorNode> processorNodesIterator = processorNodes.iterator();
            while (processorNodesIterator.hasNext()) {
                final ProcessorNode processorNode = processorNodesIterator.next();
                final Iterator<VersionedProcessor> versionedProcessorIterator = versionedProcessors.iterator();
                while (versionedProcessorIterator.hasNext()) {
                    final VersionedProcessor versionedProcessor = versionedProcessorIterator.next();
                    if (versionedProcessor.getName().equals(processorNode.getName())) {
                        verifyProcessor(versionedProcessor, processorNode, expectedGroupIdentifier);
                        versionedProcessorIterator.remove();
                        break;
                    }
                }
            }
            assertTrue("Failed to match processors by unique name", versionedProcessors.isEmpty());

            // verify connections
            final Set<Connection> connections = processGroup.getConnections();
            final Set<VersionedConnection> versionedConnections = versionedProcessGroup.getConnections();
            if (connections.isEmpty()) {
                assertTrue(versionedConnections.isEmpty());
            } else {
                final Connection connection = connections.iterator().next();
                final VersionedConnection versionedConnection = versionedConnections.iterator().next();

                assertEquals(versionedConnection.getName(), connection.getName());
                assertEquals(versionedConnection.getIdentifier(), flowMapper.getGroupId(connection.getIdentifier()));
                assertEquals(versionedConnection.getGroupIdentifier(), expectedGroupIdentifier);
                assertEquals(versionedConnection.getBends().get(0).getX(), connection.getBendPoints().get(0).getX(), 0);
                assertEquals(versionedConnection.getBends().get(0).getY(), connection.getBendPoints().get(0).getY(), 0);
                assertEquals(versionedConnection.getSource().getId(), flowMapper.getGroupId(connection.getSource().getIdentifier()));
                assertEquals(versionedConnection.getSource().getGroupId(), expectedGroupIdentifier);
                assertEquals(versionedConnection.getSource().getName(), connection.getSource().getName());
                assertEquals(versionedConnection.getSource().getType().name(), connection.getSource().getConnectableType().name());
                assertEquals(versionedConnection.getDestination().getId(), flowMapper.getGroupId(connection.getDestination().getIdentifier()));
                assertEquals(versionedConnection.getDestination().getGroupId(), expectedGroupIdentifier);
                assertEquals(versionedConnection.getDestination().getName(), connection.getDestination().getName());
                assertEquals(versionedConnection.getDestination().getType().name(), connection.getDestination().getConnectableType().name());
            }

            // verify controller services
            final Set<ControllerServiceNode> controllerServiceNodes = processGroup.getControllerServices(false);
            final Set<VersionedControllerService> versionedControllerServices = versionedProcessGroup.getControllerServices();
            if (controllerServiceNodes.isEmpty()) {
                assertTrue(versionedControllerServices.isEmpty());
            } else {
                final ControllerServiceNode controllerServiceNode = controllerServiceNodes.iterator().next();
                final VersionedControllerService versionedControllerService = versionedControllerServices.iterator().next();

                assertEquals(versionedControllerService.getName(), controllerServiceNode.getName());
                assertEquals(versionedControllerService.getIdentifier(), flowMapper.getGroupId(controllerServiceNode.getIdentifier()));
                assertEquals(versionedControllerService.getGroupIdentifier(), expectedGroupIdentifier);
            }

            // verify variables
            final Map<VariableDescriptor, String> variableRegistryMap = processGroup.getVariableRegistry().getVariableMap();
            final Map<String, String> versionedVariableMap = versionedProcessGroup.getVariables();
            if (variableRegistryMap.isEmpty()) {
                assertTrue(versionedVariableMap.isEmpty());
            } else {
                final VariableDescriptor variableRegistryKey = variableRegistryMap.keySet().iterator().next();
                assertEquals(versionedVariableMap.get(variableRegistryKey.getName()), variableRegistryMap.get(variableRegistryKey));
            }

            // verify remote process group(s)
            final Set<RemoteProcessGroup> remoteProcessGroups = processGroup.getRemoteProcessGroups();
            final Set<VersionedRemoteProcessGroup> versionedRemoteProcessGroups = versionedProcessGroup.getRemoteProcessGroups();
            if (remoteProcessGroups.isEmpty()) {
                assertTrue(versionedRemoteProcessGroups.isEmpty());
            } else {
                final RemoteProcessGroup remoteProcessGroup = remoteProcessGroups.iterator().next();
                final VersionedRemoteProcessGroup versionedRemoteProcessGroup = versionedRemoteProcessGroups.iterator().next();

                assertEquals(versionedRemoteProcessGroup.getName(), remoteProcessGroup.getName());
                assertEquals(versionedRemoteProcessGroup.getIdentifier(), flowMapper.getGroupId(remoteProcessGroup.getIdentifier()));
                assertEquals(versionedRemoteProcessGroup.getGroupIdentifier(), expectedGroupIdentifier);
                assertEquals(versionedRemoteProcessGroup.getPosition().getX(), remoteProcessGroup.getPosition().getX(), 0);
                assertEquals(versionedRemoteProcessGroup.getPosition().getY(), remoteProcessGroup.getPosition().getY(), 0);

                // verify remote ports
                final String expectedPortGroupIdentifier = flowMapper.getGroupId(remoteProcessGroup.getIdentifier());
                verifyRemotePorts(remoteProcessGroup.getInputPorts(), versionedRemoteProcessGroup.getInputPorts(),
                        ComponentType.REMOTE_INPUT_PORT, expectedPortGroupIdentifier);
                verifyRemotePorts(remoteProcessGroup.getOutputPorts(), versionedRemoteProcessGroup.getOutputPorts(),
                        ComponentType.REMOTE_OUTPUT_PORT, expectedPortGroupIdentifier);
            }

            if (verifyChildProcessGroups) {
                // recurse to verify inner process group(s)
                final Set<ProcessGroup> innerProcessGroups = processGroup.getProcessGroups();
                final Set<VersionedProcessGroup> innerVersionedProcessGroups = versionedProcessGroup.getProcessGroups();
                if (innerProcessGroups.isEmpty()) {
                    assertTrue(innerVersionedProcessGroups.isEmpty());
                } else {
                    final ProcessGroup innerProcessGroup = innerProcessGroups.iterator().next();
                    final VersionedProcessGroup innerVersionedProcessGroup = innerVersionedProcessGroups.iterator().next();

                    verifyVersionedProcessGroup(innerProcessGroup, innerVersionedProcessGroup, expectVersionControlInfo,
                            verifyChildProcessGroups);
                }
            }
        }
    }

    private void verifyPorts(final Set<Port> ports, final Set<VersionedPort> versionedPorts, final PortType portType,
                             final String expectedGroupIdentifier) {
        if (ports.isEmpty()) {
            assertTrue(versionedPorts.isEmpty());
        } else {
            final Port port = ports.iterator().next();
            final VersionedPort versionedPort = versionedPorts.iterator().next();

            assertEquals(versionedPort.getIdentifier(), flowMapper.getGroupId(port.getIdentifier()));
            assertEquals(versionedPort.getGroupIdentifier(), expectedGroupIdentifier);
            assertEquals(versionedPort.getPosition().getX(), port.getPosition().getX(), 0);
            assertEquals(versionedPort.getPosition().getY(), port.getPosition().getY(), 0);
            assertEquals(versionedPort.getName(), port.getName());
            assertEquals(versionedPort.getType(), portType);
        }
    }

    private void verifyRemotePorts(final Set<RemoteGroupPort> remotePorts,
                                   final Set<VersionedRemoteGroupPort> versionedRemotePorts,
                                   final ComponentType componentType, final String expectedPortGroupIdentifier) {
        if (remotePorts.isEmpty()) {
            assertTrue(versionedRemotePorts.isEmpty());
        } else {
            final RemoteGroupPort remotePort = remotePorts.iterator().next();
            final VersionedRemoteGroupPort versionedRemotePort = versionedRemotePorts.iterator().next();

            assertEquals(versionedRemotePort.getIdentifier(), flowMapper.getGroupId(remotePort.getIdentifier()));
            assertEquals(versionedRemotePort.getGroupIdentifier(), expectedPortGroupIdentifier);
            assertEquals(versionedRemotePort.getName(), remotePort.getName());
            assertEquals(versionedRemotePort.getComponentType(), componentType);
        }
    }

    private void verifyProcessor(final VersionedProcessor versionedProcessor, final ProcessorNode processorNode,
                                 final String expectedGroupIdentifier) {
        assertEquals(versionedProcessor.getName(), processorNode.getName());
        assertEquals(versionedProcessor.getIdentifier(), flowMapper.getGroupId(processorNode.getIdentifier()));
        assertEquals(versionedProcessor.getGroupIdentifier(), expectedGroupIdentifier);
        assertEquals(versionedProcessor.getPosition().getX(), processorNode.getPosition().getX(), 0);
        assertEquals(versionedProcessor.getPosition().getY(), processorNode.getPosition().getY(), 0);

        final PropertyDescriptor propertyDescriptor = processorNode.getProperties().keySet().iterator().next();
        final VersionedPropertyDescriptor versionedPropertyDescriptor =
                versionedProcessor.getPropertyDescriptors().get(propertyDescriptor.getName());
        assertTrue(versionedProcessor.getProperties().containsKey(propertyDescriptor.getName()));
        assertNotNull(versionedPropertyDescriptor);
        assertEquals(versionedPropertyDescriptor.getName(), propertyDescriptor.getName());
        assertEquals(versionedPropertyDescriptor.getDisplayName(), propertyDescriptor.getDisplayName());
        assertEquals(versionedPropertyDescriptor.isSensitive(), propertyDescriptor.isSensitive());
    }
}
