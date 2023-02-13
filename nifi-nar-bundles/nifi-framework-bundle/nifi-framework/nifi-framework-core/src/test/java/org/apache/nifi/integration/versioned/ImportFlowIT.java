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
package org.apache.nifi.integration.versioned;

import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Funnel;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.StandardSnippet;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedComponent;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.integration.DirectInjectionExtensionManager;
import org.apache.nifi.integration.FrameworkIntegrationTest;
import org.apache.nifi.integration.cs.LongValidatingControllerService;
import org.apache.nifi.integration.cs.NopControllerService;
import org.apache.nifi.integration.cs.NopServiceReferencingProcessor;
import org.apache.nifi.integration.processors.GenerateProcessor;
import org.apache.nifi.integration.processors.UsernamePasswordProcessor;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterDescriptor;
import org.apache.nifi.parameter.ParameterReferenceManager;
import org.apache.nifi.parameter.StandardParameterContext;
import org.apache.nifi.parameter.StandardParameterReferenceManager;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.registry.flow.diff.ComparableDataFlow;
import org.apache.nifi.registry.flow.diff.ConciseEvolvingDifferenceDescriptor;
import org.apache.nifi.registry.flow.diff.DifferenceType;
import org.apache.nifi.registry.flow.diff.FlowComparator;
import org.apache.nifi.registry.flow.diff.FlowComparatorVersionedStrategy;
import org.apache.nifi.registry.flow.diff.FlowComparison;
import org.apache.nifi.registry.flow.diff.FlowDifference;
import org.apache.nifi.registry.flow.diff.StandardComparableDataFlow;
import org.apache.nifi.registry.flow.diff.StandardFlowComparator;
import org.apache.nifi.registry.flow.mapping.InstantiatedVersionedProcessGroup;
import org.apache.nifi.registry.flow.mapping.NiFiRegistryFlowMapper;
import org.apache.nifi.util.FlowDifferenceFilters;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ImportFlowIT extends FrameworkIntegrationTest {
    @Override
    public void setup() throws IOException {
        super.setup();

        for (ProcessorNode processor : getRootGroup().getProcessors()) {
            getRootGroup().removeProcessor(processor);
        }
    }

    @Override
    protected void injectExtensionTypes(final DirectInjectionExtensionManager extensionManager) {
        extensionManager.injectExtensionType(Processor.class, NopServiceReferencingProcessor.class);
        extensionManager.injectExtensionType(ControllerService.class, LongValidatingControllerService.class);
    }

    @Test
    public void testImportFlowWithProcessorAndControllerService() throws ExecutionException, InterruptedException {
        // Build a Versioned Flow that consists of a Controller Service and a Processor
        // that references that Controller Service.
        final ControllerServiceNode controllerService = createControllerServiceNode(LongValidatingControllerService.class);
        controllerService.setProperties(Collections.singletonMap(LongValidatingControllerService.DELAY.getName(), "250 millis"));

        final ProcessorNode processor = createProcessorNode(NopServiceReferencingProcessor.class);
        processor.setAutoTerminatedRelationships(Collections.singleton(REL_SUCCESS));
        processor.setProperties(Collections.singletonMap(NopServiceReferencingProcessor.SERVICE.getName(), controllerService.getIdentifier()));

        final VersionedExternalFlow proposedFlow = createFlowSnapshot();

        // Create an Inner Process Group and update it to match the Versioned Flow.
        final ProcessGroup innerGroup = getFlowController().getFlowManager().createProcessGroup("inner-group-id");
        innerGroup.setName("Inner Group");
        getRootGroup().addProcessGroup(innerGroup);

        innerGroup.updateFlow(proposedFlow, (String) null, false, true, false);

        // Ensure that the controller service is valid and enable it.
        final Set<ControllerServiceNode> serviceNodes = innerGroup.findAllControllerServices();
        assertEquals(1, serviceNodes.size());

        final ControllerServiceNode serviceNode = serviceNodes.iterator().next();
        final ValidationStatus validationStatus = serviceNode.performValidation();
        assertEquals(ValidationStatus.VALID, validationStatus);
        getFlowController().getControllerServiceProvider().enableControllerService(serviceNode).get();
        assertTrue(serviceNode.isActive());

        // Ensure that the processor is valid.
        final List<ProcessorNode> processorNodes = innerGroup.findAllProcessors();
        assertEquals(1, processorNodes.size());

        final ProcessorNode procNode = processorNodes.get(0);
        final ValidationStatus procValidationStatus = procNode.performValidation();
        final Collection<ValidationResult> validationErrors = procNode.getValidationErrors();
        System.out.println(validationErrors);
        assertEquals(Collections.emptyList(), validationErrors);
        assertEquals(ValidationStatus.VALID, procValidationStatus);

        // Ensure that the reference to the controller service was properly updated
        final String referencedServiceId = procNode.getEffectivePropertyValue(NopServiceReferencingProcessor.SERVICE);
        assertEquals(serviceNode.getIdentifier(), referencedServiceId);
        assertNotEquals("service-id", referencedServiceId);
    }


    @Test
    public void testLocalModificationWhenSensitivePropReferencesParameter() {
        // Create a processor with a sensitive property
        final ProcessorNode processor = createProcessorNode(UsernamePasswordProcessor.class);
        processor.setProperties(Collections.singletonMap(UsernamePasswordProcessor.PASSWORD.getName(), "password"));

        // Create a VersionedExternalFlow that contains the processor
        final VersionedExternalFlow versionedFlowWithExplicitValue = createFlowSnapshot();

        // Create child group
        final ProcessGroup innerGroup = getFlowController().getFlowManager().createProcessGroup("inner-group-id");
        innerGroup.setName("Inner Group");
        getRootGroup().addProcessGroup(innerGroup);

        // Move processor into the child group
        moveProcessor(processor, innerGroup);

        // Verify that there are no differences between the versioned flow and the Process Group
        Set<FlowDifference> differences = getLocalModifications(innerGroup, versionedFlowWithExplicitValue);
        assertEquals(0, differences.size());

        // Change the value of the sensitive property from one explicit value to another. Verify no local modifications.
        processor.setProperties(Collections.singletonMap(UsernamePasswordProcessor.PASSWORD.getName(), "secret"));
        differences = getLocalModifications(innerGroup, versionedFlowWithExplicitValue);
        assertEquals(0, differences.size());

        // Change the value of the sensitive property to now reference a parameter. There should be one local modification.
        processor.setProperties(Collections.singletonMap(UsernamePasswordProcessor.PASSWORD.getName(), "#{secret-parameter}"));
        differences = getLocalModifications(innerGroup, versionedFlowWithExplicitValue);
        assertEquals(1, differences.size());
        assertEquals(DifferenceType.PROPERTY_PARAMETERIZED, differences.iterator().next().getDifferenceType());

        // Create a Versioned Flow that contains the Parameter Reference.
        final VersionedExternalFlow versionedFlowWithParameterReference = createFlowSnapshot(innerGroup);

        // Ensure no difference between the current configuration and the versioned flow
        differences = getLocalModifications(innerGroup, versionedFlowWithParameterReference);
        assertEquals(0, differences.size());

        processor.setProperties(Collections.singletonMap(UsernamePasswordProcessor.PASSWORD.getName(), "secret"));
        differences = getLocalModifications(innerGroup, versionedFlowWithParameterReference);
        assertEquals(1, differences.size());
        assertEquals(DifferenceType.PROPERTY_PARAMETERIZATION_REMOVED, differences.iterator().next().getDifferenceType());
    }

    @Test
    public void testParameterCreatedWithNullValueOnImportWithSensitivePropertyReference() {
        // Create a processor with a sensitive property
        final ProcessorNode processor = createProcessorNode(UsernamePasswordProcessor.class);
        processor.setProperties(Collections.singletonMap(UsernamePasswordProcessor.PASSWORD.getName(), "#{secret-param}"));

        // Create a VersionedExternalFlow that contains the processor
        final Parameter parameter = new Parameter(new ParameterDescriptor.Builder().name("secret-param").sensitive(true).build(), null);
        setParameter(parameter);
        final VersionedExternalFlow versionedFlowWithParameterReference = createFlowSnapshot();

        // Create child group
        final ProcessGroup innerGroup = getFlowController().getFlowManager().createProcessGroup("inner-group-id");
        innerGroup.setName("Inner Group");
        getRootGroup().addProcessGroup(innerGroup);

        final ParameterReferenceManager parameterReferenceManager = new StandardParameterReferenceManager(getFlowController().getFlowManager());
        final ParameterContext parameterContext = new StandardParameterContext.Builder()
                .id("param-context-id")
                .name("parameter-context")
                .parameterReferenceManager(parameterReferenceManager)
                .build();
        innerGroup.setParameterContext(parameterContext);

        assertTrue(parameterContext.getParameters().isEmpty());

        innerGroup.updateFlow(versionedFlowWithParameterReference, (String) null, true, true, true);

        final Collection<Parameter> parameters = parameterContext.getParameters().values();
        assertEquals(1, parameters.size());

        final Parameter firstParameter = parameters.iterator().next();
        assertEquals("secret-param", firstParameter.getDescriptor().getName());
        assertTrue(firstParameter.getDescriptor().isSensitive());
        assertNull(firstParameter.getValue());
    }

    @Test
    public void testParameterContextCreatedOnImportWithSensitivePropertyReference() {
        // Create a processor with a sensitive property
        final ProcessorNode processor = createProcessorNode(UsernamePasswordProcessor.class);
        processor.setProperties(Collections.singletonMap(UsernamePasswordProcessor.PASSWORD.getName(), "#{secret-param}"));

        // Create a VersionedExternalFlow that contains the processor
        final Parameter parameter = new Parameter(new ParameterDescriptor.Builder().name("secret-param").sensitive(true).build(), null);
        setParameter(parameter);
        final VersionedExternalFlow versionedFlowWithParameterReference = createFlowSnapshot();

        // Create child group
        final ProcessGroup innerGroup = getFlowController().getFlowManager().createProcessGroup("inner-group-id");
        innerGroup.setName("Inner Group");
        getRootGroup().addProcessGroup(innerGroup);

        innerGroup.updateFlow(versionedFlowWithParameterReference, (String) null, true, true, true);

        final ParameterContext parameterContext = innerGroup.getParameterContext();
        assertNotNull(parameterContext);

        final Collection<Parameter> parameters = parameterContext.getParameters().values();
        assertEquals(1, parameters.size());

        final Parameter firstParameter = parameters.iterator().next();
        assertEquals("secret-param", firstParameter.getDescriptor().getName());
        assertTrue(firstParameter.getDescriptor().isSensitive());
        assertNull(firstParameter.getValue());
    }


    @Test
    public void testChangeVersionFromParameterToExplicitValueSensitiveProperty() {
        // Create a processor with a sensitive property
        final ProcessorNode initialProcessor = createProcessorNode(UsernamePasswordProcessor.class);
        initialProcessor.setProperties(Collections.singletonMap(UsernamePasswordProcessor.PASSWORD.getName(), "#{secret-param}"));

        // Create a VersionedExternalFlow that contains the processor
        final Parameter parameter = new Parameter(new ParameterDescriptor.Builder().name("secret-param").sensitive(true).build(), null);
        setParameter(parameter);
        final VersionedExternalFlow versionedFlowWithParameterReference = createFlowSnapshot();

        // Update processor to have an explicit value for the second version of the flow.
        initialProcessor.setProperties(Collections.singletonMap(UsernamePasswordProcessor.PASSWORD.getName(), "secret-value"));
        final VersionedExternalFlow versionedFlowExplicitValue = createFlowSnapshot();

        // Create child group and update to the first version of the flow, with parameter ref
        final ProcessGroup innerGroup = getFlowController().getFlowManager().createProcessGroup("inner-group-id");
        innerGroup.setName("Inner Group");
        getRootGroup().addProcessGroup(innerGroup);

        innerGroup.updateFlow(versionedFlowWithParameterReference, (String) null, true, true, true);

        final ProcessorNode nodeInGroupWithRef = innerGroup.getProcessors().iterator().next();
        assertNotNull(nodeInGroupWithRef.getProperty(UsernamePasswordProcessor.PASSWORD).getRawValue());

        // Update the flow to new version that uses explicit value.
        innerGroup.updateFlow(versionedFlowExplicitValue, (String) null, true, true, true);

        // Updated flow has sensitive property that no longer references parameter. Now is an explicit value, so it should be unset
        final ProcessorNode nodeInGroupWithNoValue = innerGroup.getProcessors().iterator().next();
        assertNull(nodeInGroupWithNoValue.getProperty(UsernamePasswordProcessor.PASSWORD).getRawValue());
    }

    @Test
    public void testChangeVersionFromExplicitToExplicitValueDoesNotChangeSensitiveProperty() {
        // Create a processor with a sensitive property and create a versioned flow for it.
        final ProcessorNode initialProcessor = createProcessorNode(UsernamePasswordProcessor.class);
        final Map<String, String> initialProperties = new HashMap<>();
        initialProperties.put(UsernamePasswordProcessor.USERNAME.getName(), "user");
        initialProperties.put(UsernamePasswordProcessor.PASSWORD.getName(), "pass");
        initialProcessor.setProperties(initialProperties);

        final VersionedExternalFlow initialVersionSnapshot = createFlowSnapshot();

        // Update processor to have a different explicit value for both sensitive and non-sensitive properties and create a versioned flow for it.
        final Map<String, String> updatedProperties = new HashMap<>();
        updatedProperties.put(UsernamePasswordProcessor.USERNAME.getName(), "other");
        updatedProperties.put(UsernamePasswordProcessor.PASSWORD.getName(), "pass");
        initialProcessor.setProperties(updatedProperties);

        final VersionedExternalFlow updatedVersionSnapshot = createFlowSnapshot();

        // Create child group and update to the first version of the flow, with parameter ref
        final ProcessGroup innerGroup = getFlowController().getFlowManager().createProcessGroup("inner-group-id");
        innerGroup.setName("Inner Group");
        getRootGroup().addProcessGroup(innerGroup);

        // Import the flow into our newly created group
        innerGroup.updateFlow(initialVersionSnapshot, (String) null, true, true, true);

        final ProcessorNode initialImportedProcessor = innerGroup.getProcessors().iterator().next();
        assertEquals("user", initialImportedProcessor.getProperty(UsernamePasswordProcessor.USERNAME).getRawValue());
        assertNull(initialImportedProcessor.getProperty(UsernamePasswordProcessor.PASSWORD).getRawValue());

        // Update the sensitive property to "pass"
        initialImportedProcessor.setProperties(initialProperties);
        assertEquals("pass", initialImportedProcessor.getProperty(UsernamePasswordProcessor.PASSWORD).getRawValue());

        // Update the flow to new version
        innerGroup.updateFlow(updatedVersionSnapshot, (String) null, true, true, true);

        // Updated flow has sensitive property that no longer references parameter. Now is an explicit value, so it should be unset
        final ProcessorNode updatedImportedProcessor = innerGroup.getProcessors().iterator().next();
        assertEquals("other", updatedImportedProcessor.getProperty(UsernamePasswordProcessor.USERNAME).getRawValue());
        assertEquals("pass", updatedImportedProcessor.getProperty(UsernamePasswordProcessor.PASSWORD).getRawValue());
    }


    @Test
    public void testChangeVersionFromParamReferenceToAnotherParamReferenceIsLocalModification() {
        // Create a processor with a sensitive property and create a versioned flow for it.
        final ProcessorNode initialProcessor = createProcessorNode(UsernamePasswordProcessor.class);
        final Map<String, String> initialProperties = new HashMap<>();
        initialProperties.put(UsernamePasswordProcessor.USERNAME.getName(), "user");
        initialProperties.put(UsernamePasswordProcessor.PASSWORD.getName(), "#{secret-param}");
        initialProcessor.setProperties(initialProperties);

        final VersionedExternalFlow initialVersionSnapshot = createFlowSnapshot();

        // Update processor to have a different explicit value for both sensitive and non-sensitive properties and create a versioned flow for it.
        final Map<String, String> updatedProperties = new HashMap<>();
        updatedProperties.put(UsernamePasswordProcessor.USERNAME.getName(), "user");
        updatedProperties.put(UsernamePasswordProcessor.PASSWORD.getName(), "#{other-param}");
        initialProcessor.setProperties(updatedProperties);

        final VersionedExternalFlow updatedVersionSnapshot = createFlowSnapshot();

        // Create child group and update to the first version of the flow, with parameter ref
        final ProcessGroup innerGroup = getFlowController().getFlowManager().createProcessGroup("inner-group-id");
        innerGroup.setName("Inner Group");
        getRootGroup().addProcessGroup(innerGroup);

        // Import the flow into our newly created group
        innerGroup.updateFlow(initialVersionSnapshot, (String) null, true, true, true);

        final Set<FlowDifference> localModifications = getLocalModifications(innerGroup, updatedVersionSnapshot);
        assertEquals(1, localModifications.size());
        assertEquals(DifferenceType.PROPERTY_CHANGED, localModifications.iterator().next().getDifferenceType());
    }


    @Test
    public void testChangeVersionFromExplicitValueToParameterSensitiveProperty() {
        // Create a processor with a sensitive property
        final ProcessorNode processor = createProcessorNode(UsernamePasswordProcessor.class);
        processor.setProperties(Collections.singletonMap(UsernamePasswordProcessor.PASSWORD.getName(), "#{secret-param}"));

        // Create a VersionedExternalFlow that contains the processor
        final Parameter parameter = new Parameter(new ParameterDescriptor.Builder().name("secret-param").sensitive(true).build(), null);
        setParameter(parameter);
        final VersionedExternalFlow versionedFlowWithParameterReference = createFlowSnapshot();

        processor.setProperties(Collections.singletonMap(UsernamePasswordProcessor.PASSWORD.getName(), "secret-value"));
        final VersionedExternalFlow versionedFlowExplicitValue = createFlowSnapshot();

        // Create child group and update to the first version of the flow, with parameter ref
        final ProcessGroup innerGroup = getFlowController().getFlowManager().createProcessGroup("inner-group-id");
        innerGroup.setName("Inner Group");
        getRootGroup().addProcessGroup(innerGroup);

        innerGroup.updateFlow(versionedFlowExplicitValue, (String) null, true, true, true);

        final ProcessorNode nodeInGroupWithRef = innerGroup.getProcessors().iterator().next();
        assertNotNull(nodeInGroupWithRef.getProperty(UsernamePasswordProcessor.PASSWORD));

        // Update the flow to new version that uses explicit value.
        innerGroup.updateFlow(versionedFlowWithParameterReference, (String) null, true, true, true);

        // Updated flow has sensitive property that no longer references parameter. Now is an explicit value, so it should be unset
        final ProcessorNode nodeInGroupWithNoValue = innerGroup.getProcessors().iterator().next();
        String actual = nodeInGroupWithNoValue.getProperty(UsernamePasswordProcessor.PASSWORD).getRawValue();
        assertEquals("#{secret-param}", actual);
    }

    @Test
    public void testUpdateFlowWithInputPortMovedFromGroupAToGroupB() {
        //Testing use case NIFI-9018
        //Create Process Group A
        final ProcessGroup groupA = createProcessGroup("group-a-id", "Group A", getRootGroup());

        //Add Input Port to Process Group A
        final Port port = getFlowController().getFlowManager().createLocalInputPort("input-port-id", "Input Port");
        groupA.addInputPort(port);

        //Create a snapshot
        final VersionedExternalFlow version1 = createFlowSnapshot(groupA);

        //Create Process Group B under Process Group A
        final ProcessGroup groupB = createProcessGroup("group-b-id", "Group B", groupA);

        //Move Input Port from Process Group A to Process Group B
        moveInputPort(port, groupB);

        //Create Processor under Process Group A
        final ProcessorNode processor = createProcessorNode(GenerateProcessor.class, groupA);

        //Create Connection between Processor in Process Group A and Input Port in Process Group B
        final Connection connection = connect(groupA, processor, port, processor.getRelationships());

        //Create another snapshot
        final VersionedExternalFlow version2 = createFlowSnapshot(groupA);

        //Change Process Group A version to Version 1
        groupA.updateFlow(version1, null, false, true, true);

        //Process Group A should have only one Input Port and no Process Groups, Processors or Connections
        assertTrue(groupA.getProcessGroups().isEmpty());
        assertTrue(groupA.getProcessors().isEmpty());
        assertTrue(groupA.getConnections().isEmpty());
        assertEquals(1, groupA.getInputPorts().size());
        assertEquals(port.getName(), groupA.getInputPorts().stream().findFirst().get().getName());

        //Change Process Group A version to Version 2
        groupA.updateFlow(version2, null, false, true, true);

        //Process Group A should have a Process Group, a Processor and a Connection and no Input Ports
        assertEquals(1, groupA.getProcessGroups().size());
        assertEquals(groupB.getName(), groupA.getProcessGroups().stream().findFirst().get().getName());
        assertEquals(1, groupA.getProcessors().size());
        assertEquals(processor.getName(), groupA.getProcessors().stream().findFirst().get().getName());
        assertEquals(1, groupA.getConnections().size());
        assertEquals(connection.getName(), groupA.getConnections().stream().findFirst().get().getName());
        assertTrue(groupA.getInputPorts().isEmpty());
    }

    @Test
    public void testUpdateFlowWithOutputPortChangedToFunnelInAConnection() {
        //Testing use case NIFI-9229
        //Create Process Group
        final ProcessGroup group = createProcessGroup("p-group-id", "P Group", getRootGroup());

        //Create Processor under Process Group
        final ProcessorNode processor = createProcessorNode(GenerateProcessor.class, group);

        //Add Output Port to Process Group
        final Port port = getFlowController().getFlowManager().createLocalOutputPort("output-port-id", "Output Port");
        group.addOutputPort(port);

        //Create Connection between Processor and Input Port
        final Connection connection = connect(group, processor, port, processor.getRelationships());

        //Create a snapshot
        final VersionedExternalFlow version1 = createFlowSnapshot(group);

        //Create Funnel under Process Group
        Funnel funnel = getFlowController().getFlowManager().createFunnel("funnel-id");
        group.addFunnel(funnel);

        //Modify connection's destination from Output Port to Funnel
        connection.setDestination(funnel);

        //Delete Output Port
        group.removeOutputPort(port);

        //Create another snapshot
        final VersionedExternalFlow version2 = createFlowSnapshot(group);

        //Change Process Group version to Version 1
        group.updateFlow(version1, null, false, true, true);

        //Process Group should have only one Output Port, One Processor and One connection
        assertEquals(1, group.getProcessors().size());
        assertEquals(processor.getVersionedComponentId(), group.getProcessors().stream().findFirst().get().getVersionedComponentId());
        assertEquals(1, group.getConnections().size());
        assertEquals(connection.getVersionedComponentId(), group.getConnections().stream().findFirst().get().getVersionedComponentId());
        assertEquals(1, group.getOutputPorts().size());
        assertEquals(port.getName(), group.getOutputPorts().stream().findFirst().get().getName());
        assertTrue(group.getFunnels().isEmpty());
        assertEquals(connection.getDestination().getName(), port.getName());

        //Change Process Group version to Version 2
        group.updateFlow(version2, null, false, true, true);

        //Process Group should have a Funnel, a Processor, a Connection and no Output Ports
        assertTrue(group.getOutputPorts().isEmpty());
        assertEquals(1, group.getProcessors().size());
        assertEquals(processor.getName(), group.getProcessors().stream().findFirst().get().getName());
        assertEquals(1, group.getConnections().size());
        assertEquals(connection.getName(), group.getConnections().stream().findFirst().get().getName());
        assertEquals(1, group.getFunnels().size());
        assertEquals(funnel.getName(), group.getFunnels().stream().findFirst().get().getName());
        assertEquals(connection.getDestination().getName(), funnel.getName());
    }

    @Test
    public void testUpdateFlowWithModifyingConnectionDeletingAndMovingPort() {
        //Create Process Group A
        final ProcessGroup groupA = createProcessGroup("group-a-id", "Group A", getRootGroup());

        //Create Process Group B under Process Group A
        final ProcessGroup groupBunderA = createProcessGroup("group-b-id", "Group B", groupA);

        //Add Input port under Process Group B
        final Port inputPortBThenStayThenDelete = getFlowController().getFlowManager().createLocalInputPort("input-port-id", "Input Port");
        groupBunderA.addInputPort(inputPortBThenStayThenDelete);

        //Add Processor 1 under Process Group A
        final ProcessorNode processorA1 = createProcessorNode(GenerateProcessor.class, groupA);

        //Add Processor 2 under Process Group A
        final ProcessorNode processorA2 = createProcessorNode(GenerateProcessor.class, groupA);

        //Add Output Port under Process Group A
        final Port outputPortAThenB = getFlowController().getFlowManager().createLocalOutputPort("output-port-id", "Output Port");
        groupA.addOutputPort(outputPortAThenB);

        //Connect Processor 1 and Output Port as Connection 1
        final Connection connectionProcessorA1ToOutputPortAThenProcessorA2 = connect(groupA, processorA1, outputPortAThenB, processorA1.getRelationships());

        //Connect Processor 1 and Input Port as Connection 2
        final Connection connectionProcessorA1ToInputPortBThenStayThenDelete = connect(groupA, processorA1, inputPortBThenStayThenDelete, processorA1.getRelationships());

        //Create a snapshot
        final VersionedExternalFlow version1 = createFlowSnapshot(groupA);

        //Modify Connection 1 to point to Processor 2
        connectionProcessorA1ToOutputPortAThenProcessorA2.setDestination(processorA2);

        //Move Output Port to Process Group B
        moveOutputPort(outputPortAThenB, groupBunderA);

        //Create another snapshot
        final VersionedExternalFlow version2 = createFlowSnapshot(groupA);

        //Delete connection 2
        groupA.removeConnection(connectionProcessorA1ToInputPortBThenStayThenDelete);

        //Delete Input Port
        groupBunderA.removeInputPort(inputPortBThenStayThenDelete);

        //Create another snapshot
        final VersionedExternalFlow version3 = createFlowSnapshot(groupA);

        //Change Process Group version to Version 1
        groupA.updateFlow(version1, null, false, true, true);

        //Process Group A should have two Processors, 2 Connections, one Output Port and one Process Group with one Input Port
        assertEquals(2, groupA.getProcessors().size());
        assertEquals(2, groupA.getConnections().size());
        assertEquals(connectionProcessorA1ToOutputPortAThenProcessorA2.getDestination().getName(), outputPortAThenB.getName());
        assertEquals(1, groupA.getOutputPorts().size());
        assertEquals(1, groupA.getProcessGroups().size());
        assertEquals(1, groupBunderA.getInputPorts().size());

        //Change Process Group version to Version 2
        groupA.updateFlow(version2, null, false, true, true);

        //Connection1 destination changed to Processor2 and Output Port moved to Process Group B
        assertTrue(groupA.getOutputPorts().isEmpty());
        assertEquals(connectionProcessorA1ToOutputPortAThenProcessorA2.getDestination().getName(), processorA2.getName());
        assertEquals(1, groupBunderA.getOutputPorts().size());
        assertEquals(outputPortAThenB.getName(), groupBunderA.getOutputPorts().stream().findFirst().get().getName());

        //Change Process Group version to Version 3
        groupA.updateFlow(version3, null, false, true, true);

        //Connection2 and Input Port should be deleted
        assertEquals(1, groupA.getConnections().size());
        assertEquals(connectionProcessorA1ToOutputPortAThenProcessorA2.getName(), groupA.getConnections().stream().findFirst().get().getName());
        assertTrue(groupBunderA.getInputPorts().isEmpty());
    }

    @Test
    public void testUpdateFlowWithDeletingConnectionDeletingAndMovingPort() {
        //Create Process Group A
        final ProcessGroup groupA = createProcessGroup("group-a-id", "Group A", getRootGroup());

        //Create Process Group B under Process Group A
        final ProcessGroup groupB = createProcessGroup("group-b-id", "Group B", groupA);

        //Add Input port under Process Group B
        final Port inputPort = getFlowController().getFlowManager().createLocalInputPort("input-port-id", "Input Port");
        groupB.addInputPort(inputPort);

        //Add Processor 1 under Process Group A
        final ProcessorNode processor1 = createProcessorNode(GenerateProcessor.class, groupA);

        //Add Processor 2 under Process Group A
        final ProcessorNode processor2 = createProcessorNode(GenerateProcessor.class, groupA);

        //Add Output Port under Process Group A
        final Port outputPort = getFlowController().getFlowManager().createLocalOutputPort("output-port-id", "Output Port");
        groupA.addOutputPort(outputPort);

        //Connect Processor 1 and Output Port as Connection 1
        final Connection connection1 = connect(groupA, processor1, outputPort, processor1.getRelationships());

        //Connect Processor 1 and Input Port as Connection 2
        final Connection connection2 = connect(groupA, processor1, inputPort, processor1.getRelationships());

        //Create a snapshot
        final VersionedExternalFlow version1 = createFlowSnapshot(groupA);

        //Modify Connection 1 to point to Processor 2
        connection1.setDestination(processor2);

        //Delete Output Port
        groupA.removeOutputPort(outputPort);

        //Create another snapshot
        final VersionedExternalFlow version2 = createFlowSnapshot(groupA);

        //Delete connection 2
        groupA.removeConnection(connection2);

        //Move Input Port to Process Group A
        moveInputPort(inputPort, groupA);

        //Create another snapshot
        final VersionedExternalFlow version3 = createFlowSnapshot(groupA);

        //Change Process Group version to Version 1
        groupA.updateFlow(version1, null, false, true, true);

        //Process Group A should have two Processors, 2 Connections, one Output Port and one Process Group with one Input Port
        assertEquals(2, groupA.getProcessors().size());
        assertEquals(2, groupA.getConnections().size());
        assertEquals(connection1.getDestination().getName(), outputPort.getName());
        assertEquals(1, groupA.getOutputPorts().size());
        assertEquals(1, groupA.getProcessGroups().size());
        assertEquals(1, groupB.getInputPorts().size());

        //Change Process Group version to Version 2
        groupA.updateFlow(version2, null, false, true, true);

        //Connection1 destination changed to Processor2 and Output Port deleted
        assertEquals(connection1.getDestination().getName(), processor2.getName());
        assertTrue(groupA.getOutputPorts().isEmpty());
        assertTrue(groupB.getOutputPorts().isEmpty());

        //Change Process Group version to Version 3
        groupA.updateFlow(version3, null, false, true, true);

        //Connection2 should be deleted and Input Port moved to Process Group A
        assertEquals(1, groupA.getConnections().size());
        assertEquals(connection1.getName(), groupA.getConnections().stream().findFirst().get().getName());
        assertTrue(groupB.getInputPorts().isEmpty());
        assertEquals(1, groupA.getInputPorts().size());
        assertEquals(inputPort.getName(), groupA.getInputPorts().stream().findFirst().get().getName());
    }

    @Test
    public void testExportImportFlowSwitchesVersionedIdToAndFromInstanceIdOfParameterReferencedControllerService() {
        ControllerServiceNode controllerService = createControllerServiceNode(NopControllerService.class);

        ProcessorNode processor = createProcessorNode(NopServiceReferencingProcessor.class);
        processor.setAutoTerminatedRelationships(Collections.singleton(REL_SUCCESS));
        processor.setProperties(Collections.singletonMap(NopServiceReferencingProcessor.SERVICE.getName(), "#{service}"));

        // Setting value to the instance id
        Parameter parameter = new Parameter(new ParameterDescriptor.Builder()
            .name("service")
            .build(),
            controllerService.getIdentifier()
        );
        setParameter(parameter);
        VersionedExternalFlow flowSnapshot = createFlowSnapshot();

        VersionedControllerService snapshotControllerService = flowSnapshot.getFlowContents().getControllerServices().stream().findAny().get();

        assertEquals(controllerService.getIdentifier(), snapshotControllerService.getInstanceIdentifier());
        // Exported flow contains versioned id instead of instance id
        assertEquals(snapshotControllerService.getIdentifier(), flowSnapshot.getParameterContexts().get("unimportant").getParameters().stream().findAny().get().getValue());

        getRootGroup().setParameterContext(null);
        getRootGroup().updateFlow(flowSnapshot, null, false, true, true);

        // Imported flow contains instance id again
        assertEquals(snapshotControllerService.getInstanceIdentifier(), getRootGroup().getParameterContext().getParameter("service").get().getValue());
    }

    private void setParameter(Parameter parameter) {
        ParameterContext rootParameterContext = getFlowController().getFlowManager().getParameterContextManager().getParameterContext("unimportant");
        if (rootParameterContext == null) {
            rootParameterContext = getFlowController().getFlowManager().createParameterContext("unimportant", "unimportant", Collections.emptyMap(), Collections.emptyList(), null);
            getRootGroup().setParameterContext(rootParameterContext);
        }

        Map<String, Parameter> parameterMap = new HashMap<>();
        parameterMap.put(parameter.getDescriptor().getName(), parameter);
        rootParameterContext.setParameters(parameterMap);
    }

    private ProcessGroup createProcessGroup(final String groupId, final String groupName, final ProcessGroup destination) {
        final ProcessGroup group = getFlowController().getFlowManager().createProcessGroup(groupId);
        group.setName(groupName);
        destination.addProcessGroup(group);
        return group;
    }

    private void moveInputPort(final Port port, final ProcessGroup destination) {
        final StandardSnippet snippet = new StandardSnippet();
        snippet.setParentGroupId(port.getProcessGroupIdentifier());
        snippet.addInputPorts(Collections.singletonMap(port.getIdentifier(), null));

        port.getProcessGroup().move(snippet, destination);
    }

    private void moveOutputPort(final Port port, final ProcessGroup destination) {
        final StandardSnippet snippet = new StandardSnippet();
        snippet.setParentGroupId(port.getProcessGroupIdentifier());
        snippet.addOutputPorts(Collections.singletonMap(port.getIdentifier(), null));

        port.getProcessGroup().move(snippet, destination);
    }


    private Set<FlowDifference> getLocalModifications(final ProcessGroup processGroup, final VersionedExternalFlow VersionedExternalFlow) {
        final NiFiRegistryFlowMapper mapper = new NiFiRegistryFlowMapper(getFlowController().getExtensionManager());
        final VersionedProcessGroup localGroup = mapper.mapProcessGroup(processGroup, getFlowController().getControllerServiceProvider(), getFlowController().getFlowManager(), true);
        final VersionedProcessGroup registryGroup = VersionedExternalFlow.getFlowContents();

        final ComparableDataFlow localFlow = new StandardComparableDataFlow("Local Flow", localGroup);
        final ComparableDataFlow registryFlow = new StandardComparableDataFlow("Versioned Flow", registryGroup);

        final Set<String> ancestorServiceIds = processGroup.getAncestorServiceIds();
        final FlowComparator flowComparator = new StandardFlowComparator(registryFlow, localFlow, ancestorServiceIds, new ConciseEvolvingDifferenceDescriptor(), Function.identity(),
            VersionedComponent::getIdentifier, FlowComparatorVersionedStrategy.SHALLOW);
        final FlowComparison flowComparison = flowComparator.compare();
        final Set<FlowDifference> differences = flowComparison.getDifferences().stream()
            .filter(FlowDifferenceFilters.FILTER_ADDED_REMOVED_REMOTE_PORTS)
            .filter(FlowDifferenceFilters.FILTER_PUBLIC_PORT_NAME_CHANGES)
            .filter(FlowDifferenceFilters.FILTER_IGNORABLE_VERSIONED_FLOW_COORDINATE_CHANGES)
            .filter(difference -> !FlowDifferenceFilters.isVariableValueChange(difference))
            .collect(Collectors.toCollection(HashSet::new));

        return differences;
    }

    private VersionedExternalFlow createFlowSnapshot() {
        return createFlowSnapshot(getRootGroup());
    }

    private VersionedExternalFlow createFlowSnapshot(final ProcessGroup group) {
        final NiFiRegistryFlowMapper flowMapper = new NiFiRegistryFlowMapper(getExtensionManager());

        InstantiatedVersionedProcessGroup instantiatedVersionedProcessGroup = flowMapper.mapNonVersionedProcessGroup(group, getFlowController().getControllerServiceProvider());
        final VersionedExternalFlow flow = new VersionedExternalFlow();
        flow.setFlowContents(instantiatedVersionedProcessGroup);

        Map<String, VersionedParameterContext> parameterContexts = new HashMap<>();
        if (getRootGroup().getParameterContext() != null) {
            parameterContexts.put(getRootGroup().getParameterContext().getName(), flowMapper.mapParameterContext(getRootGroup().getParameterContext()));
        }
        Set<ProcessGroup> childProcessGroups = getRootGroup().getProcessGroups();
        for (ProcessGroup processGroup : childProcessGroups) {
            if (processGroup.getParameterContext() != null) {
                parameterContexts.put(processGroup.getParameterContext().getName(), flowMapper.mapParameterContext(processGroup.getParameterContext()));
            }
        }
        flow.setParameterContexts(parameterContexts);

        return flow;
    }
}
