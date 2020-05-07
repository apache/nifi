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

import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.integration.DirectInjectionExtensionManager;
import org.apache.nifi.integration.FrameworkIntegrationTest;
import org.apache.nifi.integration.cs.LongValidatingControllerService;
import org.apache.nifi.integration.cs.NopServiceReferencingProcessor;
import org.apache.nifi.integration.processors.UsernamePasswordProcessor;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterDescriptor;
import org.apache.nifi.parameter.ParameterReferenceManager;
import org.apache.nifi.parameter.StandardParameterContext;
import org.apache.nifi.parameter.StandardParameterReferenceManager;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.registry.bucket.Bucket;
import org.apache.nifi.registry.flow.Bundle;
import org.apache.nifi.registry.flow.VersionedControllerService;
import org.apache.nifi.registry.flow.VersionedFlow;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.apache.nifi.registry.flow.VersionedFlowSnapshotMetadata;
import org.apache.nifi.registry.flow.VersionedParameter;
import org.apache.nifi.registry.flow.VersionedParameterContext;
import org.apache.nifi.registry.flow.VersionedProcessGroup;
import org.apache.nifi.registry.flow.VersionedProcessor;
import org.apache.nifi.registry.flow.diff.ComparableDataFlow;
import org.apache.nifi.registry.flow.diff.ConciseEvolvingDifferenceDescriptor;
import org.apache.nifi.registry.flow.diff.DifferenceType;
import org.apache.nifi.registry.flow.diff.FlowComparator;
import org.apache.nifi.registry.flow.diff.FlowComparison;
import org.apache.nifi.registry.flow.diff.FlowDifference;
import org.apache.nifi.registry.flow.diff.StandardComparableDataFlow;
import org.apache.nifi.registry.flow.diff.StandardFlowComparator;
import org.apache.nifi.registry.flow.mapping.NiFiRegistryFlowMapper;
import org.apache.nifi.util.FlowDifferenceFilters;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.AssertJUnit.assertNull;

public class ImportFlowIT extends FrameworkIntegrationTest {

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

        final VersionedFlowSnapshot proposedFlow = createFlowSnapshot(Collections.singletonList(controllerService), Collections.singletonList(processor), null);

        // Create an Inner Process Group and update it to match the Versioned Flow.
        final ProcessGroup innerGroup = getFlowController().getFlowManager().createProcessGroup("inner-group-id");
        innerGroup.setName("Inner Group");
        getRootGroup().addProcessGroup(innerGroup);

        innerGroup.updateFlow(proposedFlow, null, false, true, false);

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

        // Create a VersionedFlowSnapshot that contains the processor
        final VersionedFlowSnapshot versionedFlowWithExplicitValue = createFlowSnapshot(Collections.emptyList(), Collections.singletonList(processor), null);

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
        final VersionedFlowSnapshot versionedFlowWithParameterReference = createFlowSnapshot(Collections.emptyList(), Collections.singletonList(processor), null);

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

        // Create a VersionedFlowSnapshot that contains the processor
        final Parameter parameter = new Parameter(new ParameterDescriptor.Builder().name("secret-param").sensitive(true).build(), null);
        final VersionedFlowSnapshot versionedFlowWithParameterReference = createFlowSnapshot(Collections.emptyList(), Collections.singletonList(processor), Collections.singleton(parameter));

        // Create child group
        final ProcessGroup innerGroup = getFlowController().getFlowManager().createProcessGroup("inner-group-id");
        innerGroup.setName("Inner Group");
        getRootGroup().addProcessGroup(innerGroup);

        final ParameterReferenceManager parameterReferenceManager = new StandardParameterReferenceManager(getFlowController().getFlowManager());
        final ParameterContext parameterContext = new StandardParameterContext("param-context-id", "parameter-context", parameterReferenceManager, null);
        innerGroup.setParameterContext(parameterContext);

        assertTrue(parameterContext.getParameters().isEmpty());

        innerGroup.updateFlow(versionedFlowWithParameterReference, null, true, true, true);

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

        // Create a VersionedFlowSnapshot that contains the processor
        final Parameter parameter = new Parameter(new ParameterDescriptor.Builder().name("secret-param").sensitive(true).build(), null);
        final VersionedFlowSnapshot versionedFlowWithParameterReference = createFlowSnapshot(Collections.emptyList(), Collections.singletonList(processor), Collections.singleton(parameter));

        // Create child group
        final ProcessGroup innerGroup = getFlowController().getFlowManager().createProcessGroup("inner-group-id");
        innerGroup.setName("Inner Group");
        getRootGroup().addProcessGroup(innerGroup);

        innerGroup.updateFlow(versionedFlowWithParameterReference, null, true, true, true);

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

        // Create a VersionedFlowSnapshot that contains the processor
        final Parameter parameter = new Parameter(new ParameterDescriptor.Builder().name("secret-param").sensitive(true).build(), null);
        final VersionedFlowSnapshot versionedFlowWithParameterReference = createFlowSnapshot(Collections.emptyList(),
            Collections.singletonList(initialProcessor), Collections.singleton(parameter));


        // Update processor to have an explicit value for the second version of the flow.
        initialProcessor.setProperties(Collections.singletonMap(UsernamePasswordProcessor.PASSWORD.getName(), "secret-value"));
        final VersionedFlowSnapshot versionedFlowExplicitValue = createFlowSnapshot(Collections.emptyList(), Collections.singletonList(initialProcessor), null);

        // Create child group and update to the first version of the flow, with parameter ref
        final ProcessGroup innerGroup = getFlowController().getFlowManager().createProcessGroup("inner-group-id");
        innerGroup.setName("Inner Group");
        getRootGroup().addProcessGroup(innerGroup);

        innerGroup.updateFlow(versionedFlowWithParameterReference, null, true, true, true);

        final ProcessorNode nodeInGroupWithRef = innerGroup.getProcessors().iterator().next();
        assertNotNull(nodeInGroupWithRef.getProperty(UsernamePasswordProcessor.PASSWORD).getRawValue());

        // Update the flow to new version that uses explicit value.
        innerGroup.updateFlow(versionedFlowExplicitValue, null, true, true, true);

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

        final VersionedFlowSnapshot initialVersionSnapshot = createFlowSnapshot(Collections.emptyList(), Collections.singletonList(initialProcessor), null);

        // Update processor to have a different explicit value for both sensitive and non-sensitive properties and create a versioned flow for it.
        final Map<String, String> updatedProperties = new HashMap<>();
        updatedProperties.put(UsernamePasswordProcessor.USERNAME.getName(), "other");
        updatedProperties.put(UsernamePasswordProcessor.PASSWORD.getName(), "pass");
        initialProcessor.setProperties(updatedProperties);

        final VersionedFlowSnapshot updatedVersionSnapshot = createFlowSnapshot(Collections.emptyList(), Collections.singletonList(initialProcessor), null);

        // Create child group and update to the first version of the flow, with parameter ref
        final ProcessGroup innerGroup = getFlowController().getFlowManager().createProcessGroup("inner-group-id");
        innerGroup.setName("Inner Group");
        getRootGroup().addProcessGroup(innerGroup);

        // Import the flow into our newly created group
        innerGroup.updateFlow(initialVersionSnapshot, null, true, true, true);

        final ProcessorNode initialImportedProcessor = innerGroup.getProcessors().iterator().next();
        assertEquals("user", initialImportedProcessor.getProperty(UsernamePasswordProcessor.USERNAME).getRawValue());
        assertNull("pass", initialImportedProcessor.getProperty(UsernamePasswordProcessor.PASSWORD).getRawValue());

        // Update the sensitive property to "pass"
        initialImportedProcessor.setProperties(initialProperties);
        assertEquals("pass", initialImportedProcessor.getProperty(UsernamePasswordProcessor.PASSWORD).getRawValue());

        // Update the flow to new version
        innerGroup.updateFlow(updatedVersionSnapshot, null, true, true, true);

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

        final VersionedFlowSnapshot initialVersionSnapshot = createFlowSnapshot(Collections.emptyList(), Collections.singletonList(initialProcessor), null);

        // Update processor to have a different explicit value for both sensitive and non-sensitive properties and create a versioned flow for it.
        final Map<String, String> updatedProperties = new HashMap<>();
        updatedProperties.put(UsernamePasswordProcessor.USERNAME.getName(), "user");
        updatedProperties.put(UsernamePasswordProcessor.PASSWORD.getName(), "#{other-param}");
        initialProcessor.setProperties(updatedProperties);

        final VersionedFlowSnapshot updatedVersionSnapshot = createFlowSnapshot(Collections.emptyList(), Collections.singletonList(initialProcessor), null);

        // Create child group and update to the first version of the flow, with parameter ref
        final ProcessGroup innerGroup = getFlowController().getFlowManager().createProcessGroup("inner-group-id");
        innerGroup.setName("Inner Group");
        getRootGroup().addProcessGroup(innerGroup);

        // Import the flow into our newly created group
        innerGroup.updateFlow(initialVersionSnapshot, null, true, true, true);

        final Set<FlowDifference> localModifications = getLocalModifications(innerGroup, updatedVersionSnapshot);
        assertEquals(1, localModifications.size());
        assertEquals(DifferenceType.PROPERTY_CHANGED, localModifications.iterator().next().getDifferenceType());
    }


    @Test
    public void testChangeVersionFromExplicitValueToParameterSensitiveProperty() {
        // Create a processor with a sensitive property
        final ProcessorNode processorWithParamRef = createProcessorNode(UsernamePasswordProcessor.class);
        processorWithParamRef.setProperties(Collections.singletonMap(UsernamePasswordProcessor.PASSWORD.getName(), "#{secret-param}"));

        final ProcessorNode processorWithExplicitValue = createProcessorNode(UsernamePasswordProcessor.class);
        processorWithExplicitValue.setProperties(Collections.singletonMap(UsernamePasswordProcessor.PASSWORD.getName(), "secret-value"));


        // Create a VersionedFlowSnapshot that contains the processor
        final Parameter parameter = new Parameter(new ParameterDescriptor.Builder().name("secret-param").sensitive(true).build(), null);
        final VersionedFlowSnapshot versionedFlowWithParameterReference = createFlowSnapshot(Collections.emptyList(),
            Collections.singletonList(processorWithParamRef), Collections.singleton(parameter));

        final VersionedFlowSnapshot versionedFlowExplicitValue = createFlowSnapshot(Collections.emptyList(), Collections.singletonList(processorWithExplicitValue), null);

        // Create child group and update to the first version of the flow, with parameter ref
        final ProcessGroup innerGroup = getFlowController().getFlowManager().createProcessGroup("inner-group-id");
        innerGroup.setName("Inner Group");
        getRootGroup().addProcessGroup(innerGroup);

        innerGroup.updateFlow(versionedFlowExplicitValue, null, true, true, true);

        final ProcessorNode nodeInGroupWithRef = innerGroup.getProcessors().iterator().next();
        assertNotNull(nodeInGroupWithRef.getProperty(UsernamePasswordProcessor.PASSWORD));


        // Update the flow to new version that uses explicit value.
        innerGroup.updateFlow(versionedFlowWithParameterReference, null, true, true, true);

        // Updated flow has sensitive property that no longer references parameter. Now is an explicit value, so it should be unset
        final ProcessorNode nodeInGroupWithNoValue = innerGroup.getProcessors().iterator().next();
        assertEquals("#{secret-param}", nodeInGroupWithNoValue.getProperty(UsernamePasswordProcessor.PASSWORD).getRawValue());
    }




    private Set<FlowDifference> getLocalModifications(final ProcessGroup processGroup, final VersionedFlowSnapshot versionedFlowSnapshot) {
        final NiFiRegistryFlowMapper mapper = new NiFiRegistryFlowMapper(getFlowController().getExtensionManager());
        final VersionedProcessGroup localGroup = mapper.mapProcessGroup(processGroup, getFlowController().getControllerServiceProvider(), getFlowController().getFlowRegistryClient(), true);
        final VersionedProcessGroup registryGroup = versionedFlowSnapshot.getFlowContents();

        final ComparableDataFlow localFlow = new StandardComparableDataFlow("Local Flow", localGroup);
        final ComparableDataFlow registryFlow = new StandardComparableDataFlow("Versioned Flow", registryGroup);

        final Set<String> ancestorServiceIds = processGroup.getAncestorServiceIds();
        final FlowComparator flowComparator = new StandardFlowComparator(registryFlow, localFlow, ancestorServiceIds, new ConciseEvolvingDifferenceDescriptor());
        final FlowComparison flowComparison = flowComparator.compare();
        final Set<FlowDifference> differences = flowComparison.getDifferences().stream()
            .filter(difference -> difference.getDifferenceType() != DifferenceType.BUNDLE_CHANGED)
            .filter(FlowDifferenceFilters.FILTER_ADDED_REMOVED_REMOTE_PORTS)
            .filter(FlowDifferenceFilters.FILTER_PUBLIC_PORT_NAME_CHANGES)
            .filter(FlowDifferenceFilters.FILTER_IGNORABLE_VERSIONED_FLOW_COORDINATE_CHANGES)
            .collect(Collectors.toCollection(HashSet::new));

        return differences;
    }

    private VersionedFlowSnapshot createFlowSnapshot(final List<ControllerServiceNode> controllerServices, final List<ProcessorNode> processors, final Set<Parameter> parameters) {
        final VersionedFlowSnapshotMetadata snapshotMetadata = new VersionedFlowSnapshotMetadata();
        snapshotMetadata.setAuthor("unit-test");
        snapshotMetadata.setBucketIdentifier("unit-test-bucket");
        snapshotMetadata.setFlowIdentifier("unit-test-flow");
        snapshotMetadata.setTimestamp(System.currentTimeMillis());
        snapshotMetadata.setVersion(1);

        final Bucket bucket = new Bucket();
        bucket.setCreatedTimestamp(System.currentTimeMillis());
        bucket.setIdentifier("unit-test-bucket");
        bucket.setName("Unit Test Bucket");

        final VersionedFlow flow = new VersionedFlow();
        flow.setBucketIdentifier("unit-test-bucket");
        flow.setBucketName("Unit Test Bucket");
        flow.setCreatedTimestamp(System.currentTimeMillis());
        flow.setIdentifier("unit-test-flow");
        flow.setName("Unit Test Flow");

        final BundleCoordinate coordinate = getSystemBundle().getBundleDetails().getCoordinate();
        final Bundle bundle = new Bundle();
        bundle.setArtifact(coordinate.getId());
        bundle.setGroup(coordinate.getGroup());
        bundle.setVersion(coordinate.getVersion());

        final NiFiRegistryFlowMapper flowMapper = new NiFiRegistryFlowMapper(getExtensionManager());

        final Set<VersionedProcessor> versionedProcessors = new HashSet<>();
        for (final ProcessorNode processor : processors) {
            final VersionedProcessor versionedProcessor = flowMapper.mapProcessor(processor, getFlowController().getControllerServiceProvider(), Collections.emptySet(), new HashMap<>());
            versionedProcessors.add(versionedProcessor);
            processor.setVersionedComponentId(versionedProcessor.getIdentifier());
        }

        final Set<VersionedControllerService> services = new HashSet<>();
        for (final ControllerServiceNode serviceNode : controllerServices) {
            final VersionedControllerService service = flowMapper.mapControllerService(serviceNode, getFlowController().getControllerServiceProvider(), Collections.emptySet(), new HashMap<>());
            services.add(service);
            serviceNode.setVersionedComponentId(service.getIdentifier());
        }

        final VersionedProcessGroup flowContents = new VersionedProcessGroup();
        flowContents.setIdentifier("unit-test-flow-contents");
        flowContents.setName("Unit Test");
        flowContents.setProcessors(versionedProcessors);
        flowContents.setControllerServices(services);

        final VersionedFlowSnapshot versionedFlowSnapshot = new VersionedFlowSnapshot();
        versionedFlowSnapshot.setSnapshotMetadata(snapshotMetadata);
        versionedFlowSnapshot.setBucket(bucket);
        versionedFlowSnapshot.setFlow(flow);
        versionedFlowSnapshot.setFlowContents(flowContents);

        if (parameters != null) {
            final Set<VersionedParameter> versionedParameters = new HashSet<>();
            for (final Parameter parameter : parameters) {
                final VersionedParameter versionedParameter = new VersionedParameter();
                versionedParameter.setName(parameter.getDescriptor().getName());
                versionedParameter.setValue(parameter.getValue());
                versionedParameter.setSensitive(parameter.getDescriptor().isSensitive());

                versionedParameters.add(versionedParameter);
            }

            final VersionedParameterContext versionedParameterContext = new VersionedParameterContext();
            versionedParameterContext.setName("Unit Test Context");
            versionedParameterContext.setParameters(versionedParameters);
            versionedFlowSnapshot.setParameterContexts(Collections.singletonMap(versionedParameterContext.getName(), versionedParameterContext));

            flowContents.setParameterContextName("Unit Test Context");
        }

        return versionedFlowSnapshot;
    }
}
