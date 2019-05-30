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
import org.apache.nifi.processor.Processor;
import org.apache.nifi.registry.bucket.Bucket;
import org.apache.nifi.registry.flow.Bundle;
import org.apache.nifi.registry.flow.VersionedControllerService;
import org.apache.nifi.registry.flow.VersionedFlow;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.apache.nifi.registry.flow.VersionedFlowSnapshotMetadata;
import org.apache.nifi.registry.flow.VersionedProcessGroup;
import org.apache.nifi.registry.flow.VersionedProcessor;
import org.apache.nifi.registry.flow.mapping.NiFiRegistryFlowMapper;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

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

        final VersionedFlowSnapshot proposedFlow = createFlowSnapshot(Collections.singletonList(controllerService), Collections.singletonList(processor));

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
        final String referencedServiceId = procNode.getProperty(NopServiceReferencingProcessor.SERVICE);
        assertEquals(serviceNode.getIdentifier(), referencedServiceId);
        assertNotEquals("service-id", referencedServiceId);
    }


    private VersionedFlowSnapshot createFlowSnapshot(final List<ControllerServiceNode> controllerServices, final List<ProcessorNode> processors) {
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
            final VersionedProcessor versionedProcessor = flowMapper.mapProcessor(processor, getFlowController().getControllerServiceProvider());
            versionedProcessors.add(versionedProcessor);
        }

        final Set<VersionedControllerService> services = new HashSet<>();
        for (final ControllerServiceNode serviceNode : controllerServices) {
            final VersionedControllerService service = flowMapper.mapControllerService(serviceNode, getFlowController().getControllerServiceProvider());
            services.add(service);
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

        return versionedFlowSnapshot;
    }
}
