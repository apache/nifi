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

package org.apache.nifi.registry.flow.diff;

import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.ScheduledState;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.flow.VersionedPropertyDescriptor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ComponentAddedRebaseHandlerTest {

    private static final String ROOT = "root";
    private static final String CHILD = "child";
    private static final String PROCESSOR_ID = "processor-a";
    private static final String EXISTING_SERVICE_ID = "service-x";
    private static final String ADDED_SERVICE_ID = "service-y";
    private static final String SERVICE_REFERENCE_PROPERTY = "delegate.service";
    private static final String LOCAL_SERVICE_NAME = "Local Controller Service";
    private static final String LOCAL_SERVICE_TYPE = "org.apache.nifi.services.LocalControllerService";
    private static final String BUNDLE_GROUP = "group";
    private static final String BUNDLE_ARTIFACT = "artifact";
    private static final String BUNDLE_VERSION = "1.0.0";
    private static final String LOCAL_COMMENTS = "local comments";
    private static final String SERVICE_ENABLED_PROPERTY = "service.enabled";
    private static final String SERVICE_ENABLED_VALUE = "true";

    private ComponentAddedRebaseHandler handler;

    @BeforeEach
    void setup() {
        handler = new ComponentAddedRebaseHandler();
    }

    @Test
    void testClassifyReferencedControllerServiceAdditionIsCompatible() {
        final VersionedControllerService addedService = createControllerService(ADDED_SERVICE_ID, ROOT);
        addedService.setProperties(Map.of(SERVICE_REFERENCE_PROPERTY, EXISTING_SERVICE_ID));
        addedService.setPropertyDescriptors(Map.of(SERVICE_REFERENCE_PROPERTY, createDescriptor(SERVICE_REFERENCE_PROPERTY, false, false)));

        final FlowDifference localDifference = new StandardFlowDifference(DifferenceType.COMPONENT_ADDED, null, addedService,
                null, addedService, "Referenced controller service added locally");

        final VersionedProcessGroup targetSnapshot = createTargetSnapshotWithExistingService(EXISTING_SERVICE_ID, ROOT);

        final RebaseAnalysis.ClassifiedDifference result = handler.classify(localDifference, Collections.emptySet(), targetSnapshot);

        assertEquals(RebaseClassification.COMPATIBLE, result.getClassification());
    }

    @Test
    void testClassifyUnreferencedControllerServiceAdditionIsCompatible() {
        final VersionedControllerService addedService = createControllerService(ADDED_SERVICE_ID, ROOT);
        final FlowDifference localDifference = new StandardFlowDifference(DifferenceType.COMPONENT_ADDED, null, addedService,
                null, addedService, "Unreferenced controller service added locally");

        final VersionedProcessGroup targetSnapshot = createTargetSnapshotWithExistingService(EXISTING_SERVICE_ID, ROOT);

        final RebaseAnalysis.ClassifiedDifference result = handler.classify(localDifference, Collections.emptySet(), targetSnapshot);

        assertEquals(RebaseClassification.COMPATIBLE, result.getClassification());
    }

    @Test
    void testClassifyNonControllerServiceAdditionIsUnsupported() {
        final VersionedProcessor processor = new VersionedProcessor();
        processor.setIdentifier(PROCESSOR_ID);

        final FlowDifference localDifference = new StandardFlowDifference(DifferenceType.COMPONENT_ADDED, null, processor,
                null, processor, "Processor added locally");

        final VersionedProcessGroup targetSnapshot = createRootGroup();

        final RebaseAnalysis.ClassifiedDifference result = handler.classify(localDifference, Collections.emptySet(), targetSnapshot);

        assertEquals(RebaseClassification.UNSUPPORTED, result.getClassification());
        assertEquals(RebaseConflictCode.UNSUPPORTED_COMPONENT_TYPE, result.getConflictCode());
    }

    @Test
    void testClassifyNullParentIsUnsupported() {
        final VersionedControllerService addedService = createControllerService(ADDED_SERVICE_ID, null);
        final FlowDifference localDifference = new StandardFlowDifference(DifferenceType.COMPONENT_ADDED, null, addedService,
                null, addedService, "Controller service added without parent");

        final RebaseAnalysis.ClassifiedDifference result = handler.classify(localDifference, Collections.emptySet(), createRootGroup());

        assertEquals(RebaseClassification.UNSUPPORTED, result.getClassification());
        assertEquals(RebaseConflictCode.COMPONENT_NOT_FOUND, result.getConflictCode());
    }

    @Test
    void testClassifyMissingParentIsUnsupported() {
        final VersionedControllerService addedService = createControllerService(ADDED_SERVICE_ID, CHILD);
        final FlowDifference localDifference = new StandardFlowDifference(DifferenceType.COMPONENT_ADDED, null, addedService,
                null, addedService, "Controller service added to missing parent");
        final VersionedProcessGroup removedParent = createChildGroup(CHILD);
        final Set<FlowDifference> upstreamDifferences = Set.of(new StandardFlowDifference(DifferenceType.COMPONENT_REMOVED, removedParent, null,
                removedParent, null, "Parent removed upstream"));

        final RebaseAnalysis.ClassifiedDifference result = handler.classify(localDifference, upstreamDifferences, createRootGroup());

        assertEquals(RebaseClassification.UNSUPPORTED, result.getClassification());
        assertEquals(RebaseConflictCode.COMPONENT_NOT_FOUND, result.getConflictCode());
    }

    @Test
    void testClassifySameIdentifierTargetCollisionIsConflicting() {
        final VersionedControllerService addedService = createControllerService(ADDED_SERVICE_ID, ROOT);
        final FlowDifference localDifference = new StandardFlowDifference(DifferenceType.COMPONENT_ADDED, null, addedService,
                null, addedService, "Controller service added with colliding identifier");

        final VersionedProcessGroup childGroup = new VersionedProcessGroup();
        childGroup.setIdentifier(CHILD);

        final VersionedProcessor collidingProcessor = new VersionedProcessor();
        collidingProcessor.setIdentifier(ADDED_SERVICE_ID);
        childGroup.getProcessors().add(collidingProcessor);

        final VersionedProcessGroup targetSnapshot = createRootGroup();
        targetSnapshot.getProcessGroups().add(childGroup);

        final RebaseAnalysis.ClassifiedDifference result = handler.classify(localDifference, Collections.emptySet(), targetSnapshot);

        assertEquals(RebaseClassification.CONFLICTING, result.getClassification());
        assertEquals(RebaseConflictCode.COMPONENT_IDENTIFIER_COLLISION, result.getConflictCode());
    }

    @Test
    void testApplyAddsControllerServiceToRootPreservingIdentityAndConfiguration() {
        final VersionedControllerService addedService = createControllerService(ADDED_SERVICE_ID, ROOT);
        final FlowDifference localDifference = new StandardFlowDifference(DifferenceType.COMPONENT_ADDED, null, addedService,
                null, addedService, "Controller service added to root");

        final VersionedProcessGroup mergedFlow = new VersionedProcessGroup();
        mergedFlow.setIdentifier(ROOT);

        handler.apply(localDifference, mergedFlow);

        assertNotNull(mergedFlow.getControllerServices());
        assertEquals(1, mergedFlow.getControllerServices().size());

        final VersionedControllerService insertedService = mergedFlow.getControllerServices().iterator().next();
        assertSame(addedService, insertedService);
        assertEquals(ADDED_SERVICE_ID, insertedService.getIdentifier());
        assertEquals(ROOT, insertedService.getGroupIdentifier());
        assertEquals(LOCAL_SERVICE_NAME, insertedService.getName());
        assertEquals(LOCAL_SERVICE_TYPE, insertedService.getType());
        assertEquals(BUNDLE_GROUP, insertedService.getBundle().getGroup());
        assertEquals(BUNDLE_ARTIFACT, insertedService.getBundle().getArtifact());
        assertEquals(BUNDLE_VERSION, insertedService.getBundle().getVersion());
        assertSame(addedService.getProperties(), insertedService.getProperties());
        assertSame(addedService.getPropertyDescriptors(), insertedService.getPropertyDescriptors());
        assertEquals(LOCAL_COMMENTS, insertedService.getComments());
        assertEquals(ScheduledState.DISABLED, insertedService.getScheduledState());
    }

    @Test
    void testApplyAddsControllerServiceToNestedParent() {
        final VersionedControllerService addedService = createControllerService(ADDED_SERVICE_ID, CHILD);
        final FlowDifference localDifference = new StandardFlowDifference(DifferenceType.COMPONENT_ADDED, null, addedService,
                null, addedService, "Controller service added to nested group");

        final VersionedProcessGroup childGroup = new VersionedProcessGroup();
        childGroup.setIdentifier(CHILD);

        final VersionedProcessGroup mergedFlow = createRootGroup();
        mergedFlow.getProcessGroups().add(childGroup);

        handler.apply(localDifference, mergedFlow);

        assertEquals(1, childGroup.getControllerServices().size());
        assertSame(addedService, childGroup.getControllerServices().iterator().next());
        assertEquals(0, mergedFlow.getControllerServices().size());
    }

    @Test
    void testApplyThrowsWhenVerifiedParentIsMissing() {
        final VersionedControllerService addedService = createControllerService(ADDED_SERVICE_ID, CHILD);
        final FlowDifference localDifference = new StandardFlowDifference(DifferenceType.COMPONENT_ADDED, null, addedService,
                null, addedService, "Controller service added to missing parent");

        final IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> handler.apply(localDifference, createRootGroup()));

        assertEquals("Parent Process Group child for Controller Service service-y was verified during classification but is absent during apply",
                exception.getMessage());
    }

    @Test
    void testApplyThrowsWhenIdentifierAlreadyExists() {
        final VersionedControllerService addedService = createControllerService(ADDED_SERVICE_ID, ROOT);
        final FlowDifference localDifference = new StandardFlowDifference(DifferenceType.COMPONENT_ADDED, null, addedService,
                null, addedService, "Controller service added with colliding identifier");

        final VersionedProcessor existingProcessor = new VersionedProcessor();
        existingProcessor.setIdentifier(ADDED_SERVICE_ID);

        final VersionedProcessGroup mergedFlow = createRootGroup();
        mergedFlow.getProcessors().add(existingProcessor);

        final IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> handler.apply(localDifference, mergedFlow));

        assertEquals("Merged flow already contains component VersionedProcessor with identifier service-y", exception.getMessage());
    }

    private VersionedProcessGroup createTargetSnapshotWithExistingService(final String serviceIdentifier, final String groupIdentifier) {
        final VersionedProcessGroup rootGroup = createRootGroup();
        final VersionedProcessGroup parentGroup = ROOT.equals(groupIdentifier) ? rootGroup : createChildGroup(groupIdentifier);

        if (parentGroup != rootGroup) {
            rootGroup.getProcessGroups().add(parentGroup);
        }

        parentGroup.getControllerServices().add(createControllerService(serviceIdentifier, groupIdentifier));
        return rootGroup;
    }

    private VersionedProcessGroup createRootGroup() {
        final VersionedProcessGroup rootGroup = new VersionedProcessGroup();
        rootGroup.setIdentifier(ROOT);
        rootGroup.setInstanceIdentifier(ROOT);
        rootGroup.setControllerServices(new HashSet<>());
        return rootGroup;
    }

    private VersionedProcessGroup createChildGroup(final String identifier) {
        final VersionedProcessGroup childGroup = new VersionedProcessGroup();
        childGroup.setIdentifier(identifier);
        childGroup.setInstanceIdentifier(identifier);
        childGroup.setControllerServices(new HashSet<>());
        return childGroup;
    }

    private VersionedControllerService createControllerService(final String identifier, final String groupIdentifier) {
        final VersionedControllerService service = new VersionedControllerService();
        service.setIdentifier(identifier);
        service.setGroupIdentifier(groupIdentifier);
        service.setName(LOCAL_SERVICE_NAME);
        service.setType(LOCAL_SERVICE_TYPE);
        service.setBundle(new Bundle(BUNDLE_GROUP, BUNDLE_ARTIFACT, BUNDLE_VERSION));
        service.setScheduledState(ScheduledState.DISABLED);
        service.setComments(LOCAL_COMMENTS);

        final Map<String, String> properties = new HashMap<>();
        properties.put(SERVICE_ENABLED_PROPERTY, SERVICE_ENABLED_VALUE);
        service.setProperties(properties);

        final Map<String, VersionedPropertyDescriptor> descriptors = new HashMap<>();
        descriptors.put(SERVICE_ENABLED_PROPERTY, createDescriptor(SERVICE_ENABLED_PROPERTY, false, false));
        service.setPropertyDescriptors(descriptors);
        return service;
    }

    private VersionedPropertyDescriptor createDescriptor(final String name, final boolean dynamic, final boolean sensitive) {
        final VersionedPropertyDescriptor descriptor = new VersionedPropertyDescriptor();
        descriptor.setName(name);
        descriptor.setDynamic(dynamic);
        descriptor.setSensitive(sensitive);
        return descriptor;
    }
}
