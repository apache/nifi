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

package org.apache.nifi.migration;

import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.bundle.BundleDetails;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ComponentNode;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.nar.ExtensionManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestStandardControllerServiceFactory {
    private static final String VERSION_2 = "2.0.0";
    private static final String FRAMEWORK_VERSION = "8.7.6";
    private static final String GROUP_ID = "org.apache.nifi";
    private static final String LONE_BUNDLE = "lone-bundle";
    private static final String IMPL_CLASS = "org.apache.nifi.auth.AuthorizerService";

    private StandardControllerServiceFactory factory;
    private List<Bundle> bundles;
    private ComponentNode creator;

    @BeforeEach
    public void setup() {
        final ExtensionManager extensionManager = mock(ExtensionManager.class);
        final FlowManager flowManager = mock(FlowManager.class);
        creator = mock(ComponentNode.class);

        bundles = new ArrayList<>();
        bundles.add(createBundle(LONE_BUNDLE, VERSION_2));
        when(extensionManager.getBundles(IMPL_CLASS)).thenAnswer(invocation -> bundles);

        final ControllerServiceProvider serviceProvider = mock(ControllerServiceProvider.class);

        final Bundle frameworkBundle = createBundle("framework-nar", FRAMEWORK_VERSION);
        factory = new StandardControllerServiceFactory(extensionManager, flowManager, serviceProvider, creator) {
            @Override
            protected Bundle getFrameworkBundle() {
                return frameworkBundle;
            }
        };
    }

    @Test
    public void testBundleDetermination() {
        final Map<String, String> serviceProperties = Map.of("PropertyA", "ValueA");
        final ControllerServiceCreationDetails details = factory.getCreationDetails(IMPL_CLASS, serviceProperties);
        assertNotNull(details);
        assertEquals(IMPL_CLASS, details.type());
        assertEquals(serviceProperties, details.serviceProperties());

        // Test lone bundle
        final BundleCoordinate coordinate = details.serviceBundleCoordinate();
        assertEquals("%s:%s:%s".formatted(GROUP_ID, LONE_BUNDLE, VERSION_2), coordinate.getCoordinate());

        // Test no bundles
        bundles.clear();
        assertThrows(IllegalArgumentException.class, () -> factory.getCreationDetails(IMPL_CLASS, serviceProperties));

        // Test matching creator bundle
        final BundleCoordinate coordinateA = createCoordinate("bundle-A", VERSION_2);
        final BundleCoordinate coordinateB = createCoordinate("bundle-B", VERSION_2);
        when(creator.getBundleCoordinate()).thenReturn(coordinateB);
        bundles.add(createBundle(coordinateA));
        bundles.add(createBundle(coordinateB));
        assertEquals(coordinateB, factory.getCreationDetails(IMPL_CLASS, serviceProperties).serviceBundleCoordinate());

        // Test matching creator version with two options
        final BundleCoordinate coordinateC = createCoordinate("bundle-C", VERSION_2);
        when(creator.getBundleCoordinate()).thenReturn(coordinateC);
        assertThrows(IllegalArgumentException.class, () -> factory.getCreationDetails(IMPL_CLASS, serviceProperties));

        // Test matching creator version with only 1 option
        bundles.remove(createBundle(coordinateB));
        assertEquals(coordinateA, factory.getCreationDetails(IMPL_CLASS, serviceProperties).serviceBundleCoordinate());

        bundles.clear();
        final BundleCoordinate frameworkVersionCoordinate = createCoordinate("bundle-X", FRAMEWORK_VERSION);
        bundles.add(createBundle(frameworkVersionCoordinate));
        assertEquals(frameworkVersionCoordinate, factory.getCreationDetails(IMPL_CLASS, serviceProperties).serviceBundleCoordinate());
    }

    @Test
    public void testServiceIdDeterministic() {
        final Map<String, String> serviceProperties = Map.of("PropertyA", "ValueA");
        final String initialServiceId = factory.determineServiceId(IMPL_CLASS, serviceProperties);
        assertNotNull(initialServiceId);

        // Create the service several times, ensuring that the same ID is returned each time.
        for (int i = 0; i < 5; i++) {
            assertEquals(initialServiceId, factory.determineServiceId(IMPL_CLASS, serviceProperties));
        }

        // Service ID should change if the component's group changes
        when(creator.getProcessGroupIdentifier()).thenReturn("new-id");
        final String secondGroupId = factory.determineServiceId(IMPL_CLASS, serviceProperties);
        assertNotNull(secondGroupId);

        // Ensure that with the same parameters we keep getting the same value
        for (int i = 0; i < 5; i++) {
            assertEquals(secondGroupId, factory.determineServiceId(IMPL_CLASS, serviceProperties));
        }

        final String thirdId = factory.determineServiceId(IMPL_CLASS, Map.of());
        assertNotNull(thirdId);

        final String fourthId = factory.determineServiceId(IMPL_CLASS, Map.of("Another", "Value"));
        assertNotNull(fourthId);

        // Assert all IDs are unique
        assertEquals(4, Set.of(initialServiceId, secondGroupId, thirdId, fourthId).size());
    }

    @Nested
    class VersionedIdAssignment {
        private static final String ENDPOINT_PROPERTY = "Endpoint";
        private static final String ENDPOINT_PARAMETER = "#{endpoint}";
        private static final String MODE_PROPERTY = "Mode";
        private static final String READ_MODE = "read";
        private static final String PASSWORD_PROPERTY = "Password";

        private static final String OWNING_GROUP_IDENTITY = "shared-version";

        private static final String CONSUMER_GROUP_ID = "consumer-instance";

        @Test
        void testVersionedIdentityDependsOnSuppliedIdentityTypeAndProperties() {
            final Map<String, String> properties = new LinkedHashMap<>();
            properties.put(ENDPOINT_PROPERTY, ENDPOINT_PARAMETER);
            properties.put(MODE_PROPERTY, READ_MODE);
            final String first = createServiceAndGetVersionedId("author-instance", OWNING_GROUP_IDENTITY, IMPL_CLASS, properties);

            final Map<String, String> reordered = new LinkedHashMap<>();
            reordered.put(MODE_PROPERTY, READ_MODE);
            reordered.put(ENDPOINT_PROPERTY, ENDPOINT_PARAMETER);
            final String second = createServiceAndGetVersionedId(CONSUMER_GROUP_ID, OWNING_GROUP_IDENTITY, IMPL_CLASS, reordered);

            assertNotNull(first);
            assertEquals(first, second);
            assertNotEquals(first, createServiceAndGetVersionedId(CONSUMER_GROUP_ID, "different-version", IMPL_CLASS, properties));
            assertNotEquals(first, createServiceAndGetVersionedId(CONSUMER_GROUP_ID, OWNING_GROUP_IDENTITY, "AnotherService", properties));
            assertNotEquals(first, createServiceAndGetVersionedId(CONSUMER_GROUP_ID, OWNING_GROUP_IDENTITY, IMPL_CLASS, Map.of()));
        }

        @Test
        void testSensitiveValuesDoNotAffectVersionedIdentity() {
            final Map<String, String> authorProperties = Map.of(ENDPOINT_PROPERTY, ENDPOINT_PARAMETER, PASSWORD_PROPERTY, "author-password");
            final Map<String, String> consumerProperties = Map.of(ENDPOINT_PROPERTY, ENDPOINT_PARAMETER, PASSWORD_PROPERTY, "consumer-password");
            final String authorId = createServiceAndGetVersionedId("author-instance", OWNING_GROUP_IDENTITY, IMPL_CLASS, authorProperties);
            final String consumerId = createServiceAndGetVersionedId(CONSUMER_GROUP_ID, OWNING_GROUP_IDENTITY, IMPL_CLASS, consumerProperties);

            assertEquals(authorId, consumerId);
            assertEquals(authorId, createServiceAndGetVersionedId(CONSUMER_GROUP_ID, OWNING_GROUP_IDENTITY, IMPL_CLASS, Map.of(ENDPOINT_PROPERTY, ENDPOINT_PARAMETER)));
            assertNotEquals(authorId, createServiceAndGetVersionedId(CONSUMER_GROUP_ID, OWNING_GROUP_IDENTITY, IMPL_CLASS,
                    Map.of(ENDPOINT_PROPERTY, "another-endpoint", PASSWORD_PROPERTY, "consumer-password")));
            assertNotEquals(factory.determineServiceId(IMPL_CLASS, authorProperties), factory.determineServiceId(IMPL_CLASS, consumerProperties));
        }

        @Test
        void testOccupiedVersionedIdentityLeavesNewServiceUnversioned() {
            final Map<String, String> properties = Map.of(ENDPOINT_PROPERTY, ENDPOINT_PARAMETER);
            final String occupiedId = createServiceAndGetVersionedId("author-instance", OWNING_GROUP_IDENTITY, IMPL_CLASS, properties);
            final ControllerServiceNode existingService = mock(ControllerServiceNode.class);
            when(existingService.getVersionedComponentId()).thenReturn(Optional.of(occupiedId));

            final ControllerServiceNode newService = createService(CONSUMER_GROUP_ID, OWNING_GROUP_IDENTITY, IMPL_CLASS, properties, Set.of(existingService));

            verify(newService, never()).setVersionedComponentId(any());
            verify(existingService, never()).setVersionedComponentId(any());
            verify(newService).migrateConfiguration(eq(properties), any());
        }

        @Test
        void testUnversionedAndDifferentVersionedServicesDoNotOccupyIdentity() {
            final Map<String, String> properties = Map.of(ENDPOINT_PROPERTY, ENDPOINT_PARAMETER);
            final String expectedId = createServiceAndGetVersionedId("author-instance", OWNING_GROUP_IDENTITY, IMPL_CLASS, properties);
            final ControllerServiceNode unversionedService = mock(ControllerServiceNode.class);
            when(unversionedService.getVersionedComponentId()).thenReturn(Optional.empty());
            final ControllerServiceNode differentlyVersionedService = mock(ControllerServiceNode.class);
            when(differentlyVersionedService.getVersionedComponentId()).thenReturn(Optional.of("another-versioned-id"));

            final ControllerServiceNode newService = createService(CONSUMER_GROUP_ID, OWNING_GROUP_IDENTITY, IMPL_CLASS, properties,
                    Set.of(unversionedService, differentlyVersionedService));

            verify(newService).setVersionedComponentId(expectedId);
            verify(unversionedService, never()).setVersionedComponentId(any());
            verify(differentlyVersionedService, never()).setVersionedComponentId(any());
        }

        @Test
        void testCreationWithoutOwningGroupIdentity() {
            final ComponentNode creator = mock(ComponentNode.class);
            final FlowManager flowManager = mock(FlowManager.class);
            final StandardControllerServiceFactory factory = new StandardControllerServiceFactory(
                    mock(ExtensionManager.class), flowManager, mock(ControllerServiceProvider.class), creator);
            final ControllerServiceNode service = mock(ControllerServiceNode.class);
            final ControllerServiceCreationDetails details = new ControllerServiceCreationDetails("service-id", IMPL_CLASS,
                    createCoordinate(LONE_BUNDLE, VERSION_2), Map.of(), ControllerServiceCreationDetails.CreationState.SERVICE_TO_BE_CREATED);
            when(flowManager.createControllerService(eq(IMPL_CLASS), anyString(), any(), any(), eq(true), eq(true), eq(null))).thenReturn(service);
            when(creator.getParentProcessGroup()).thenReturn(Optional.empty());

            factory.create(details);

            verify(flowManager).addRootControllerService(service);
            verify(service, never()).setVersionedComponentId(any());

            final ProcessGroup group = mock(ProcessGroup.class);
            when(creator.getParentProcessGroup()).thenReturn(Optional.of(group));

            factory.create(details);

            verify(group).addControllerService(service);
            verify(service, never()).setVersionedComponentId(any());
        }

        private String createServiceAndGetVersionedId(final String localGroupId, final String owningGroupIdentity, final String type, final Map<String, String> properties) {
            final ControllerServiceNode service = createService(localGroupId, owningGroupIdentity, type, properties, Set.of());
            final ArgumentCaptor<String> versionedId = ArgumentCaptor.forClass(String.class);
            verify(service).setVersionedComponentId(versionedId.capture());
            return versionedId.getValue();
        }

        private ControllerServiceNode createService(final String localGroupId, final String owningGroupIdentity, final String type,
                                                   final Map<String, String> properties, final Set<ControllerServiceNode> existingServices) {
            final ComponentNode component = mock(ComponentNode.class);
            final ProcessGroup group = mock(ProcessGroup.class);
            when(component.getParentProcessGroup()).thenReturn(Optional.of(group));
            when(component.getProcessGroupIdentifier()).thenReturn(localGroupId);
            final FlowManager manager = mock(FlowManager.class);
            final ControllerServiceNode service = mock(ControllerServiceNode.class);
            when(service.getPropertyDescriptor(anyString())).thenAnswer(invocation -> {
                final String name = invocation.getArgument(0);
                return new PropertyDescriptor.Builder().name(name).sensitive(PASSWORD_PROPERTY.equals(name)).build();
            });
            final Set<ControllerServiceNode> groupServices = new HashSet<>(existingServices);
            groupServices.add(service);
            when(group.getControllerServices(false)).thenReturn(groupServices);
            when(manager.createControllerService(eq(type), anyString(), any(), any(), eq(true), eq(true), eq(null))).thenReturn(service);

            final StandardControllerServiceFactory serviceFactory = new StandardControllerServiceFactory(
                    mock(ExtensionManager.class), manager, mock(ControllerServiceProvider.class), component, owningGroupIdentity);
            final String instanceId = serviceFactory.determineServiceId(type, properties);
            final ControllerServiceCreationDetails details = new ControllerServiceCreationDetails(instanceId, type, createCoordinate(LONE_BUNDLE, VERSION_2), properties,
                    ControllerServiceCreationDetails.CreationState.SERVICE_TO_BE_CREATED);
            serviceFactory.create(details);

            verify(service).setProperties(properties);
            return service;
        }
    }

    private BundleCoordinate createCoordinate(final String artifactId, final String version) {
        return new BundleCoordinate(GROUP_ID, artifactId, version);
    }

    private Bundle createBundle(final BundleCoordinate coordinate) {
        final BundleDetails details = new BundleDetails.Builder()
                .coordinate(coordinate)
                .workingDir(new File("target/work"))
                .build();
        return new Bundle(details, getClass().getClassLoader());
    }

    private Bundle createBundle(final String artifactId, final String version) {
        return createBundle(createCoordinate(artifactId, version));
    }
}
