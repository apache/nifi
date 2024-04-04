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
import org.apache.nifi.controller.ComponentNode;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.nar.ExtensionManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
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
    private ControllerServiceProvider serviceProvider;

    @BeforeEach
    public void setup() {
        final ExtensionManager extensionManager = mock(ExtensionManager.class);
        final FlowManager flowManager = mock(FlowManager.class);
        creator = mock(ComponentNode.class);

        bundles = new ArrayList<>();
        bundles.add(createBundle(LONE_BUNDLE, VERSION_2));
        when(extensionManager.getBundles(IMPL_CLASS)).thenAnswer(invocation -> bundles);

        serviceProvider = mock(ControllerServiceProvider.class);

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
        Assertions.assertThrows(IllegalArgumentException.class, () -> factory.getCreationDetails(IMPL_CLASS, serviceProperties));

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
        Assertions.assertThrows(IllegalArgumentException.class, () -> factory.getCreationDetails(IMPL_CLASS, serviceProperties));

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
        for (int i=0; i < 5; i++) {
            assertEquals(initialServiceId, factory.determineServiceId(IMPL_CLASS, serviceProperties));
        }

        // Service ID should change if the component's group changes
        when(creator.getProcessGroupIdentifier()).thenReturn("new-id");
        final String secondGroupId = factory.determineServiceId(IMPL_CLASS, serviceProperties);
        assertNotNull(secondGroupId);

        // Ensure that with the same parameters we keep getting the same value
        for (int i=0; i < 5; i++) {
            assertEquals(secondGroupId, factory.determineServiceId(IMPL_CLASS, serviceProperties));
        }

        final String thirdId = factory.determineServiceId(IMPL_CLASS, Map.of());
        assertNotNull(thirdId);

        final String fourthId = factory.determineServiceId(IMPL_CLASS, Map.of("Another", "Value"));
        assertNotNull(fourthId);

        // Assert all IDs are unique
        assertEquals(4, Set.of(initialServiceId, secondGroupId, thirdId, fourthId).size());
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
