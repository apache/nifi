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
package org.apache.nifi.controller.service;

import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.components.validation.ValidationTrigger;
import org.apache.nifi.components.validation.VerifiableComponentFactory;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ExtensionBuilder;
import org.apache.nifi.controller.NodeTypeProvider;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.ReloadComponent;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.nar.ExtensionDiscoveringManager;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.StandardExtensionDiscoveringManager;
import org.apache.nifi.nar.SystemBundle;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.NiFiProperties;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StandardControllerServiceProviderTest {

    private static final String NAR_LIBRARY_DIRECTORY = "./target/lib";

    private static final String TEST_SERVICE_CLASS = "org.apache.nifi.controller.service.util.TestControllerService";
    private static final String PRIMARY_SERVICE_CLASS = "org.apache.nifi.controller.service.mock.PrimaryService";
    private static final String SECONDARY_SERVICE_CLASS = "org.apache.nifi.controller.service.mock.SecondaryService";

    private static final String SECONDARY_SERVICE_ENABLED_PROPERTY = "Secondary Service Enabled";
    private static final String SECONDARY_SERVICE_PROPERTY = "Secondary Service";

    private static final String SERVICE_ID = "service-id";
    private static final String SECONDARY_SERVICE_ID = "secondary-service-id";

    private static ExtensionDiscoveringManager extensionManager;
    private static Bundle systemBundle;

    @Mock
    private ProcessScheduler scheduler;

    @Mock
    private FlowManager flowManager;

    private ControllerServiceProvider serviceProvider;

    private ControllerService proxied;
    private ControllerService implementation;

    @BeforeAll
    static void setupSuite() {
        final Map<String, String> additionalProperties = Map.of(
                NiFiProperties.NAR_LIBRARY_DIRECTORY, NAR_LIBRARY_DIRECTORY
        );
        final NiFiProperties nifiProperties = NiFiProperties.createBasicNiFiProperties(null, additionalProperties);

        systemBundle = SystemBundle.create(nifiProperties);
        extensionManager = new StandardExtensionDiscoveringManager();
        extensionManager.discoverExtensions(systemBundle, Collections.emptySet());
    }

    @BeforeEach
    void setup() {
        serviceProvider = new StandardControllerServiceProvider(scheduler, null, flowManager, mock(ExtensionManager.class));
        final ControllerServiceNode node = createControllerService(SERVICE_ID, TEST_SERVICE_CLASS, systemBundle.getBundleDetails().getCoordinate(), serviceProvider);
        proxied = node.getProxiedControllerService();
        implementation = node.getControllerServiceImplementation();
    }

    @Test
    void testCallProxiedOnPropertyModified() {
        assertThrows(UnsupportedOperationException.class,
                () -> proxied.onPropertyModified(null, "oldValue", "newValue"));
    }

    @Test
    void testCallImplementationOnPropertyModified() {
        implementation.onPropertyModified(null, "oldValue", "newValue");
    }

    @Test
    void testCallProxiedInitialized() {
        assertThrows(UnsupportedOperationException.class,
                () -> proxied.initialize(null));
    }

    @Test
    void testCallImplementationInitialized() throws InitializationException {
        implementation.initialize(null);
    }

    @Test
    void testEnableControllerServicesAllAreEnabled() {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        future.complete(null);

        when(scheduler.enableControllerService(any())).thenReturn(future);

        final List<ControllerServiceNode> serviceNodes = new ArrayList<>();
        serviceNodes.add(populateControllerService(null));
        serviceNodes.add(populateControllerService(null));
        serviceProvider.enableControllerServices(serviceNodes);
        verify(scheduler).enableControllerService(serviceNodes.get(0));
        verify(scheduler).enableControllerService(serviceNodes.get(1));
    }

    @Test
    void testEnableControllerServicesSomeAreEnabled() {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        future.complete(null);
        when(scheduler.enableControllerService(any())).thenReturn(future);

        final List<ControllerServiceNode> serviceNodes = new ArrayList<>();
        final ControllerServiceNode disabledController = populateControllerService(null);
        // Do not start because disabledController is not in the serviceNodes (list of services to start)
        serviceNodes.add(populateControllerService(disabledController));
        // Start this service because it has no required services
        serviceNodes.add(populateControllerService(null));
        // Do not start because its required service is not started because the required service
        // depends on disabledController which is not in the serviceNodes (list of services to start)
        serviceNodes.add(populateControllerService(serviceNodes.get(0)));
        // Do not start because its required service depends on disabledController through 2 other levels of services
        serviceNodes.add(populateControllerService(serviceNodes.get(2)));
        // Start this service because it has a required service which is in the list of services to start
        serviceNodes.add(populateControllerService(serviceNodes.get(1)));

        serviceProvider.enableControllerServices(serviceNodes);

        verify(scheduler, times(0)).enableControllerService(serviceNodes.get(0));
        verify(scheduler, times(2)).enableControllerService(serviceNodes.get(1));
        verify(scheduler, times(0)).enableControllerService(serviceNodes.get(2));
        verify(scheduler, times(0)).enableControllerService(serviceNodes.get(3));
        verify(scheduler, times(1)).enableControllerService(serviceNodes.get(4));
    }

    @Timeout(10)
    @Test
    void testEnableControllerServicesDependencyNotEnabled() throws InterruptedException {
        final BundleCoordinate systemCoordinate = systemBundle.getBundleDetails().getCoordinate();
        final ControllerServiceNode primaryServiceNode = createControllerService(SERVICE_ID, PRIMARY_SERVICE_CLASS, systemCoordinate, serviceProvider);
        serviceProvider.onControllerServiceAdded(primaryServiceNode);

        final ControllerServiceNode secondaryServiceNode = createControllerService(SECONDARY_SERVICE_ID, SECONDARY_SERVICE_CLASS, systemCoordinate, serviceProvider);
        serviceProvider.onControllerServiceAdded(secondaryServiceNode);

        final Map<String, String> primaryProperties = Map.of(
                SECONDARY_SERVICE_ENABLED_PROPERTY, Boolean.FALSE.toString(),
                SECONDARY_SERVICE_PROPERTY, SECONDARY_SERVICE_ID
        );
        primaryServiceNode.setProperties(primaryProperties);
        final List<ControllerServiceNode> serviceNodes = List.of(primaryServiceNode);

        // Enable Primary Service using Count Down Latch to signal completion
        final CountDownLatch enabledLatch = new CountDownLatch(1);
        when(scheduler.enableControllerService(eq(primaryServiceNode))).then(invocationOnMock -> {
            try (ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor()) {
                final CompletableFuture<Void> nodeEnabledFuture = primaryServiceNode.enable(executorService, 5000, true);
                nodeEnabledFuture.join();
                enabledLatch.countDown();
            }
            final CompletableFuture<Void> enableFuture = new CompletableFuture<>();
            enableFuture.complete(null);
            return enableFuture;
        });

        serviceProvider.enableControllerServices(serviceNodes);

        enabledLatch.await();
        assertEquals(ControllerServiceState.ENABLED, primaryServiceNode.getState());

        assertEquals(ControllerServiceState.DISABLED, secondaryServiceNode.getState(), "Secondary Service should remain disabled");
    }

    private ControllerServiceNode createControllerService(
            final String identifier,
            final String type,
            final BundleCoordinate bundleCoordinate,
            final ControllerServiceProvider serviceProvider
    ) {
        return new ExtensionBuilder()
                .identifier(identifier)
                .type(type)
                .bundleCoordinate(bundleCoordinate)
                .controllerServiceProvider(serviceProvider)
                .processScheduler(mock(ProcessScheduler.class))
                .nodeTypeProvider(mock(NodeTypeProvider.class))
                .validationTrigger(mock(ValidationTrigger.class))
                .reloadComponent(mock(ReloadComponent.class))
                .verifiableComponentFactory(mock(VerifiableComponentFactory.class))
                .stateManagerProvider(mock(StateManagerProvider.class))
                .extensionManager(extensionManager)
                .buildControllerService();
    }

    private ControllerServiceNode populateControllerService(final ControllerServiceNode requiredService) {
        final ControllerServiceNode controllerServiceNode = mock(ControllerServiceNode.class);
        if (requiredService != null) {
            final List<ControllerServiceNode> requiredServices = List.of(requiredService);
            when(controllerServiceNode.getRequiredControllerServices()).thenReturn(requiredServices);
        }
        return controllerServiceNode;
    }
}
