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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StandardControllerServiceProviderTest {

    private ControllerService proxied;
    private ControllerService implementation;
    private static ExtensionDiscoveringManager extensionManager;
    private static Bundle systemBundle;

    @BeforeAll
    public static void setupSuite() {
        final NiFiProperties nifiProperties = NiFiProperties.createBasicNiFiProperties(StandardControllerServiceProviderTest.class.getResource("/conf/nifi.properties").getFile());

        // load the system bundle
        systemBundle = SystemBundle.create(nifiProperties);
        extensionManager = new StandardExtensionDiscoveringManager();
        extensionManager.discoverExtensions(systemBundle, Collections.emptySet());
    }

    @BeforeEach
    public void setup() throws Exception {
        String id = "id";
        String clazz = "org.apache.nifi.controller.service.util.TestControllerService";
        ControllerServiceProvider provider = new StandardControllerServiceProvider(null, null, Mockito.mock(FlowManager.class), Mockito.mock(ExtensionManager.class));
        ControllerServiceNode node = createControllerService(clazz, id, systemBundle.getBundleDetails().getCoordinate(), provider);
        proxied = node.getProxiedControllerService();
        implementation = node.getControllerServiceImplementation();
    }

    private ControllerServiceNode createControllerService(final String type, final String id, final BundleCoordinate bundleCoordinate, final ControllerServiceProvider serviceProvider) {
        return new ExtensionBuilder()
            .identifier(id)
            .type(type)
            .bundleCoordinate(bundleCoordinate)
            .controllerServiceProvider(serviceProvider)
            .processScheduler(Mockito.mock(ProcessScheduler.class))
            .nodeTypeProvider(Mockito.mock(NodeTypeProvider.class))
            .validationTrigger(Mockito.mock(ValidationTrigger.class))
            .reloadComponent(Mockito.mock(ReloadComponent.class))
            .stateManagerProvider(Mockito.mock(StateManagerProvider.class))
            .extensionManager(extensionManager)
            .buildControllerService();
    }

    @Test
    public void testCallProxiedOnPropertyModified() {
        assertThrows(UnsupportedOperationException.class,
                () -> proxied.onPropertyModified(null, "oldValue", "newValue"));
    }

    @Test
    public void testCallImplementationOnPropertyModified() {
        implementation.onPropertyModified(null, "oldValue", "newValue");
    }

    @Test
    public void testCallProxiedInitialized() throws InitializationException {
        assertThrows(UnsupportedOperationException.class,
                () -> proxied.initialize(null));
    }

    @Test
    public void testCallImplementationInitialized() throws InitializationException {
        implementation.initialize(null);
    }

    private ControllerServiceNode populateControllerService(ControllerServiceNode requiredService) { // Collection<ControllerServiceNode> serviceNodes) {
        ControllerServiceNode controllerServiceNode = mock(ControllerServiceNode.class);
        ArrayList<ControllerServiceNode> requiredServices = new ArrayList<>();
        if (requiredService != null) {
            requiredServices.add(requiredService);
        }
        when(controllerServiceNode.getRequiredControllerServices()).thenReturn(requiredServices);
        return controllerServiceNode;
    }

    @Test
    public void testEnableControllerServicesAllAreEnabled() {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        future.complete(null);

        ProcessScheduler scheduler = Mockito.mock(ProcessScheduler.class);
        when(scheduler.enableControllerService(any())).thenReturn(future);
        ControllerServiceProvider provider = new StandardControllerServiceProvider(scheduler, null, Mockito.mock(FlowManager.class), Mockito.mock(ExtensionManager.class));

        final ArrayList<ControllerServiceNode> serviceNodes = new ArrayList<>();
        serviceNodes.add(populateControllerService(null));
        serviceNodes.add(populateControllerService(null));
        provider.enableControllerServices(serviceNodes);
        verify(scheduler).enableControllerService(serviceNodes.get(0));
        verify(scheduler).enableControllerService(serviceNodes.get(1));
    }

    @Test
    public void testEnableControllerServicesSomeAreEnabled() {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        future.complete(null);

        ProcessScheduler scheduler = Mockito.mock(ProcessScheduler.class);
        when(scheduler.enableControllerService(any())).thenReturn(future);
        ControllerServiceProvider provider = new StandardControllerServiceProvider(scheduler, null, Mockito.mock(FlowManager.class), Mockito.mock(ExtensionManager.class));

        final ArrayList<ControllerServiceNode> serviceNodes = new ArrayList<>();
        ControllerServiceNode disabledController = populateControllerService(null);
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
        provider.enableControllerServices(serviceNodes);
        verify(scheduler, Mockito.times(0)).enableControllerService(serviceNodes.get(0));
        verify(scheduler, Mockito.times(2)).enableControllerService(serviceNodes.get(1));
        verify(scheduler, Mockito.times(0)).enableControllerService(serviceNodes.get(2));
        verify(scheduler, Mockito.times(0)).enableControllerService(serviceNodes.get(3));
        verify(scheduler, Mockito.times(1)).enableControllerService(serviceNodes.get(4));
    }
}
