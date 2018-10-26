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
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.components.validation.ValidationTrigger;
import org.apache.nifi.controller.ExtensionBuilder;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.NodeTypeProvider;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.ReloadComponent;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.scheduling.StandardProcessScheduler;
import org.apache.nifi.controller.service.mock.MockProcessGroup;
import org.apache.nifi.controller.service.mock.ServiceA;
import org.apache.nifi.controller.service.mock.ServiceB;
import org.apache.nifi.engine.FlowEngine;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.nar.ExtensionDiscoveringManager;
import org.apache.nifi.nar.StandardExtensionDiscoveringManager;
import org.apache.nifi.nar.SystemBundle;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.util.NiFiProperties;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.beans.PropertyDescriptor;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertTrue;

public class StandardControllerServiceProviderIT {
    private static Bundle systemBundle;
    private static NiFiProperties niFiProperties;
    private static ExtensionDiscoveringManager extensionManager;
    private static VariableRegistry variableRegistry = VariableRegistry.ENVIRONMENT_SYSTEM_REGISTRY;

    private static StateManagerProvider stateManagerProvider = new StateManagerProvider() {
        @Override
        public StateManager getStateManager(final String componentId) {
            return Mockito.mock(StateManager.class);
        }

        @Override
        public void shutdown() {
        }

        @Override
        public void enableClusterProvider() {
        }

        @Override
        public void disableClusterProvider() {
        }

        @Override
        public void onComponentRemoved(final String componentId) {
        }
    };

    @BeforeClass
    public static void setNiFiProps() {
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, TestStandardControllerServiceProvider.class.getResource("/conf/nifi.properties").getFile());
        niFiProperties = NiFiProperties.createBasicNiFiProperties(null, null);

        // load the system bundle
        systemBundle = SystemBundle.create(niFiProperties);
        extensionManager = new StandardExtensionDiscoveringManager();
        extensionManager.discoverExtensions(systemBundle, Collections.emptySet());
    }

    /**
     * We run the same test 1000 times and prior to bug fix (see NIFI-1143) it
     * would fail on some iteration. For more details please see
     * {@link PropertyDescriptor}.isDependentServiceEnableable() as well as
     * https://issues.apache.org/jira/browse/NIFI-1143
     */
    @Test(timeout = 120000)
    public void testConcurrencyWithEnablingReferencingServicesGraph() throws InterruptedException, ExecutionException {
        final StandardProcessScheduler scheduler = new StandardProcessScheduler(new FlowEngine(1, "Unit Test", true), Mockito.mock(FlowController.class),
            null, stateManagerProvider, niFiProperties);

        for (int i = 0; i < 5000; i++) {
            testEnableReferencingServicesGraph(scheduler);
        }
    }

    private ControllerServiceNode createControllerService(final String type, final String id, final BundleCoordinate bundleCoordinate, final ControllerServiceProvider serviceProvider) {
        final ControllerServiceNode serviceNode = new ExtensionBuilder()
            .identifier(id)
            .type(type)
            .bundleCoordinate(bundleCoordinate)
            .controllerServiceProvider(serviceProvider)
            .processScheduler(Mockito.mock(ProcessScheduler.class))
            .nodeTypeProvider(Mockito.mock(NodeTypeProvider.class))
            .validationTrigger(Mockito.mock(ValidationTrigger.class))
            .reloadComponent(Mockito.mock(ReloadComponent.class))
            .variableRegistry(variableRegistry)
            .stateManagerProvider(Mockito.mock(StateManagerProvider.class))
            .extensionManager(extensionManager)
            .buildControllerService();

        serviceProvider.onControllerServiceAdded(serviceNode);

        return serviceNode;
    }

    public void testEnableReferencingServicesGraph(final StandardProcessScheduler scheduler) throws InterruptedException, ExecutionException {
        final FlowController controller = Mockito.mock(FlowController.class);

        final ProcessGroup procGroup = new MockProcessGroup(controller);
        final FlowManager flowManager = Mockito.mock(FlowManager.class);
        Mockito.when(controller.getFlowManager()).thenReturn(flowManager);

        Mockito.when(flowManager.getGroup(Mockito.anyString())).thenReturn(procGroup);
        Mockito.when(controller.getExtensionManager()).thenReturn(extensionManager);

        final ControllerServiceProvider serviceProvider = new StandardControllerServiceProvider(controller, scheduler, null);

        // build a graph of controller services with dependencies as such:
        //
        // A -> B -> D
        // C ---^----^
        //
        // In other words, A references B, which references D.
        // AND
        // C references B and D.
        //
        // So we have to verify that if D is enabled, when we enable its referencing services,
        // we enable C and B, even if we attempt to enable C before B... i.e., if we try to enable C, we cannot do so
        // until B is first enabled so ensure that we enable B first.
        final ControllerServiceNode serviceNode1 = createControllerService(ServiceA.class.getName(), "1", systemBundle.getBundleDetails().getCoordinate(), serviceProvider);
        final ControllerServiceNode serviceNode2 = createControllerService(ServiceA.class.getName(), "2", systemBundle.getBundleDetails().getCoordinate(), serviceProvider);
        final ControllerServiceNode serviceNode3 = createControllerService(ServiceA.class.getName(), "3", systemBundle.getBundleDetails().getCoordinate(), serviceProvider);
        final ControllerServiceNode serviceNode4 = createControllerService(ServiceB.class.getName(), "4", systemBundle.getBundleDetails().getCoordinate(), serviceProvider);

        procGroup.addControllerService(serviceNode1);
        procGroup.addControllerService(serviceNode2);
        procGroup.addControllerService(serviceNode3);
        procGroup.addControllerService(serviceNode4);

        setProperty(serviceNode1, ServiceA.OTHER_SERVICE.getName(), "2");
        setProperty(serviceNode2, ServiceA.OTHER_SERVICE.getName(), "4");
        setProperty(serviceNode3, ServiceA.OTHER_SERVICE.getName(), "2");
        setProperty(serviceNode3, ServiceA.OTHER_SERVICE_2.getName(), "4");

        serviceNode4.performValidation();
        serviceProvider.enableControllerService(serviceNode4).get();
        serviceProvider.enableReferencingServices(serviceNode4);

        // Verify that the services are either ENABLING or ENABLED, and wait for all of them to become ENABLED.
        // Note that we set a timeout of 10 seconds, in case a bug occurs and the services never become ENABLED.
        final Set<ControllerServiceState> validStates = new HashSet<>();
        validStates.add(ControllerServiceState.ENABLED);
        validStates.add(ControllerServiceState.ENABLING);

        while (serviceNode3.getState() != ControllerServiceState.ENABLED || serviceNode2.getState() != ControllerServiceState.ENABLED || serviceNode1.getState() != ControllerServiceState.ENABLED) {
            assertTrue(validStates.contains(serviceNode3.getState()));
            assertTrue(validStates.contains(serviceNode2.getState()));
            assertTrue(validStates.contains(serviceNode1.getState()));
        }
    }

    private void setProperty(ControllerServiceNode serviceNode, String propName, String propValue) {
        Map<String, String> props = new LinkedHashMap<>();
        props.put(propName, propValue);
        serviceNode.setProperties(props);
    }
}
