
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
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.components.validation.ValidationTrigger;
import org.apache.nifi.controller.ExtensionBuilder;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.LoggableComponent;
import org.apache.nifi.controller.NodeTypeProvider;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReloadComponent;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.StandardProcessorNode;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.scheduling.StandardLifecycleStateManager;
import org.apache.nifi.controller.scheduling.StandardProcessScheduler;
import org.apache.nifi.controller.service.mock.DummyProcessor;
import org.apache.nifi.controller.service.mock.MockProcessGroup;
import org.apache.nifi.controller.service.mock.ServiceA;
import org.apache.nifi.controller.service.mock.ServiceB;
import org.apache.nifi.controller.service.mock.ServiceC;
import org.apache.nifi.controller.state.StandardStateMap;
import org.apache.nifi.engine.FlowEngine;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.nar.ExtensionDiscoveringManager;
import org.apache.nifi.nar.StandardExtensionDiscoveringManager;
import org.apache.nifi.nar.SystemBundle;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.StandardValidationContextFactory;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.MockProcessorInitializationContext;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.SynchronousValidationTrigger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;

public class TestStandardControllerServiceProvider {

    private static StateManagerProvider stateManagerProvider = new StateManagerProvider() {
        @Override
        public StateManager getStateManager(final String componentId, final boolean dropStateKeySupported) {
            final StateManager stateManager = Mockito.mock(StateManager.class);
            final StateMap emptyStateMap = new StandardStateMap(Collections.emptyMap(), Optional.empty());
            try {
                Mockito.when(stateManager.getState(any(Scope.class))).thenReturn(emptyStateMap);
            } catch (IOException e) {
                throw new AssertionError();
            }

            return stateManager;
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

        @Override
        public boolean isClusterProviderEnabled() {
            return false;
        }
    };

    private static NiFiProperties niFiProperties;
    private static ExtensionDiscoveringManager extensionManager;
    private static Bundle systemBundle;
    private FlowManager flowManager;

    @BeforeAll
    public static void setNiFiProps() {
        niFiProperties = NiFiProperties.createBasicNiFiProperties(TestStandardControllerServiceProvider.class.getResource("/conf/nifi.properties").getFile());

        // load the system bundle
        systemBundle = SystemBundle.create(niFiProperties);
        extensionManager = new StandardExtensionDiscoveringManager();
        extensionManager.discoverExtensions(systemBundle, Collections.emptySet());
    }

    @BeforeEach
    public void setup() {
        flowManager = Mockito.mock(FlowManager.class);

        final ConcurrentMap<String, ProcessorNode> processorMap = new ConcurrentHashMap<>();
        Mockito.doAnswer((Answer<ProcessorNode>) invocation -> {
            final String id = invocation.getArgument(0);
            return processorMap.get(id);
        }).when(flowManager).getProcessorNode(Mockito.anyString());

        Mockito.doAnswer((Answer<Object>) invocation -> {
            final ProcessorNode procNode = invocation.getArgument(0);
            processorMap.putIfAbsent(procNode.getIdentifier(), procNode);
            return null;
        }).when(flowManager).onProcessorAdded(any(ProcessorNode.class));
    }

    private StandardProcessScheduler createScheduler() {
        return new StandardProcessScheduler(new FlowEngine(1, "Unit Test", true), Mockito.mock(FlowController.class),
                stateManagerProvider, niFiProperties, new StandardLifecycleStateManager());
    }

    private void setProperty(ControllerServiceNode serviceNode, String propName, String propValue) {
        Map<String, String> props = new LinkedHashMap<>();
        props.put(propName, propValue);
        serviceNode.setProperties(props);
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
            .stateManagerProvider(Mockito.mock(StateManagerProvider.class))
            .extensionManager(extensionManager)
            .buildControllerService();

        serviceProvider.onControllerServiceAdded(serviceNode);

        return serviceNode;
    }


    @Test
    public void testDisableControllerService() {
        final ProcessGroup procGroup = new MockProcessGroup(flowManager);
        final FlowManager flowManager = Mockito.mock(FlowManager.class);

        Mockito.when(flowManager.getGroup(Mockito.anyString())).thenReturn(procGroup);

        final StandardProcessScheduler scheduler = createScheduler();
        final StandardControllerServiceProvider provider = new StandardControllerServiceProvider(scheduler, null, flowManager, extensionManager);

        final ControllerServiceNode serviceNode = createControllerService(ServiceB.class.getName(), "B", systemBundle.getBundleDetails().getCoordinate(), provider);
        serviceNode.performValidation();
        serviceNode.getValidationStatus(5, TimeUnit.SECONDS);
        provider.enableControllerService(serviceNode);
        provider.disableControllerService(serviceNode);
    }

    @Test
    @Timeout(10)
    public void testEnableDisableWithReference() throws InterruptedException {
        final ProcessGroup group = new MockProcessGroup(flowManager);
        final FlowManager flowManager = Mockito.mock(FlowManager.class);

        Mockito.when(flowManager.getGroup(Mockito.anyString())).thenReturn(group);

        final StandardProcessScheduler scheduler = createScheduler();
        final StandardControllerServiceProvider provider = new StandardControllerServiceProvider(scheduler, null, flowManager, extensionManager);

        final ControllerServiceNode serviceNodeB = createControllerService(ServiceB.class.getName(), "B", systemBundle.getBundleDetails().getCoordinate(), provider);
        final ControllerServiceNode serviceNodeA = createControllerService(ServiceA.class.getName(), "A", systemBundle.getBundleDetails().getCoordinate(), provider);
        group.addControllerService(serviceNodeA);
        group.addControllerService(serviceNodeB);

        setProperty(serviceNodeA, ServiceA.OTHER_SERVICE.getName(), "B");

        try {
            provider.enableControllerService(serviceNodeA);
        } catch (final IllegalStateException expected) {
        }

        assertSame(ControllerServiceState.ENABLING, serviceNodeA.getState());

        serviceNodeB.performValidation();
        assertSame(ValidationStatus.VALID, serviceNodeB.getValidationStatus(5, TimeUnit.SECONDS));
        provider.enableControllerService(serviceNodeB);

        serviceNodeA.performValidation();

        final long maxTime = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        // Wait for Service A to become ENABLED. This will happen in a background thread after approximately 5 seconds, now that Service A is valid.
        while (serviceNodeA.getState() != ControllerServiceState.ENABLED && System.nanoTime() <= maxTime) {
            Thread.sleep(5L);
        }
        assertSame(ControllerServiceState.ENABLED, serviceNodeA.getState());
        assertThrows(IllegalStateException.class, () -> provider.disableControllerService(serviceNodeB));

        provider.disableControllerService(serviceNodeA);
        waitForServiceState(serviceNodeA, ControllerServiceState.DISABLED);

        provider.disableControllerService(serviceNodeB);
        waitForServiceState(serviceNodeB, ControllerServiceState.DISABLED);
    }

    private void waitForServiceState(final ControllerServiceNode service, final ControllerServiceState desiredState) {
        while (service.getState() != desiredState) {
            try {
                Thread.sleep(50L);
            } catch (final InterruptedException ignored) {
            }
        }
    }

    @Test
    public void testOrderingOfServices() {
        final ProcessGroup procGroup = new MockProcessGroup(flowManager);

        final FlowManager flowManager = Mockito.mock(FlowManager.class);
        Mockito.when(flowManager.getGroup(Mockito.anyString())).thenReturn(procGroup);

        final StandardControllerServiceProvider provider = new StandardControllerServiceProvider(null, null, flowManager, extensionManager);
        final ControllerServiceNode serviceNode1 = createControllerService(ServiceA.class.getName(), "1", systemBundle.getBundleDetails().getCoordinate(), provider);
        final ControllerServiceNode serviceNode2 = createControllerService(ServiceB.class.getName(), "2", systemBundle.getBundleDetails().getCoordinate(), provider);

        setProperty(serviceNode1, ServiceA.OTHER_SERVICE.getName(), "2");

        final Map<String, ControllerServiceNode> nodeMap = new LinkedHashMap<>();
        nodeMap.put("1", serviceNode1);
        nodeMap.put("2", serviceNode2);

        List<List<ControllerServiceNode>> branches = StandardControllerServiceProvider.determineEnablingOrder(nodeMap);
        assertEquals(2, branches.size());
        List<ControllerServiceNode> ordered = branches.get(0);
        assertEquals(2, ordered.size());
        assertSame(ordered.get(0), serviceNode2);
        assertSame(ordered.get(1), serviceNode1);
        assertEquals(1, branches.get(1).size());
        assertSame(branches.get(1).get(0), serviceNode2);

        nodeMap.clear();
        nodeMap.put("2", serviceNode2);
        nodeMap.put("1", serviceNode1);

        branches = StandardControllerServiceProvider.determineEnablingOrder(nodeMap);
        assertEquals(2, branches.size());
        ordered = branches.get(1);
        assertEquals(2, ordered.size());
        assertSame(ordered.get(0), serviceNode2);
        assertSame(ordered.get(1), serviceNode1);
        assertEquals(1, branches.get(0).size());
        assertSame(branches.get(0).get(0), serviceNode2);

        // add circular dependency on self.
        nodeMap.clear();
        setProperty(serviceNode1, ServiceA.OTHER_SERVICE_2.getName(), "1");
        nodeMap.put("1", serviceNode1);
        nodeMap.put("2", serviceNode2);

        branches = StandardControllerServiceProvider.determineEnablingOrder(nodeMap);
        assertEquals(2, branches.size());
        ordered = branches.get(0);
        assertEquals(2, ordered.size());
        assertSame(ordered.get(0), serviceNode2);
        assertSame(ordered.get(1), serviceNode1);

        nodeMap.clear();
        nodeMap.put("2", serviceNode2);
        nodeMap.put("1", serviceNode1);
        branches = StandardControllerServiceProvider.determineEnablingOrder(nodeMap);
        assertEquals(2, branches.size());
        ordered = branches.get(1);
        assertEquals(2, ordered.size());
        assertSame(ordered.get(0), serviceNode2);
        assertSame(ordered.get(1), serviceNode1);

        // add circular dependency once removed. In this case, we won't actually be able to enable these because of the
        // circular dependency because they will never be valid because they will always depend on a disabled service.
        // But we want to ensure that the method returns successfully without throwing a StackOverflowException or anything
        // like that.
        nodeMap.clear();
        final ControllerServiceNode serviceNode3 = createControllerService(ServiceA.class.getName(), "3", systemBundle.getBundleDetails().getCoordinate(), provider);
        setProperty(serviceNode1, ServiceA.OTHER_SERVICE.getName(), "3");
        setProperty(serviceNode3, ServiceA.OTHER_SERVICE.getName(), "1");
        nodeMap.put("1", serviceNode1);
        nodeMap.put("3", serviceNode3);
        branches = StandardControllerServiceProvider.determineEnablingOrder(nodeMap);
        assertEquals(2, branches.size());
        ordered = branches.get(0);
        assertEquals(2, ordered.size());
        assertSame(ordered.get(0), serviceNode3);
        assertSame(ordered.get(1), serviceNode1);

        nodeMap.clear();
        nodeMap.put("3", serviceNode3);
        nodeMap.put("1", serviceNode1);
        branches = StandardControllerServiceProvider.determineEnablingOrder(nodeMap);
        assertEquals(2, branches.size());
        ordered = branches.get(1);
        assertEquals(2, ordered.size());
        assertSame(ordered.get(0), serviceNode3);
        assertSame(ordered.get(1), serviceNode1);

        // Add multiple completely disparate branches.
        nodeMap.clear();
        setProperty(serviceNode1, ServiceA.OTHER_SERVICE.getName(), "2");
        final ControllerServiceNode serviceNode4 = createControllerService(ServiceB.class.getName(), "4", systemBundle.getBundleDetails().getCoordinate(), provider);
        final ControllerServiceNode serviceNode5 = createControllerService(ServiceB.class.getName(), "5", systemBundle.getBundleDetails().getCoordinate(), provider);
        setProperty(serviceNode3, ServiceA.OTHER_SERVICE.getName(), "4");
        nodeMap.put("1", serviceNode1);
        nodeMap.put("2", serviceNode2);
        nodeMap.put("3", serviceNode3);
        nodeMap.put("4", serviceNode4);
        nodeMap.put("5", serviceNode5);

        branches = StandardControllerServiceProvider.determineEnablingOrder(nodeMap);
        assertEquals(5, branches.size());

        ordered = branches.get(0);
        assertEquals(2, ordered.size());
        assertSame(ordered.get(0), serviceNode2);
        assertSame(ordered.get(1), serviceNode1);

        assertEquals(1, branches.get(1).size());
        assertSame(branches.get(1).get(0), serviceNode2);

        ordered = branches.get(2);
        assertEquals(2, ordered.size());
        assertSame(ordered.get(0), serviceNode4);
        assertSame(ordered.get(1), serviceNode3);

        assertEquals(1, branches.get(3).size());
        assertSame(branches.get(3).get(0), serviceNode4);

        assertEquals(1, branches.get(4).size());
        assertSame(branches.get(4).get(0), serviceNode5);

        // create 2 branches both dependent on the same service
        nodeMap.clear();
        setProperty(serviceNode1, ServiceA.OTHER_SERVICE.getName(), "2");
        setProperty(serviceNode3, ServiceA.OTHER_SERVICE.getName(), "2");
        nodeMap.put("1", serviceNode1);
        nodeMap.put("2", serviceNode2);
        nodeMap.put("3", serviceNode3);

        branches = StandardControllerServiceProvider.determineEnablingOrder(nodeMap);
        assertEquals(3, branches.size());

        ordered = branches.get(0);
        assertEquals(2, ordered.size());
        assertSame(ordered.get(0), serviceNode2);
        assertSame(ordered.get(1), serviceNode1);

        ordered = branches.get(1);
        assertEquals(1, ordered.size());
        assertSame(ordered.get(0), serviceNode2);

        ordered = branches.get(2);
        assertEquals(2, ordered.size());
        assertSame(ordered.get(0), serviceNode2);
        assertSame(ordered.get(1), serviceNode3);
    }

    private ProcessorNode createProcessor(final StandardProcessScheduler scheduler, final ControllerServiceProvider serviceProvider) {
        final ReloadComponent reloadComponent = Mockito.mock(ReloadComponent.class);

        final Processor processor = new DummyProcessor();
        final MockProcessContext context = new MockProcessContext(processor, Mockito.mock(StateManager.class));
        final MockProcessorInitializationContext mockInitContext = new MockProcessorInitializationContext(processor, context);
        processor.initialize(mockInitContext);

        final LoggableComponent<Processor> dummyProcessor = new LoggableComponent<>(processor, systemBundle.getBundleDetails().getCoordinate(), null);
        final ProcessorNode procNode = new StandardProcessorNode(dummyProcessor, mockInitContext.getIdentifier(),
                new StandardValidationContextFactory(serviceProvider), scheduler, serviceProvider,
                reloadComponent, extensionManager, new SynchronousValidationTrigger());

        final FlowManager flowManager = Mockito.mock(FlowManager.class);
        final FlowController flowController = Mockito.mock(FlowController.class );
        Mockito.when(flowController.getFlowManager()).thenReturn(flowManager);
        Mockito.when(flowController.getStateManagerProvider()).thenReturn(stateManagerProvider);

        final ProcessGroup group = new MockProcessGroup(null);
        group.addProcessor(procNode);
        procNode.setProcessGroup(group);

        return procNode;
    }

    @Test
    public void testEnableReferencingComponents() {
        final ProcessGroup procGroup = new MockProcessGroup(flowManager);

        final FlowManager flowManager = Mockito.mock(FlowManager.class);
        Mockito.when(flowManager.getGroup(Mockito.anyString())).thenReturn(procGroup);

        final StandardProcessScheduler scheduler = createScheduler();
        final StandardControllerServiceProvider provider = new StandardControllerServiceProvider(scheduler, null, flowManager, extensionManager);
        final ControllerServiceNode serviceNode = createControllerService(ServiceA.class.getName(), "1", systemBundle.getBundleDetails().getCoordinate(), provider);

        final ProcessorNode procNode = createProcessor(scheduler, provider);
        serviceNode.addReference(procNode, PropertyDescriptor.NULL_DESCRIPTOR);

        // procNode.setScheduledState(ScheduledState.STOPPED);
        provider.unscheduleReferencingComponents(serviceNode);
        assertEquals(ScheduledState.STOPPED, procNode.getScheduledState());

        // procNode.setScheduledState(ScheduledState.RUNNING);
        provider.unscheduleReferencingComponents(serviceNode);
        assertEquals(ScheduledState.STOPPED, procNode.getScheduledState());
    }

    @Test
    public void validateEnableServices() {
        final FlowManager flowManager = Mockito.mock(FlowManager.class);

        StandardProcessScheduler scheduler = createScheduler();

        final StandardControllerServiceProvider provider = new StandardControllerServiceProvider(scheduler, null, flowManager, extensionManager);
        ProcessGroup procGroup = new MockProcessGroup(flowManager);

        Mockito.when(flowManager.getGroup(Mockito.anyString())).thenReturn(procGroup);

        ControllerServiceNode A = createControllerService(ServiceA.class.getName(), "A", systemBundle.getBundleDetails().getCoordinate(), provider);
        ControllerServiceNode B = createControllerService(ServiceA.class.getName(), "B", systemBundle.getBundleDetails().getCoordinate(), provider);
        ControllerServiceNode C = createControllerService(ServiceA.class.getName(), "C", systemBundle.getBundleDetails().getCoordinate(), provider);
        ControllerServiceNode D = createControllerService(ServiceB.class.getName(), "D", systemBundle.getBundleDetails().getCoordinate(), provider);
        ControllerServiceNode E = createControllerService(ServiceA.class.getName(), "E", systemBundle.getBundleDetails().getCoordinate(), provider);
        ControllerServiceNode F = createControllerService(ServiceB.class.getName(), "F", systemBundle.getBundleDetails().getCoordinate(), provider);

        procGroup.addControllerService(A);
        procGroup.addControllerService(B);
        procGroup.addControllerService(C);
        procGroup.addControllerService(D);
        procGroup.addControllerService(E);
        procGroup.addControllerService(F);

        setProperty(A, ServiceA.OTHER_SERVICE.getName(), "B");
        setProperty(B, ServiceA.OTHER_SERVICE.getName(), "D");
        setProperty(C, ServiceA.OTHER_SERVICE.getName(), "B");
        setProperty(C, ServiceA.OTHER_SERVICE_2.getName(), "D");
        setProperty(E, ServiceA.OTHER_SERVICE.getName(), "A");
        setProperty(E, ServiceA.OTHER_SERVICE_2.getName(), "F");

        final List<ControllerServiceNode> serviceNodes = Arrays.asList(A, B, C, D, E, F);
        serviceNodes.stream().forEach(ControllerServiceNode::performValidation);
        provider.enableControllerServices(serviceNodes);

        assertTrue(A.isActive());
        assertTrue(B.isActive());
        assertTrue(C.isActive());
        assertTrue(D.isActive());
        assertTrue(E.isActive());
        assertTrue(F.isActive());
    }

    /**
     * This test is similar to the above, but different combination of service
     * dependencies
     *
     */
    @Test
    public void validateEnableServices2() {
        final FlowManager flowManager = Mockito.mock(FlowManager.class);

        StandardProcessScheduler scheduler = createScheduler();

        final StandardControllerServiceProvider provider = new StandardControllerServiceProvider(scheduler, null, flowManager, extensionManager);
        ProcessGroup procGroup = new MockProcessGroup(flowManager);

        Mockito.when(flowManager.getGroup(Mockito.anyString())).thenReturn(procGroup);

        ControllerServiceNode A = createControllerService(ServiceC.class.getName(), "A", systemBundle.getBundleDetails().getCoordinate(), provider);
        ControllerServiceNode B = createControllerService(ServiceA.class.getName(), "B", systemBundle.getBundleDetails().getCoordinate(), provider);
        ControllerServiceNode C = createControllerService(ServiceB.class.getName(), "C", systemBundle.getBundleDetails().getCoordinate(), provider);
        ControllerServiceNode D = createControllerService(ServiceA.class.getName(), "D", systemBundle.getBundleDetails().getCoordinate(), provider);
        ControllerServiceNode F = createControllerService(ServiceA.class.getName(), "F", systemBundle.getBundleDetails().getCoordinate(), provider);

        procGroup.addControllerService(A);
        procGroup.addControllerService(B);
        procGroup.addControllerService(C);
        procGroup.addControllerService(D);
        procGroup.addControllerService(F);

        setProperty(A, ServiceC.REQ_SERVICE_1.getName(), "B");
        setProperty(A, ServiceC.REQ_SERVICE_2.getName(), "D");
        setProperty(B, ServiceA.OTHER_SERVICE.getName(), "C");

        setProperty(F, ServiceA.OTHER_SERVICE.getName(), "D");
        setProperty(D, ServiceA.OTHER_SERVICE.getName(), "C");

        final List<ControllerServiceNode> services = Arrays.asList(C, F, A, B, D);
        services.forEach(ControllerServiceNode::performValidation);

        provider.enableControllerServices(services);

        assertTrue(A.isActive());
        assertTrue(B.isActive());
        assertTrue(C.isActive());
        assertTrue(D.isActive());
        assertTrue(F.isActive());
    }

    @Test
    public void validateEnableServicesWithDisabledMissingService() {
        final FlowManager flowManager = Mockito.mock(FlowManager.class);

        StandardProcessScheduler scheduler = createScheduler();

        final StandardControllerServiceProvider provider = new StandardControllerServiceProvider(scheduler, null, flowManager, extensionManager);
        ProcessGroup procGroup = new MockProcessGroup(flowManager);

        Mockito.when(flowManager.getGroup(Mockito.anyString())).thenReturn(procGroup);

        ControllerServiceNode serviceNode1 = createControllerService(ServiceA.class.getName(), "1", systemBundle.getBundleDetails().getCoordinate(), provider);
        ControllerServiceNode serviceNode2 = createControllerService(ServiceA.class.getName(), "2", systemBundle.getBundleDetails().getCoordinate(), provider);
        ControllerServiceNode serviceNode3 = createControllerService(ServiceA.class.getName(), "3", systemBundle.getBundleDetails().getCoordinate(), provider);
        ControllerServiceNode serviceNode4 = createControllerService(ServiceB.class.getName(), "4", systemBundle.getBundleDetails().getCoordinate(), provider);
        ControllerServiceNode serviceNode5 = createControllerService(ServiceA.class.getName(), "5", systemBundle.getBundleDetails().getCoordinate(), provider);
        ControllerServiceNode serviceNode6 = createControllerService(ServiceB.class.getName(), "6", systemBundle.getBundleDetails().getCoordinate(), provider);
        ControllerServiceNode serviceNode7 = createControllerService(ServiceC.class.getName(), "7", systemBundle.getBundleDetails().getCoordinate(), provider);

        procGroup.addControllerService(serviceNode1);
        procGroup.addControllerService(serviceNode2);
        procGroup.addControllerService(serviceNode3);
        procGroup.addControllerService(serviceNode4);
        procGroup.addControllerService(serviceNode5);
        procGroup.addControllerService(serviceNode6);
        procGroup.addControllerService(serviceNode7);

        setProperty(serviceNode1, ServiceA.OTHER_SERVICE.getName(), "2");
        setProperty(serviceNode2, ServiceA.OTHER_SERVICE.getName(), "4");
        setProperty(serviceNode3, ServiceA.OTHER_SERVICE.getName(), "2");
        setProperty(serviceNode3, ServiceA.OTHER_SERVICE_2.getName(), "4");
        setProperty(serviceNode5, ServiceA.OTHER_SERVICE.getName(), "6");
        setProperty(serviceNode7, ServiceC.REQ_SERVICE_1.getName(), "2");
        setProperty(serviceNode7, ServiceC.REQ_SERVICE_2.getName(), "3");

        final List<ControllerServiceNode> allBut6 = Arrays.asList(serviceNode1, serviceNode2, serviceNode3, serviceNode4, serviceNode5, serviceNode7);
        allBut6.stream().forEach(ControllerServiceNode::performValidation);

        provider.enableControllerServices(allBut6);
        assertTrue(serviceNode1.isActive());
        assertTrue(serviceNode2.isActive());
        assertTrue(serviceNode3.isActive());
        assertTrue(serviceNode4.isActive());
        assertFalse(serviceNode5.isActive());
        assertFalse(serviceNode6.isActive());

        serviceNode6.performValidation();
        provider.enableControllerService(serviceNode6);

        provider.enableControllerServices(Arrays.asList(
                serviceNode1, serviceNode2, serviceNode3, serviceNode4, serviceNode5));

        assertTrue(serviceNode1.isActive());
        assertTrue(serviceNode2.isActive());
        assertTrue(serviceNode3.isActive());
        assertTrue(serviceNode4.isActive());
        assertTrue(serviceNode5.isActive());
        assertTrue(serviceNode6.isActive());
    }
}
