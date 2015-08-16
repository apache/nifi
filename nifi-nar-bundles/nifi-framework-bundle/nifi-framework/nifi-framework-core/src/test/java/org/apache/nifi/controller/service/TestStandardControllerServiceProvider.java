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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.StandardProcessorNode;
import org.apache.nifi.controller.service.mock.DummyProcessor;
import org.apache.nifi.controller.service.mock.ServiceA;
import org.apache.nifi.controller.service.mock.ServiceB;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.processor.StandardProcessorInitializationContext;
import org.apache.nifi.processor.StandardValidationContextFactory;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestStandardControllerServiceProvider {

    private ProcessScheduler createScheduler() {
        final ProcessScheduler scheduler = Mockito.mock(ProcessScheduler.class);
        Mockito.doAnswer(new Answer<Object>() {
            @Override
            public Object answer(final InvocationOnMock invocation) throws Throwable {
                final ControllerServiceNode node = (ControllerServiceNode) invocation.getArguments()[0];
                node.verifyCanEnable();
                node.setState(ControllerServiceState.ENABLED);
                return null;
            }
        }).when(scheduler).enableControllerService(Mockito.any(ControllerServiceNode.class));

        Mockito.doAnswer(new Answer<Object>() {
            @Override
            public Object answer(final InvocationOnMock invocation) throws Throwable {
                final ControllerServiceNode node = (ControllerServiceNode) invocation.getArguments()[0];
                node.verifyCanDisable();
                node.setState(ControllerServiceState.DISABLED);
                return null;
            }
        }).when(scheduler).disableControllerService(Mockito.any(ControllerServiceNode.class));

        return scheduler;
    }

    @Test
    public void testDisableControllerService() {
        final ProcessScheduler scheduler = createScheduler();
        final StandardControllerServiceProvider provider = new StandardControllerServiceProvider(scheduler, null);

        final ControllerServiceNode serviceNode = provider.createControllerService(ServiceB.class.getName(), "B", false);
        provider.enableControllerService(serviceNode);
        provider.disableControllerService(serviceNode);
    }

    @Test
    public void testEnableDisableWithReference() {
        final ProcessScheduler scheduler = createScheduler();
        final StandardControllerServiceProvider provider = new StandardControllerServiceProvider(scheduler, null);

        final ControllerServiceNode serviceNodeB = provider.createControllerService(ServiceB.class.getName(), "B", false);
        final ControllerServiceNode serviceNodeA = provider.createControllerService(ServiceA.class.getName(), "A", false);

        serviceNodeA.setProperty(ServiceA.OTHER_SERVICE.getName(), "B");

        try {
            provider.enableControllerService(serviceNodeA);
            Assert.fail("Was able to enable Service A but Service B is disabled.");
        } catch (final IllegalStateException expected) {
        }

        provider.enableControllerService(serviceNodeB);
        provider.enableControllerService(serviceNodeA);

        try {
            provider.disableControllerService(serviceNodeB);
            Assert.fail("Was able to disable Service B but Service A is enabled and references B");
        } catch (final IllegalStateException expected) {
        }

        provider.disableControllerService(serviceNodeA);
        provider.disableControllerService(serviceNodeB);
    }

    @Test
    public void testEnableReferencingServicesGraph() {
        final ProcessScheduler scheduler = createScheduler();
        final StandardControllerServiceProvider provider = new StandardControllerServiceProvider(scheduler, null);

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
        final ControllerServiceNode serviceNode1 = provider.createControllerService(ServiceA.class.getName(), "1", false);
        final ControllerServiceNode serviceNode2 = provider.createControllerService(ServiceA.class.getName(), "2", false);
        final ControllerServiceNode serviceNode3 = provider.createControllerService(ServiceA.class.getName(), "3", false);
        final ControllerServiceNode serviceNode4 = provider.createControllerService(ServiceB.class.getName(), "4", false);

        serviceNode1.setProperty(ServiceA.OTHER_SERVICE.getName(), "2");
        serviceNode2.setProperty(ServiceA.OTHER_SERVICE.getName(), "4");
        serviceNode3.setProperty(ServiceA.OTHER_SERVICE.getName(), "2");
        serviceNode3.setProperty(ServiceA.OTHER_SERVICE_2.getName(), "4");

        provider.enableControllerService(serviceNode4);
        provider.enableReferencingServices(serviceNode4);

        assertEquals(ControllerServiceState.ENABLED, serviceNode3.getState());
        assertEquals(ControllerServiceState.ENABLED, serviceNode2.getState());
        assertEquals(ControllerServiceState.ENABLED, serviceNode1.getState());
    }

    @Test
    public void testStartStopReferencingComponents() {
        final ProcessScheduler scheduler = createScheduler();
        final StandardControllerServiceProvider provider = new StandardControllerServiceProvider(scheduler, null);

        // build a graph of reporting tasks and controller services with dependencies as such:
        //
        // Processor P1 -> A -> B -> D
        // Processor P2 -> C ---^----^
        //
        // In other words, Processor P1 references Controller Service A, which references B, which references D.
        // AND
        // Processor P2 references Controller Service C, which references B and D.
        //
        // So we have to verify that if D is enabled, when we enable its referencing services,
        // we enable C and B, even if we attempt to enable C before B... i.e., if we try to enable C, we cannot do so
        // until B is first enabled so ensure that we enable B first.
        final ControllerServiceNode serviceNode1 = provider.createControllerService(ServiceA.class.getName(), "1", false);
        final ControllerServiceNode serviceNode2 = provider.createControllerService(ServiceA.class.getName(), "2", false);
        final ControllerServiceNode serviceNode3 = provider.createControllerService(ServiceA.class.getName(), "3", false);
        final ControllerServiceNode serviceNode4 = provider.createControllerService(ServiceB.class.getName(), "4", false);

        final ProcessGroup mockProcessGroup = Mockito.mock(ProcessGroup.class);
        Mockito.doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                final ProcessorNode procNode = (ProcessorNode) invocation.getArguments()[0];
                procNode.verifyCanStart();
                procNode.setScheduledState(ScheduledState.RUNNING);
                return null;
            }
        }).when(mockProcessGroup).startProcessor(Mockito.any(ProcessorNode.class));

        Mockito.doAnswer(new Answer<Object>() {
            @Override
            public Object answer(final InvocationOnMock invocation) throws Throwable {
                final ProcessorNode procNode = (ProcessorNode) invocation.getArguments()[0];
                procNode.verifyCanStop();
                procNode.setScheduledState(ScheduledState.STOPPED);
                return null;
            }
        }).when(mockProcessGroup).stopProcessor(Mockito.any(ProcessorNode.class));

        final String id1 = UUID.randomUUID().toString();
        final ProcessorNode procNodeA = new StandardProcessorNode(new DummyProcessor(), id1,
                new StandardValidationContextFactory(provider), scheduler, provider);
        procNodeA.getProcessor().initialize(new StandardProcessorInitializationContext(id1, null, provider));
        procNodeA.setProperty(DummyProcessor.SERVICE.getName(), "1");
        procNodeA.setProcessGroup(mockProcessGroup);

        final String id2 = UUID.randomUUID().toString();
        final ProcessorNode procNodeB = new StandardProcessorNode(new DummyProcessor(), id2,
                new StandardValidationContextFactory(provider), scheduler, provider);
        procNodeB.getProcessor().initialize(new StandardProcessorInitializationContext(id2, null, provider));
        procNodeB.setProperty(DummyProcessor.SERVICE.getName(), "3");
        procNodeB.setProcessGroup(mockProcessGroup);

        serviceNode1.setProperty(ServiceA.OTHER_SERVICE.getName(), "2");
        serviceNode2.setProperty(ServiceA.OTHER_SERVICE.getName(), "4");
        serviceNode3.setProperty(ServiceA.OTHER_SERVICE.getName(), "2");
        serviceNode3.setProperty(ServiceA.OTHER_SERVICE_2.getName(), "4");

        provider.enableControllerService(serviceNode4);
        provider.enableReferencingServices(serviceNode4);
        provider.scheduleReferencingComponents(serviceNode4);

        assertEquals(ControllerServiceState.ENABLED, serviceNode3.getState());
        assertEquals(ControllerServiceState.ENABLED, serviceNode2.getState());
        assertEquals(ControllerServiceState.ENABLED, serviceNode1.getState());
        assertTrue(procNodeA.isRunning());
        assertTrue(procNodeB.isRunning());

        // stop processors and verify results.
        provider.unscheduleReferencingComponents(serviceNode4);
        assertFalse(procNodeA.isRunning());
        assertFalse(procNodeB.isRunning());
        assertEquals(ControllerServiceState.ENABLED, serviceNode3.getState());
        assertEquals(ControllerServiceState.ENABLED, serviceNode2.getState());
        assertEquals(ControllerServiceState.ENABLED, serviceNode1.getState());

        provider.disableReferencingServices(serviceNode4);
        assertEquals(ControllerServiceState.DISABLED, serviceNode3.getState());
        assertEquals(ControllerServiceState.DISABLED, serviceNode2.getState());
        assertEquals(ControllerServiceState.DISABLED, serviceNode1.getState());
        assertEquals(ControllerServiceState.ENABLED, serviceNode4.getState());

        provider.disableControllerService(serviceNode4);
        assertEquals(ControllerServiceState.DISABLED, serviceNode4.getState());
    }

    @Test
    public void testOrderingOfServices() {
        final StandardControllerServiceProvider provider = new StandardControllerServiceProvider(null, null);
        final ControllerServiceNode serviceNode1 = provider.createControllerService(ServiceA.class.getName(), "1", false);
        final ControllerServiceNode serviceNode2 = provider.createControllerService(ServiceB.class.getName(), "2", false);

        serviceNode1.setProperty(ServiceA.OTHER_SERVICE.getName(), "2");

        final Map<String, ControllerServiceNode> nodeMap = new LinkedHashMap<>();
        nodeMap.put("1", serviceNode1);
        nodeMap.put("2", serviceNode2);

        List<List<ControllerServiceNode>> branches = StandardControllerServiceProvider.determineEnablingOrder(nodeMap);
        assertEquals(2, branches.size());
        List<ControllerServiceNode> ordered = branches.get(0);
        assertEquals(2, ordered.size());
        assertTrue(ordered.get(0) == serviceNode2);
        assertTrue(ordered.get(1) == serviceNode1);
        assertEquals(1, branches.get(1).size());
        assertTrue(branches.get(1).get(0) == serviceNode2);

        nodeMap.clear();
        nodeMap.put("2", serviceNode2);
        nodeMap.put("1", serviceNode1);

        branches = StandardControllerServiceProvider.determineEnablingOrder(nodeMap);
        assertEquals(2, branches.size());
        ordered = branches.get(1);
        assertEquals(2, ordered.size());
        assertTrue(ordered.get(0) == serviceNode2);
        assertTrue(ordered.get(1) == serviceNode1);
        assertEquals(1, branches.get(0).size());
        assertTrue(branches.get(0).get(0) == serviceNode2);

        // add circular dependency on self.
        nodeMap.clear();
        serviceNode1.setProperty(ServiceA.OTHER_SERVICE_2.getName(), "1");
        nodeMap.put("1", serviceNode1);
        nodeMap.put("2", serviceNode2);

        branches = StandardControllerServiceProvider.determineEnablingOrder(nodeMap);
        assertEquals(2, branches.size());
        ordered = branches.get(0);
        assertEquals(2, ordered.size());
        assertTrue(ordered.get(0) == serviceNode2);
        assertTrue(ordered.get(1) == serviceNode1);

        nodeMap.clear();
        nodeMap.put("2", serviceNode2);
        nodeMap.put("1", serviceNode1);
        branches = StandardControllerServiceProvider.determineEnablingOrder(nodeMap);
        assertEquals(2, branches.size());
        ordered = branches.get(1);
        assertEquals(2, ordered.size());
        assertTrue(ordered.get(0) == serviceNode2);
        assertTrue(ordered.get(1) == serviceNode1);

        // add circular dependency once removed. In this case, we won't actually be able to enable these because of the
        // circular dependency because they will never be valid because they will always depend on a disabled service.
        // But we want to ensure that the method returns successfully without throwing a StackOverflowException or anything
        // like that.
        nodeMap.clear();
        final ControllerServiceNode serviceNode3 = provider.createControllerService(ServiceA.class.getName(), "3", false);
        serviceNode1.setProperty(ServiceA.OTHER_SERVICE.getName(), "3");
        serviceNode3.setProperty(ServiceA.OTHER_SERVICE.getName(), "1");
        nodeMap.put("1", serviceNode1);
        nodeMap.put("3", serviceNode3);
        branches = StandardControllerServiceProvider.determineEnablingOrder(nodeMap);
        assertEquals(2, branches.size());
        ordered = branches.get(0);
        assertEquals(2, ordered.size());
        assertTrue(ordered.get(0) == serviceNode3);
        assertTrue(ordered.get(1) == serviceNode1);

        nodeMap.clear();
        nodeMap.put("3", serviceNode3);
        nodeMap.put("1", serviceNode1);
        branches = StandardControllerServiceProvider.determineEnablingOrder(nodeMap);
        assertEquals(2, branches.size());
        ordered = branches.get(1);
        assertEquals(2, ordered.size());
        assertTrue(ordered.get(0) == serviceNode3);
        assertTrue(ordered.get(1) == serviceNode1);

        // Add multiple completely disparate branches.
        nodeMap.clear();
        serviceNode1.setProperty(ServiceA.OTHER_SERVICE.getName(), "2");
        final ControllerServiceNode serviceNode4 = provider.createControllerService(ServiceB.class.getName(), "4", false);
        final ControllerServiceNode serviceNode5 = provider.createControllerService(ServiceB.class.getName(), "5", false);
        serviceNode3.setProperty(ServiceA.OTHER_SERVICE.getName(), "4");
        nodeMap.put("1", serviceNode1);
        nodeMap.put("2", serviceNode2);
        nodeMap.put("3", serviceNode3);
        nodeMap.put("4", serviceNode4);
        nodeMap.put("5", serviceNode5);

        branches = StandardControllerServiceProvider.determineEnablingOrder(nodeMap);
        assertEquals(5, branches.size());

        ordered = branches.get(0);
        assertEquals(2, ordered.size());
        assertTrue(ordered.get(0) == serviceNode2);
        assertTrue(ordered.get(1) == serviceNode1);

        assertEquals(1, branches.get(1).size());
        assertTrue(branches.get(1).get(0) == serviceNode2);

        ordered = branches.get(2);
        assertEquals(2, ordered.size());
        assertTrue(ordered.get(0) == serviceNode4);
        assertTrue(ordered.get(1) == serviceNode3);

        assertEquals(1, branches.get(3).size());
        assertTrue(branches.get(3).get(0) == serviceNode4);

        assertEquals(1, branches.get(4).size());
        assertTrue(branches.get(4).get(0) == serviceNode5);

        // create 2 branches both dependent on the same service
        nodeMap.clear();
        serviceNode1.setProperty(ServiceA.OTHER_SERVICE.getName(), "2");
        serviceNode3.setProperty(ServiceA.OTHER_SERVICE.getName(), "2");
        nodeMap.put("1", serviceNode1);
        nodeMap.put("2", serviceNode2);
        nodeMap.put("3", serviceNode3);

        branches = StandardControllerServiceProvider.determineEnablingOrder(nodeMap);
        assertEquals(3, branches.size());

        ordered = branches.get(0);
        assertEquals(2, ordered.size());
        assertTrue(ordered.get(0) == serviceNode2);
        assertTrue(ordered.get(1) == serviceNode1);

        ordered = branches.get(1);
        assertEquals(1, ordered.size());
        assertTrue(ordered.get(0) == serviceNode2);

        ordered = branches.get(2);
        assertEquals(2, ordered.size());
        assertTrue(ordered.get(0) == serviceNode2);
        assertTrue(ordered.get(1) == serviceNode3);
    }

}
