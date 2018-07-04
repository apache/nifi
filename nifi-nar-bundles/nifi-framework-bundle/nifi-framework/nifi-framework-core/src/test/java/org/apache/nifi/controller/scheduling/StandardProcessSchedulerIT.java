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

package org.apache.nifi.controller.scheduling;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;

import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.controller.service.StandardControllerServiceProvider;
import org.apache.nifi.engine.FlowEngine;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.SystemBundle;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.SynchronousValidationTrigger;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class StandardProcessSchedulerIT {
    private final StateManagerProvider stateMgrProvider = Mockito.mock(StateManagerProvider.class);
    private VariableRegistry variableRegistry = VariableRegistry.ENVIRONMENT_SYSTEM_REGISTRY;
    private FlowController controller;
    private NiFiProperties nifiProperties;
    private Bundle systemBundle;
    private volatile String propsFile = TestStandardProcessScheduler.class.getResource("/standardprocessschedulertest.nifi.properties").getFile();

    @Before
    public void setup() throws InitializationException {
        this.nifiProperties = NiFiProperties.createBasicNiFiProperties(propsFile, null);

        // load the system bundle
        systemBundle = SystemBundle.create(nifiProperties);
        ExtensionManager.discoverExtensions(systemBundle, Collections.emptySet());

        controller = Mockito.mock(FlowController.class);
    }

    /**
     * Validates that the service that is currently in ENABLING state can be
     * disabled and that its @OnDisabled operation will be invoked as soon as
     *
     * @OnEnable finishes.
     */
    @Test
    public void validateLongEnablingServiceCanStillBeDisabled() throws Exception {
        final StandardProcessScheduler scheduler = new StandardProcessScheduler(new FlowEngine(1, "Unit Test", true), null, null, stateMgrProvider, nifiProperties);
        final StandardControllerServiceProvider provider = new StandardControllerServiceProvider(controller, scheduler, null,
            stateMgrProvider, variableRegistry, nifiProperties, new SynchronousValidationTrigger());

        final ControllerServiceNode serviceNode = provider.createControllerService(LongEnablingService.class.getName(),
            "1", systemBundle.getBundleDetails().getCoordinate(), null, false);

        final LongEnablingService ts = (LongEnablingService) serviceNode.getControllerServiceImplementation();
        ts.setLimit(3000);
        scheduler.enableControllerService(serviceNode);
        Thread.sleep(2000);
        assertTrue(serviceNode.isActive());
        assertEquals(1, ts.enableInvocationCount());

        Thread.sleep(500);
        scheduler.disableControllerService(serviceNode);
        assertFalse(serviceNode.isActive());
        assertEquals(ControllerServiceState.DISABLING, serviceNode.getState());
        assertEquals(0, ts.disableInvocationCount());
        // wait a bit. . . Enabling will finish and @OnDisabled will be invoked
        // automatically
        Thread.sleep(4000);
        assertEquals(ControllerServiceState.DISABLED, serviceNode.getState());
        assertEquals(1, ts.disableInvocationCount());
    }

}
