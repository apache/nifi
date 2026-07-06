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
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.components.validation.ValidationTrigger;
import org.apache.nifi.components.validation.VerifiableComponentFactory;
import org.apache.nifi.controller.ExtensionBuilder;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.MockStateManagerProvider;
import org.apache.nifi.controller.NodeTypeProvider;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.ReloadComponent;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.scheduling.StandardLifecycleStateManager;
import org.apache.nifi.controller.scheduling.StandardProcessScheduler;
import org.apache.nifi.controller.service.mock.FailToEnableService;
import org.apache.nifi.controller.service.mock.MockProcessGroup;
import org.apache.nifi.engine.FlowEngine;
import org.apache.nifi.groups.DefaultComponentScheduler;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.nar.ExtensionDiscoveringManager;
import org.apache.nifi.nar.StandardExtensionDiscoveringManager;
import org.apache.nifi.nar.SystemBundle;
import org.apache.nifi.registry.flow.mapping.VersionedComponentStateLookup;
import org.apache.nifi.util.NiFiProperties;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;

import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Reproduces the controller-service enablement ordering gap behind SNOW-3697891.
 *
 * <p>During a connector/versioned flow upgrade the flow synchronizer reconciles the flow while a
 * {@link org.apache.nifi.groups.ComponentScheduler} is paused, queuing controller services to be (re)enabled and
 * stateless groups to be (re)started. On {@code resume()} the scheduler enables the impacted controller services and
 * then starts the stateless groups (see {@link org.apache.nifi.groups.AbstractComponentScheduler#resume()}).</p>
 *
 * <p>Crucially, the enable step is best-effort: {@link StandardControllerServiceProvider#enableControllerServices(java.util.Collection)}
 * waits up to 30 seconds per service and swallows any failure or timeout. When an impacted service fails to come back
 * ENABLED, the scheduler nonetheless proceeds to start the stateless group, which then attempts to process its pending
 * FlowFiles with a disabled service and fails with "Controller Service ... is disabled" -- exactly the customer symptom.</p>
 *
 * <p>The connector upgrade path uses {@link org.apache.nifi.groups.RetainExistingStateComponentScheduler}, which
 * delegates its {@code resume()} to the {@link DefaultComponentScheduler} exercised here.</p>
 */
public class StatelessGroupServiceEnableFailureUpgradeTest {

    private static NiFiProperties niFiProperties;
    private static ExtensionDiscoveringManager extensionManager;
    private static Bundle systemBundle;

    private FlowManager flowManager;

    @BeforeAll
    public static void setNiFiProps() {
        final URL propertiesUrl = StatelessGroupServiceEnableFailureUpgradeTest.class.getResource("/conf/nifi.properties");
        assertNotNull(propertiesUrl);
        niFiProperties = NiFiProperties.createBasicNiFiProperties(propertiesUrl.getFile());

        systemBundle = SystemBundle.create(niFiProperties);
        extensionManager = new StandardExtensionDiscoveringManager();
        extensionManager.discoverExtensions(systemBundle, Collections.emptySet());
    }

    @BeforeEach
    public void setup() {
        flowManager = Mockito.mock(FlowManager.class);
    }

    @Test
    @Timeout(60)
    public void testStatelessGroupStartedEvenWhenImpactedServiceFailsToEnableDuringUpgrade() {
        final MockProcessGroup parentGroup = new MockProcessGroup(flowManager);
        when(flowManager.getGroup(Mockito.anyString())).thenReturn(parentGroup);

        final StandardProcessScheduler processScheduler = new StandardProcessScheduler(
                new FlowEngine(1, "Unit Test", true), mock(FlowController.class),
                new MockStateManagerProvider(), niFiProperties, new StandardLifecycleStateManager());
        final StandardControllerServiceProvider provider = new StandardControllerServiceProvider(processScheduler, null, flowManager, extensionManager);

        // The shared Controller Service owned by the STANDARD parent group (mirrors the customer's "Snowflake Connection
        // Pool"). It is valid, but its @OnEnabled always throws, so it can never reach the ENABLED state -- simulating a
        // service that fails to come back up during the upgrade.
        final ControllerServiceNode failingService = createControllerService(FailToEnableService.class.getName(), "shared-service", provider);
        parentGroup.addControllerService(failingService);
        failingService.performValidation();
        failingService.getValidationStatus(5, TimeUnit.SECONDS);

        // The scheduler the flow synchronizer uses to (re)enable services and (re)start groups during the upgrade.
        final DefaultComponentScheduler componentScheduler = new DefaultComponentScheduler(provider, VersionedComponentStateLookup.IDENTITY_LOOKUP);

        // The nested STATELESS child group that references the shared service and has pending FlowFiles to drain.
        final ProcessGroup statelessChild = mock(ProcessGroup.class);
        when(statelessChild.getIdentifier()).thenReturn("stateless-child");

        // Simulate the synchronizer's paused reconciliation during the upgrade:
        //  (3) the impacted service is queued to be (re)enabled,
        //  (4) components are adjusted,
        //  (5)/(7) on resume the service is enabled and then the stateless group is started.
        componentScheduler.pause();
        componentScheduler.enableControllerServicesAsync(List.of(failingService));
        componentScheduler.startStatelessGroup(statelessChild);
        componentScheduler.resume();

        // (5) The upgrade attempted to enable the impacted service, but enabling failed and the failure was swallowed,
        // so the service never reached ENABLED.
        assertNotEquals(ControllerServiceState.ENABLED, failingService.getState(),
                "The shared service should have failed to enable during the upgrade");

        // (7) Despite the shared service being disabled, the stateless group was started anyway. This is the defect: the
        // upgrade does not gate stateless-group start on the impacted Controller Service actually reaching ENABLED, so
        // the group will attempt to process its pending FlowFiles against a disabled service.
        verify(statelessChild).startProcessing();
    }

    private ControllerServiceNode createControllerService(final String type, final String id, final ControllerServiceProvider serviceProvider) {
        final ControllerServiceNode serviceNode = new ExtensionBuilder()
                .identifier(id)
                .type(type)
                .bundleCoordinate(systemBundle.getBundleDetails().getCoordinate())
                .controllerServiceProvider(serviceProvider)
                .processScheduler(mock(ProcessScheduler.class))
                .nodeTypeProvider(mock(NodeTypeProvider.class))
                .validationTrigger(mock(ValidationTrigger.class))
                .reloadComponent(mock(ReloadComponent.class))
                .verifiableComponentFactory(mock(VerifiableComponentFactory.class))
                .stateManagerProvider(mock(StateManagerProvider.class))
                .extensionManager(extensionManager)
                .buildControllerService();

        serviceProvider.onControllerServiceAdded(serviceNode);
        return serviceNode;
    }
}
