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
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarClassLoaders;
import org.apache.nifi.nar.SystemBundle;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.registry.variable.FileBasedVariableRegistry;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.SynchronousValidationTrigger;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

public class StandardControllerServiceProviderFailedInitTest {

    private static VariableRegistry variableRegistry;
    private static NiFiProperties nifiProperties;
    private static Bundle systemBundle;

    @BeforeClass
    public static void setupSuite() throws Exception {
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, StandardControllerServiceProviderFailedInitTest.class.getResource("/conf/nifi.properties").getFile());
        nifiProperties = NiFiProperties.createBasicNiFiProperties(null, null);

        NarClassLoaders.getInstance().init(nifiProperties.getFrameworkWorkingDirectory(), nifiProperties.getExtensionsWorkingDirectory());

        // load the system bundle
        systemBundle = SystemBundle.create(nifiProperties);
        ExtensionManager.discoverExtensions(systemBundle, NarClassLoaders.getInstance().getBundles());

        variableRegistry = new FileBasedVariableRegistry(nifiProperties.getVariableRegistryPropertiesPaths());
    }

    @Test
    public void testGhostControllerService() throws Exception {
        String id = "id";
        String clazz = "org.apache.nifi.controller.service.util.FailedInitControllerService";
        ControllerServiceProvider provider = new StandardControllerServiceProvider(null, null, null, new StateManagerProvider() {
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
            public void onComponentRemoved(String componentId) {
            }
        }, variableRegistry, nifiProperties, new SynchronousValidationTrigger());
        ControllerServiceNode node = provider.createControllerService(clazz, id, systemBundle.getBundleDetails().getCoordinate(), null, true);

        Assert.assertTrue(node instanceof StandardControllerServiceNode);
        Assert.assertThat(node.getComponentType(), CoreMatchers.startsWith(StandardControllerServiceProvider.PRE_MISSING));
        Assert.assertThat(node.toString(), CoreMatchers.startsWith("GhostControllerService"));
    }

}
