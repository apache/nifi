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
package org.apache.nifi.controller.python;

import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.nar.StandardExtensionDiscoveringManager;
import org.apache.nifi.nar.SystemBundle;
import org.apache.nifi.python.DisabledPythonBridge;
import org.apache.nifi.python.PythonBridge;
import org.apache.nifi.util.NiFiProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Answers.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;

public class TestPythonBridgeFactory {

    private NiFiProperties properties;
    private StandardExtensionDiscoveringManager extensionManager;
    private ControllerServiceProvider serviceProvider;

    @Test
    public void shouldReturnDisabledPythonBridgeIfPythonSupportNotEnabled() {
        giveANifiProperties(Map.of(NiFiProperties.PYTHON_SUPPORT_ENABLED, "false"));

        PythonBridge bridge = PythonBridgeFactory.createPythonBridge(properties, null, null);

        Assertions.assertInstanceOf(DisabledPythonBridge.class, bridge, "Python bridge should be disabled");
    }

    @Test
    public void shouldThrowExceptionIfPythonCommandIsNotDefined() {
        giveANifiProperties(Map.of(NiFiProperties.PYTHON_SUPPORT_ENABLED, "true"));

        assertThrows(RuntimeException.class, () -> PythonBridgeFactory.createPythonBridge(properties, null, null));
    }

    @Test
    public void shouldCreatePythonBridege() throws ClassNotFoundException, InvocationTargetException, InstantiationException, IllegalAccessException, NoSuchMethodException {
        giveANifiProperties(Map.of(NiFiProperties.PYTHON_SUPPORT_ENABLED, "true",
                NiFiProperties.PYTHON_COMMAND, "python3",
                NiFiProperties.PYTHON_WORKING_DIRECTORY, "python_working_dir",
                NiFiProperties.PYTHON_LOGS_DIRECTORY, "python_log_dir"));
        giveAnExtensionManager();
        giveAControllerServiceProvider();

        PythonBridge bridge = PythonBridgeFactory.createPythonBridge(properties, serviceProvider, extensionManager);
    }

    private void giveANifiProperties(Map<String, String> properties) {
        this.properties = NiFiProperties.createBasicNiFiProperties("src/test/resources/pythonbridgefactory/pythonbridegefactorytest.nifi.properties", properties);
    }

    private void giveAnExtensionManager() {
        Bundle systemBundle = SystemBundle.create(properties);
        extensionManager = new StandardExtensionDiscoveringManager();
        extensionManager.discoverExtensions(systemBundle, Collections.emptySet());
    }

    private void giveAControllerServiceProvider() {
        serviceProvider = mock(ControllerServiceProvider.class, RETURNS_MOCKS);
    }

}
