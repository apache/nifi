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
package org.apache.nifi.init;

import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.mock.MockComponentLogger;
import org.apache.nifi.mock.MockConfigurationContext;
import org.apache.nifi.mock.MockControllerServiceInitializationContext;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.reporting.InitializationException;

/**
 * Initializes a ControllerService using a MockControllerServiceInitializationContext
 *
 *
 */
public class ControllerServiceInitializer implements ConfigurableComponentInitializer {

    @Override
    public void initialize(ConfigurableComponent component) throws InitializationException {
        ControllerService controllerService = (ControllerService) component;
        ControllerServiceInitializationContext context = new MockControllerServiceInitializationContext();
        try (NarCloseable narCloseable = NarCloseable.withComponentNarLoader(component.getClass(), context.getIdentifier())) {
            controllerService.initialize(context);
        }
    }

    @Override
    public void teardown(ConfigurableComponent component) {
        try (NarCloseable narCloseable = NarCloseable.withComponentNarLoader(component.getClass(), component.getIdentifier())) {
            ControllerService controllerService = (ControllerService) component;

            final ComponentLog logger = new MockComponentLogger();
            final MockConfigurationContext context = new MockConfigurationContext();
            ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnShutdown.class, controllerService, logger, context);
        } finally {
            ExtensionManager.removeInstanceClassLoader(component.getIdentifier());
        }
    }
}
