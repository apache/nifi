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
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.mock.MockComponentLogger;
import org.apache.nifi.mock.MockProcessContext;
import org.apache.nifi.mock.MockProcessorInitializationContext;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.ProcessorInitializationContext;

/**
 * Initializes a Processor using a MockProcessorInitializationContext
 *
 *
 */
public class ProcessorInitializer implements ConfigurableComponentInitializer {

    private final ExtensionManager extensionManager;

    public ProcessorInitializer(final ExtensionManager extensionManager) {
        this.extensionManager = extensionManager;
    }

    @Override
    public void initialize(ConfigurableComponent component) {
        Processor processor = (Processor) component;
        ProcessorInitializationContext initializationContext = new MockProcessorInitializationContext();
        try (NarCloseable narCloseable = NarCloseable.withComponentNarLoader(extensionManager, component.getClass(), initializationContext.getIdentifier())) {
            processor.initialize(initializationContext);
        }
    }

    @Override
    public void teardown(ConfigurableComponent component) {
        Processor processor = (Processor) component;
        try (NarCloseable narCloseable = NarCloseable.withComponentNarLoader(extensionManager, component.getClass(), component.getIdentifier())) {

            final ComponentLog logger = new MockComponentLogger();
            final MockProcessContext context = new MockProcessContext();
            ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnShutdown.class, processor, logger, context);
        } finally {
            extensionManager.removeInstanceClassLoader(component.getIdentifier());
        }
    }
}
