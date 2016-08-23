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
package org.apache.nifi.documentation.init;

import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.documentation.ConfigurableComponentInitializer;
import org.apache.nifi.documentation.mock.MockProcessContext;
import org.apache.nifi.documentation.mock.MockProcessorInitializationContext;
import org.apache.nifi.documentation.mock.MockComponentLogger;
import org.apache.nifi.documentation.util.ReflectionUtils;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.processor.Processor;

/**
 * Initializes a Procesor using a MockProcessorInitializationContext
 *
 *
 */
public class ProcessorInitializer implements ConfigurableComponentInitializer {

    @Override
    public void initialize(ConfigurableComponent component) {
        Processor processor = (Processor) component;
        try (NarCloseable narCloseable = NarCloseable.withComponentNarLoader(component.getClass())) {
            processor.initialize(new MockProcessorInitializationContext());
        }
    }

    @Override
    public void teardown(ConfigurableComponent component) {
        Processor processor = (Processor) component;
        try (NarCloseable narCloseable = NarCloseable.withComponentNarLoader(component.getClass())) {

            final ComponentLog logger = new MockComponentLogger();
            final MockProcessContext context = new MockProcessContext();
            ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnShutdown.class, processor, logger, context);
        }
    }
}
