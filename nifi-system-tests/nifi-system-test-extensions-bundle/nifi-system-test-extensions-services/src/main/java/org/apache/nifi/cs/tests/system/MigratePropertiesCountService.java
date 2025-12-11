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
package org.apache.nifi.cs.tests.system;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class MigratePropertiesCountService extends AbstractControllerService implements CountService {
    private static final String PREVIOUS_START_VALUE_PROPERTY_NAME = "start-value";

    private static final String PREVIOUS_COUNT_SERVICE_PROPERTY_NAME = "count-service";

    static final PropertyDescriptor START_VALUE = new PropertyDescriptor.Builder()
            .name("Start Value")
            .description("Initial value for counting")
            .required(true)
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .defaultValue("0")
            .build();

    static final PropertyDescriptor COUNT_SERVICE = new PropertyDescriptor.Builder()
            .name("Count Service")
            .description("Count Service for testing Controller Service dependencies")
            .required(false)
            .identifiesControllerService(CountService.class)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            START_VALUE,
            COUNT_SERVICE
    );

    private final AtomicLong counter = new AtomicLong();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public void migrateProperties(final PropertyConfiguration propertyConfiguration) {
        propertyConfiguration.renameProperty(PREVIOUS_START_VALUE_PROPERTY_NAME, START_VALUE.getName());
        propertyConfiguration.renameProperty(PREVIOUS_COUNT_SERVICE_PROPERTY_NAME, COUNT_SERVICE.getName());
    }

    @Override
    public long count() {
        return counter.getAndIncrement();
    }
}
