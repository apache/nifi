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

package org.apache.nifi.processors.tests.system;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.cs.tests.system.StateBackedStoreService;
import org.apache.nifi.cs.tests.system.StoreService;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Post-upgrade shape of a processor whose property migration creates the store Controller Service
 * that the pre-upgrade shape did not have. Each execution appends a row to the store so that tests
 * can observe whether store contents survive flow and runtime upgrades.
 */
@InputRequirement(Requirement.INPUT_FORBIDDEN)
public class MigrateToControllerService extends AbstractProcessor {

    static final PropertyDescriptor STORE_SERVICE = new PropertyDescriptor.Builder()
            .name("Store Service")
            .required(false)
            .identifiesControllerService(StoreService.class)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").build();

    private static final List<PropertyDescriptor> PROPERTIES = List.of(STORE_SERVICE);
    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS);

    private final AtomicLong rowCounter = new AtomicLong(0L);

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public void migrateProperties(final PropertyConfiguration config) {
        final String storeName = config.getPropertyValue("store-name").orElse(null);
        config.removeProperty("store-name");

        if (storeName != null) {
            final String serviceId = config.createControllerService(StateBackedStoreService.class.getName(), Map.of("Store Name", storeName));
            config.setProperty(STORE_SERVICE, serviceId);
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final StoreService storeService = context.getProperty(STORE_SERVICE).asControllerService(StoreService.class);
        if (storeService == null) {
            context.yield();
            return;
        }

        storeService.append("row-" + rowCounter.getAndIncrement());
    }

}
