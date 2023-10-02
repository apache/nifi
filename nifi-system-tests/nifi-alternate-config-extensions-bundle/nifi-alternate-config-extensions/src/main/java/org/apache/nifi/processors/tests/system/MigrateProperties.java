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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.cs.tests.system.MigrationService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.migration.RelationshipConfiguration;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class MigrateProperties extends AbstractProcessor {

    static PropertyDescriptor INGEST = new PropertyDescriptor.Builder()
            .name("Ingest Data")
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(true)
            .build();

    static PropertyDescriptor ATTRIBUTE_NAME = new PropertyDescriptor.Builder()
            .name("Attribute to add")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static PropertyDescriptor ATTRIBUTE_VALUE = new PropertyDescriptor.Builder()
            .name("Attribute Value")
            .required(true)
            .dependsOn(ATTRIBUTE_NAME)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static PropertyDescriptor SERVICE = new PropertyDescriptor.Builder()
            .name("Service")
            .required(false)
            .identifiesControllerService(ControllerService.class)
            .build();

    static Relationship REL_ODD = new Relationship.Builder().name("odd").build();
    static Relationship REL_EVEN = new Relationship.Builder().name("even").build();
    static Relationship REL_BROKEN = new Relationship.Builder().name("broken").build();

    private static final Set<Relationship> relationships = Set.of(REL_ODD, REL_EVEN, REL_BROKEN);
    private static final List<PropertyDescriptor> properties = List.of(
            INGEST,
            ATTRIBUTE_NAME,
            ATTRIBUTE_VALUE,
            SERVICE
    );

    private final AtomicLong counter = new AtomicLong(0L);


    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public void migrateProperties(final PropertyConfiguration config) {
        config.renameProperty("ingest-data", INGEST.getName());
        config.renameProperty("attr-to-add", ATTRIBUTE_NAME.getName());
        config.renameProperty("attr-value", ATTRIBUTE_VALUE.getName());
        config.renameProperty("never-existed", "still-doesnt-exist");
        config.setProperty("New Property", config.getPropertyValue(INGEST).orElse("New Value"));
        final String ignoredValue = config.getPropertyValue("ignored").orElse(null);
        config.removeProperty("ignored");

        // If the 'ignored' value was set, create a new Controller Service whose Start value is set to that value.
        if (ignoredValue != null && ignoredValue.matches("\\d+")) {
            final String serviceId = config.createControllerService(MigrationService.class.getName(), Map.of("Start", ignoredValue));
            config.setProperty(SERVICE, serviceId);
        }
    }

    @Override
    public void migrateRelationships(final RelationshipConfiguration config) {
        config.renameRelationship("failure", REL_BROKEN.getName());
        config.splitRelationship("success", REL_ODD.getName(), REL_EVEN.getName());
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String attributeValue = context.getProperty(ATTRIBUTE_VALUE).getValue();
        if (attributeValue != null) {
            final String attributeName = context.getProperty(ATTRIBUTE_NAME).getValue();
            flowFile = session.putAttribute(flowFile, attributeName, attributeValue);
        }

        final long count = counter.getAndIncrement();
        final Relationship relationship = count % 2 == 0 ? REL_EVEN : REL_ODD;
        session.transfer(flowFile, relationship);
    }

}
