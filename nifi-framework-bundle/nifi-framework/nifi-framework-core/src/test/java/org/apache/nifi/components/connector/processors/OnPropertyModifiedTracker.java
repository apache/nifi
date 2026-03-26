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

package org.apache.nifi.components.connector.processors;

import org.apache.nifi.annotation.lifecycle.OnConfigurationRestored;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * A test processor that tracks calls to onPropertyModified when the Configured Number property changes.
 * This is used to verify that onPropertyModified is called correctly when a Connector's applyUpdate
 * changes a parameter value.
 */
public class OnPropertyModifiedTracker extends AbstractProcessor {

    static final PropertyDescriptor CONFIGURED_NUMBER = new PropertyDescriptor.Builder()
            .name("Configured Number")
            .displayName("Configured Number")
            .description("A number property that is tracked for changes via onPropertyModified")
            .required(false)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles are routed here")
            .build();

    private volatile boolean configurationRestored = false;
    private final List<PropertyChange> propertyChanges = Collections.synchronizedList(new ArrayList<>());

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return List.of(CONFIGURED_NUMBER);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return Set.of(REL_SUCCESS);
    }

    @OnConfigurationRestored
    public void onConfigurationRestored() {
        this.configurationRestored = true;
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (!CONFIGURED_NUMBER.getName().equals(descriptor.getName())) {
            return;
        }

        if (!configurationRestored) {
            return;
        }

        if (oldValue == null) {
            getLogger().info("Property [{}] initialized to [{}]", descriptor.getName(), newValue);
            return;
        }

        getLogger().info("Property [{}] changed from [{}] to [{}]", descriptor.getName(), oldValue, newValue);
        propertyChanges.add(new PropertyChange(oldValue, newValue));
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        session.transfer(flowFile, REL_SUCCESS);
    }

    public List<PropertyChange> getPropertyChanges() {
        return new ArrayList<>(propertyChanges);
    }

    public int getPropertyChangeCount() {
        return propertyChanges.size();
    }

    public void clearPropertyChanges() {
        propertyChanges.clear();
    }

    public record PropertyChange(String oldValue, String newValue) {
    }
}
