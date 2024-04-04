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
package org.apache.nifi.documentation.example;

import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnRemoved;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.parameter.AbstractParameterProvider;
import org.apache.nifi.parameter.ParameterProvider;
import org.apache.nifi.parameter.ParameterGroup;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@CapabilityDescription("A helper parameter provider to do...")
@Tags({"first", "second", "third"})
@Restricted("parameter provider restriction description")
@SystemResourceConsideration(resource = SystemResource.CPU)
@SystemResourceConsideration(resource = SystemResource.DISK, description = "Customized disk usage description")
@SystemResourceConsideration(resource = SystemResource.MEMORY, description = "")
public class FullyDocumentedParameterProvider extends AbstractParameterProvider implements ParameterProvider {

    public static final PropertyDescriptor INCLUDE_REGEX = new PropertyDescriptor.Builder()
            .name("include-regex")
            .displayName("Include Regex")
            .description("A Regular Expression indicating what to include as parameters.")
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .required(true)
            .defaultValue(".*")
            .build();
    public static final PropertyDescriptor EXCLUDE_REGEX = new PropertyDescriptor.Builder()
            .name("exclude-regex")
            .displayName("Exclude Regex")
            .description("A Regular Expression indicating what to exclude as parameters.")
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .required(false)
            .build();

    private int onRemovedNoArgs = 0;
    private int onRemovedArgs = 0;

    private int onShutdownNoArgs = 0;
    private int onShutdownArgs = 0;

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(INCLUDE_REGEX);
        descriptors.add(EXCLUDE_REGEX);
        return descriptors;
    }

    @OnRemoved
    public void onRemovedNoArgs() {
        onRemovedNoArgs++;
    }

    @OnRemoved
    public void onRemovedArgs(ConfigurationContext context) {
        onRemovedArgs++;
    }

    @OnShutdown
    public void onShutdownNoArgs() {
        onShutdownNoArgs++;
    }

    @OnShutdown
    public void onShutdownArgs(ConfigurationContext context) {
        onShutdownArgs++;
    }

    public int getOnRemovedNoArgs() {
        return onRemovedNoArgs;
    }

    public int getOnRemovedArgs() {
        return onRemovedArgs;
    }

    public int getOnShutdownNoArgs() {
        return onShutdownNoArgs;
    }

    public int getOnShutdownArgs() {
        return onShutdownArgs;
    }

    @Override
    public List<ParameterGroup> fetchParameters(final ConfigurationContext context) {
        return Collections.emptyList();
    }
}
