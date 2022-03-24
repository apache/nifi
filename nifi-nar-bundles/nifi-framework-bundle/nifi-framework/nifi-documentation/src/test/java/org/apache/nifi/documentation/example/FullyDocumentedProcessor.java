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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.DynamicRelationship;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnRemoved;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

@Tags({"one", "two", "three"})
@CapabilityDescription("This is a processor that is used to test documentation.")
@WritesAttributes({
    @WritesAttribute(attribute = "first", description = "this is the first attribute i write"),
    @WritesAttribute(attribute = "second")})
@ReadsAttribute(attribute = "incoming", description = "this specifies the format of the thing")
@SeeAlso(value = {FullyDocumentedControllerService.class, FullyDocumentedReportingTask.class}, classNames = {"org.apache.nifi.processor.ExampleProcessor"})
@DynamicProperty(name = "Relationship Name", expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
                    value = "some XPath", description = "Routes FlowFiles to relationships based on XPath")
@DynamicRelationship(name = "name from dynamic property", description = "all files that match the properties XPath")
@Stateful(scopes = {Scope.CLUSTER, Scope.LOCAL}, description = "state management description")
@Restricted(
        value = "processor restriction description",
        restrictions = {
                @Restriction(
                        requiredPermission = RequiredPermission.READ_FILESYSTEM,
                        explanation = "Requires read filesystem permission")
        }
)
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@SystemResourceConsideration(resource = SystemResource.CPU)
@SystemResourceConsideration(resource = SystemResource.DISK, description = "Customized disk usage description")
@SystemResourceConsideration(resource = SystemResource.MEMORY, description = "")
public class FullyDocumentedProcessor extends AbstractProcessor {

    public static final PropertyDescriptor DIRECTORY = new PropertyDescriptor.Builder()
            .name("Input Directory")
            .description("The input directory from which to pull files")
            .required(true)
            .addValidator(StandardValidators.createDirectoryExistsValidator(true, false))
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor RECURSE = new PropertyDescriptor.Builder()
            .name("Recurse Subdirectories")
            .description("Indicates whether or not to pull files from subdirectories")
            .required(true)
            .allowableValues(
                    new AllowableValue("true", "true", "Should pull from sub directories"),
                    new AllowableValue("false", "false", "Should not pull from sub directories")
            )
            .defaultValue("true")
            .build();

    public static final PropertyDescriptor POLLING_INTERVAL = new PropertyDescriptor.Builder()
            .name("Polling Interval")
            .description("Indicates how long to wait before performing a directory listing")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("0 sec")
            .build();

    @SuppressWarnings("deprecation")
    public static final PropertyDescriptor OPTIONAL_PROPERTY = new PropertyDescriptor.Builder()
            .name("Optional Property")
            .description("This is a property you can use or not")
            .required(false)
            .expressionLanguageSupported(true) // test documentation of deprecated method
            .build();

    @SuppressWarnings("deprecation")
    public static final PropertyDescriptor TYPE_PROPERTY = new PropertyDescriptor.Builder()
            .name("Type")
            .description("This is the type of something that you can choose.  It has several possible values")
            .allowableValues("yes", "no", "maybe", "possibly", "not likely", "longer option name")
            .required(true)
            .expressionLanguageSupported(false) // test documentation of deprecated method
            .build();

    public static final PropertyDescriptor SERVICE_PROPERTY = new PropertyDescriptor.Builder()
            .name("Controller Service")
            .description("This is the controller service to use to do things")
            .identifiesControllerService(SampleService.class)
            .required(true)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successful files")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failing files")
            .build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    private int onRemovedNoArgs = 0;
    private int onRemovedArgs = 0;

    private int onShutdownNoArgs = 0;
    private int onShutdownArgs = 0;

    @Override
    protected void init(ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(DIRECTORY);
        properties.add(RECURSE);
        properties.add(POLLING_INTERVAL);
        properties.add(OPTIONAL_PROPERTY);
        properties.add(TYPE_PROPERTY);
        properties.add(SERVICE_PROPERTY);
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(String propertyDescriptorName) {
        return new PropertyDescriptor.Builder().name(propertyDescriptorName)
                .description("This is a property you can use or not").dynamic(true).build();
    }

    @OnRemoved
    public void onRemovedNoArgs() {
        onRemovedNoArgs++;
    }

    @OnRemoved
    public void onRemovedArgs(ProcessContext context) {
        onRemovedArgs++;
    }

    @OnShutdown
    public void onShutdownNoArgs() {
        onShutdownNoArgs++;
    }

    @OnShutdown
    public void onShutdownArgs(ProcessContext context) {
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
}
