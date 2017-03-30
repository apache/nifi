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

package org.apache.nifi.serialization;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schemaregistry.services.SchemaRegistry;
import org.apache.nifi.serialization.record.RecordSchema;

public abstract class SchemaRegistryRecordReader extends AbstractControllerService {

    protected static final PropertyDescriptor REQUIRED_SCHEMA_REGISTRY = new PropertyDescriptor.Builder()
        .name("Schema Registry")
        .description("Specifies the Controller Service to use for the Schema Registry")
        .identifiesControllerService(SchemaRegistry.class)
        .required(true)
        .build();

    protected static final PropertyDescriptor OPTIONAL_SCHEMA_REGISTRY = new PropertyDescriptor.Builder()
        .fromPropertyDescriptor(REQUIRED_SCHEMA_REGISTRY)
        .required(false)
        .build();

    protected static final PropertyDescriptor REQUIRED_SCHEMA_NAME = new PropertyDescriptor.Builder()
        .name("Schema Name")
        .description("Name of the Schema that is stored in the Schema Registry")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(true)
        .required(true)
        .build();

    protected static final PropertyDescriptor OPTIONAL_SCHEMA_NAME = new PropertyDescriptor.Builder()
        .fromPropertyDescriptor(REQUIRED_SCHEMA_NAME)
        .required(false)
        .build();


    private volatile SchemaRegistry schemaRegistry;
    private volatile PropertyValue schemaName;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(2);
        if (isSchemaRequired()) {
            properties.add(REQUIRED_SCHEMA_REGISTRY);
            properties.add(REQUIRED_SCHEMA_NAME);
        } else {
            properties.add(OPTIONAL_SCHEMA_REGISTRY);
            properties.add(OPTIONAL_SCHEMA_NAME);
        }

        return properties;
    }

    @OnEnabled
    public void storeRegistryValues(final ConfigurationContext context) {
        schemaRegistry = context.getProperty(REQUIRED_SCHEMA_REGISTRY).asControllerService(SchemaRegistry.class);
        schemaName = context.getProperty(REQUIRED_SCHEMA_NAME);
    }

    public RecordSchema getSchema(final FlowFile flowFile) {
        final String evaluatedSchemaName = schemaName.evaluateAttributeExpressions(flowFile).getValue();
        final RecordSchema schema = schemaRegistry.retrieveSchema(evaluatedSchemaName);
        return schema;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        if (validationContext.getProperty(OPTIONAL_SCHEMA_REGISTRY).isSet() && !validationContext.getProperty(OPTIONAL_SCHEMA_NAME).isSet()) {
            return Collections.singleton(new ValidationResult.Builder()
                .subject("Schema Registry")
                .explanation("If the Schema Registry is configured, the Schema name must also be configured")
                .valid(false)
                .build());
        }

        return Collections.emptyList();
    }

    protected boolean isSchemaRequired() {
        return true;
    }
}
