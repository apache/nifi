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
package com.marklogic.nifi.processor;

import com.marklogic.client.DatabaseClient;
import com.marklogic.nifi.controller.DatabaseClientService;
import com.marklogic.nifi.controller.DatabaseClientService;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Defines common properties for MarkLogic processors.
 */
public abstract class AbstractMarkLogicProcessor extends AbstractSessionFactoryProcessor {

    protected List<PropertyDescriptor> properties;
    protected Set<Relationship> relationships;

    // NiFi requires a validator for every property, even those that don't need any validation
    protected static Validator NO_VALIDATION_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(String subject, String input, ValidationContext context) {
            return new ValidationResult.Builder().valid(true).build();
        }
    };

    public static final PropertyDescriptor DATABASE_CLIENT_SERVICE = new PropertyDescriptor.Builder()
        .name("DatabaseClient Service")
        .required(true)
        .description("The DatabaseClient Controller Service that provides the MarkLogic connection")
        .identifiesControllerService(DatabaseClientService.class)
        .build();

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
        .name("Batch size")
        .required(true)
        .defaultValue("100")
        .description("The number of documents per batch - sets the batch size on the Batcher")
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .build();

    public static final PropertyDescriptor THREAD_COUNT = new PropertyDescriptor.Builder()
        .name("Thread count")
        .required(true)
        .defaultValue("16")
        .description("The number of threads - sets the thread count on the Batcher")
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .build();

    @Override
    public void init(final ProcessorInitializationContext context) {
        List<PropertyDescriptor> list = new ArrayList<>();
        list.add(DATABASE_CLIENT_SERVICE);
        list.add(BATCH_SIZE);
        list.add(THREAD_COUNT);
        properties = Collections.unmodifiableList(list);
    }

    protected DatabaseClient getDatabaseClient(ProcessContext context) {
        return context.getProperty(DATABASE_CLIENT_SERVICE)
            .asControllerService(DatabaseClientService.class)
            .getDatabaseClient();
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

}
