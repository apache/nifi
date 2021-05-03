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
package org.apache.nifi.accumulo.processors;

import com.google.common.collect.ImmutableList;
import org.apache.nifi.accumulo.controllerservices.BaseAccumuloService;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.util.StandardValidators;

/**
 * Base Accumulo class that provides connector services, table name, and thread
 * properties
 */
public abstract class BaseAccumuloProcessor extends AbstractProcessor {

    protected static final PropertyDescriptor ACCUMULO_CONNECTOR_SERVICE = new PropertyDescriptor.Builder()
            .name("accumulo-connector-service")
            .displayName("Accumulo Connector Service")
            .description("Specifies the Controller Service to use for accessing Accumulo.")
            .required(true)
            .identifiesControllerService(BaseAccumuloService.class)
            .build();


    protected static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("Table Name")
            .description("The name of the Accumulo Table into which data will be placed")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor CREATE_TABLE = new PropertyDescriptor.Builder()
            .name("Create Table")
            .description("Creates a table if it does not exist. This property will only be used when EL is not present in 'Table Name'")
            .required(true)
            .defaultValue("False")
            .allowableValues("True", "False")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();


    protected static final PropertyDescriptor THREADS = new PropertyDescriptor.Builder()
            .name("Threads")
            .description("Number of threads used for reading and writing")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("10")
            .build();

    protected static final PropertyDescriptor ACCUMULO_TIMEOUT = new PropertyDescriptor.Builder()
            .name("accumulo-timeout")
            .displayName("Accumulo Timeout")
            .description("Max amount of time to wait for an unresponsive server. Set to 0 sec for no timeout. Entered value less than 1 second may be converted to 0 sec.")
            .required(false)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("30 sec")
            .build();

    /**
     * Implementations can decide to include all base properties or individually include them. List is immutable
     * so that implementations must constructor their own lists knowingly
     */

    protected static final ImmutableList<PropertyDescriptor> baseProperties = ImmutableList.of(ACCUMULO_CONNECTOR_SERVICE,TABLE_NAME,CREATE_TABLE,THREADS,ACCUMULO_TIMEOUT);


}
