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
package org.apache.nifi.processors.cassandra;

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.cassandra.api.CQLExecutionService;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * AbstractCassandraProcessor is a base class for Cassandra processors and contains logic and variables common to most
 * processors integrating with Apache Cassandra.
 */
public abstract class AbstractCQLProcessor extends AbstractProcessor {

    // Common descriptors
    static final PropertyDescriptor CONNECTION_PROVIDER_SERVICE = new PropertyDescriptor.Builder()
            .name("cassandra-connection-provider")
            .displayName("Cassandra Connection Provider")
            .description("Specifies the Cassandra connection providing controller service to be used to connect to Cassandra cluster.")
            .required(true)
            .identifiesControllerService(CQLExecutionService.class)
            .build();

    static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
            .name("Character Set")
            .description("Specifies the character set of the record data.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("UTF-8")
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile is transferred to this relationship if the operation completed successfully.")
            .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is transferred to this relationship if the operation failed.")
            .build();

    static final Relationship REL_RETRY = new Relationship.Builder().name("retry")
            .description("A FlowFile is transferred to this relationship if the operation cannot be completed but attempting "
                    + "it again may succeed.")
            .build();

    protected static List<PropertyDescriptor> descriptors = new ArrayList<>();

    static {
        descriptors.add(CONNECTION_PROVIDER_SERVICE);
        descriptors.add(CHARSET);
    }

    protected final AtomicReference<CQLExecutionService> cqlSessionService = new AtomicReference<>(null);

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        CQLExecutionService sessionProvider = context.getProperty(CONNECTION_PROVIDER_SERVICE).asControllerService(CQLExecutionService.class);
        cqlSessionService.set(sessionProvider);
    }

    public void stop(ProcessContext context) {

    }
}

