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
package org.apache.nifi.processors.graph;

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.graph.GraphClientService;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Abstract base class for ExecuteGraphQuery processors
 */
abstract class AbstractGraphExecutor extends AbstractProcessor {
    public static final PropertyDescriptor CLIENT_SERVICE = new PropertyDescriptor.Builder()
        .name("graph-client-service")
        .displayName("Client Service")
        .description("The graph client service for connecting to the graph database.")
        .required(true)
        .identifiesControllerService(GraphClientService.class)
        .build();
    public static final PropertyDescriptor QUERY = new PropertyDescriptor.Builder()
        .name("graph-query")
        .displayName("Graph Query")
        .description("Specifies the graph query. If it is left blank, the processor will attempt " +
                "to get the query from body.")
        .required(false)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("Successful FlowFiles are routed to this relationship").build();

    static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("Failed FlowFiles are routed to this relationship").build();

    static final Relationship REL_ORIGINAL = new Relationship.Builder().name("original")
            .description("If there is an input flowfile, the original input flowfile will be " +
                    "written to this relationship if the operation succeeds.")
            .build();

    public static final String ERROR_MESSAGE = "graph.error.message";

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(String name) {
        return new PropertyDescriptor.Builder()
            .name(name)
            .displayName(name)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    }

    protected List<PropertyDescriptor> queryParameters;

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        queryParameters = context.getProperties()
            .keySet().stream()
            .filter(prop -> prop.isDynamic())
            .collect(Collectors.toList());
    }

    protected Map<String, Object> getParameters(ProcessContext context, FlowFile input) {
        Map<String, Object> params = new HashMap<>();
        for (PropertyDescriptor descriptor : queryParameters) {
            String value = context.getProperty(descriptor).evaluateAttributeExpressions(input).getValue();
            params.put(descriptor.getName(), value);
        }

        return params;
    }
}