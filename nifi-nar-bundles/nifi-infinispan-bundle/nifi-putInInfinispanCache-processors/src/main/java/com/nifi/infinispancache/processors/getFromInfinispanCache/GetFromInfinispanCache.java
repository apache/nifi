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
package com.nifi.infinispancache.processors.getFromInfinispanCache;

import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.FlowFileFilters;
import org.apache.nifi.processor.util.StandardValidators;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.configuration.ServerConfigurationBuilder;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@SupportsBatching
@Tags({"Infinispan", "Cache", "Get"})
@CapabilityDescription("Get value from Infinispan cache")
@ReadsAttributes({@ReadsAttribute(attribute = "", description = "")})
@WritesAttributes({@WritesAttribute(attribute = "", description = "")})
@DynamicProperty(name = "Infinispan host property.", value = "Infinispan Port configuration property.",
        description = "These properties will be added on the Infinispan configuration after loading any provided configuration properties."
                + " In the event a dynamic property represents a property that was already set, its value will be ignored and WARN message logged.",
        expressionLanguageScope = ExpressionLanguageScope.VARIABLE_REGISTRY)
public class GetFromInfinispanCache extends AbstractProcessor {

    public static final PropertyDescriptor INFINISPAN_HOST = new PropertyDescriptor.Builder()
            .name("Infinispan Host")
            .description("Infinispan Host")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor INFINISPAN_PORT = new PropertyDescriptor.Builder()
            .name("Infinispan Port")
            .description("Infinispan Port")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor CACHE_NAME = new PropertyDescriptor.Builder()
            .name("Cache Name")
            .description("Cache Name")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor CACHE_KEY = new PropertyDescriptor.Builder()
            .name("Cache Key")
            .description("Cache Key")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor PROP_PUT_CACHE_VALUE_IN_ATTRIBUTE = new PropertyDescriptor.Builder()
            .name("Put Cache Value In Attribute")
            .description("If set, the cache value received will be put into an attribute of the FlowFile instead of a the content of the"
                    + "FlowFile. The attribute key to put to is determined by evaluating value of this property. If multiple Cache Entry Identifiers are selected, "
                    + "multiple attributes will be written, using the evaluated value of this property, appended by a period (.) and the name of the cache entry identifier.")
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING))
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles that are created are routed to this relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Any FlowFiles that cannot be enriched are routed to this relationship")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    RemoteCacheManager cacheManager = null;
    RemoteCache cache = null;

    @OnScheduled
    public void createInfinispanPool(final ProcessContext context) {
        final ComponentLog logger = getLogger();
        try {
            final String host = context.getProperty(INFINISPAN_HOST).evaluateAttributeExpressions().getValue();
            final int port = Integer.parseInt(context.getProperty(INFINISPAN_PORT).evaluateAttributeExpressions().getValue());
            final String cacheName = context.getProperty(CACHE_NAME).evaluateAttributeExpressions().getValue();
            ServerConfigurationBuilder configuration = new ConfigurationBuilder().addServer().host(host)
                    .port(port);
            cacheManager = new RemoteCacheManager(configuration.socketTimeout(150000).build());
            cache = cacheManager.getCache(cacheName);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    @OnStopped
    public void closeInifinispanPool(final ProcessContext context) {
        final ComponentLog logger = getLogger();
        try {
            if (cache != null) {
                cache.stop();
            }

            if (cacheManager != null) {
                cacheManager.close();
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>(2);
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        return relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(INFINISPAN_HOST);
        descriptors.add(INFINISPAN_PORT);
        descriptors.add(CACHE_NAME);
        descriptors.add(CACHE_KEY);
        descriptors.add(PROP_PUT_CACHE_VALUE_IN_ATTRIBUTE);
        return descriptors;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final ComponentLog logger = getLogger();
        final List<FlowFile> flowFiles = session.get(FlowFileFilters.newSizeBasedFilter(1, DataUnit.MB,
                100));
        try {
            if (flowFiles.isEmpty()) {
                return;
            }
            for (FlowFile flowFile : flowFiles) {
                final String cacheKey = context.getProperty(CACHE_KEY).evaluateAttributeExpressions(flowFile).getValue();
                if (cache.get(cacheKey) != null) {
                    String cacheValue = cache.get(cacheKey).toString();
                    boolean putInAttribute = context.getProperty(PROP_PUT_CACHE_VALUE_IN_ATTRIBUTE).isSet();
                    if (putInAttribute) {
                        String attributeName = context.getProperty(PROP_PUT_CACHE_VALUE_IN_ATTRIBUTE).evaluateAttributeExpressions(flowFile).getValue();
                        flowFile = session.putAttribute(flowFile, attributeName, cacheValue);
                    } else {
                        flowFile = session.write(flowFile, new OutputStreamCallback() {
                            @Override
                            public void process(final OutputStream out) throws IOException {
                                out.write(cacheValue.getBytes());
                            }
                        });
                    }
                    session.transfer(flowFile, REL_SUCCESS);
                } else
                    session.transfer(flowFiles, REL_FAILURE);
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
            session.transfer(flowFiles, REL_FAILURE);
        }
    }
}
