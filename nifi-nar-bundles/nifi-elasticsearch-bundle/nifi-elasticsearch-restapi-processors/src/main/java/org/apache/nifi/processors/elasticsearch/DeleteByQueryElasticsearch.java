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

package org.apache.nifi.processors.elasticsearch;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.elasticsearch.DeleteOperationResponse;
import org.apache.nifi.elasticsearch.ElasticSearchClientService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@CapabilityDescription("Delete from an ElasticSearch index using a query. The query can be loaded from a flowfile body " +
        "or from the Query parameter.")
@Tags({ "elastic", "elasticsearch", "delete", "query"})
@WritesAttributes({
    @WritesAttribute(attribute = "elasticsearch.delete.took", description = "The amount of time that it took to complete the delete operation in ms."),
    @WritesAttribute(attribute = "elasticsearch.delete.error", description = "The error message provided by ElasticSearch if there is an error running the delete.")
})
public class DeleteByQueryElasticsearch extends AbstractProcessor implements ElasticSearchRestProcessor {
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
        .description("If the delete by query fails, and a flowfile was read, it will be sent to this relationship.").build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
        .description("If the delete by query succeeds, and a flowfile was read, it will be sent to this relationship.")
        .build();


    static final String TOOK_ATTRIBUTE = "elasticsearch.delete.took";
    static final String ERROR_ATTRIBUTE = "elasticsearch.delete.error";

    private static final Set<Relationship> relationships;
    private static final List<PropertyDescriptor> propertyDescriptors;

    private volatile ElasticSearchClientService clientService;

    static {
        final Set<Relationship> _rels = new HashSet<>();
        _rels.add(REL_SUCCESS);
        _rels.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(_rels);

        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(QUERY);
        descriptors.add(QUERY_ATTRIBUTE);
        descriptors.add(INDEX);
        descriptors.add(TYPE);
        descriptors.add(CLIENT_SERVICE);

        propertyDescriptors = Collections.unmodifiableList(descriptors);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        clientService = context.getProperty(CLIENT_SERVICE).asControllerService(ElasticSearchClientService.class);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile input = null;
        if (context.hasIncomingConnection()) {
            input = session.get();

            if (input == null && context.hasNonLoopConnection()) {
                return;
            }
        }

        try {
            final String query = getQuery(input, context, session);
            final String index = context.getProperty(INDEX).evaluateAttributeExpressions(input).getValue();
            final String type  = context.getProperty(TYPE).isSet()
                    ? context.getProperty(TYPE).evaluateAttributeExpressions(input).getValue()
                    : null;
            final String queryAttr = context.getProperty(QUERY_ATTRIBUTE).isSet()
                    ? context.getProperty(QUERY_ATTRIBUTE).evaluateAttributeExpressions(input).getValue()
                    : null;
            DeleteOperationResponse dor = clientService.deleteByQuery(query, index, type);

            if (input == null) {
                input = session.create();
            }

            Map<String, String> attrs = new HashMap<>();
            attrs.put(TOOK_ATTRIBUTE, String.valueOf(dor.getTook()));
            if (!StringUtils.isBlank(queryAttr)) {
                attrs.put(queryAttr, query);
            }

            input = session.putAllAttributes(input, attrs);

            session.transfer(input, REL_SUCCESS);
        } catch (Exception e) {
            if (input != null) {
                input = session.putAttribute(input, ERROR_ATTRIBUTE, e.getMessage());
                session.transfer(input, REL_FAILURE);
            }
            getLogger().error("Error running delete by query: ", e);
            context.yield();
        }
    }
}
