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
package org.apache.nifi.processors.cypher;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.cypher.CypherClientService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@EventDriven
@SupportsBatching
@Tags({"cypher", "neo4j", "graph", "network", "insert", "update", "delete", "put", "get",
        "node", "relationship", "connection", "executor"})
@CapabilityDescription("This processor is designed to run queries written in the Cypher query language. Cypher is " +
        "a SQL-like language for querying graph databases. For more information, see: " +
        "https://neo4j.com/developer/cypher-query-language/")
@WritesAttributes({
    @WritesAttribute(attribute = AbstractCypherExecutor.ERROR_MESSAGE, description = "Cypher error message"),
    @WritesAttribute(attribute = CypherClientService.LABELS_ADDED, description = "Number of labels added"),
    @WritesAttribute(attribute = CypherClientService.NODES_CREATED, description = "Number of nodes created"),
    @WritesAttribute(attribute = CypherClientService.NODES_DELETED, description = "Number of nodes deleted"),
    @WritesAttribute(attribute = CypherClientService.PROPERTIES_SET, description = "Number of properties set"),
    @WritesAttribute(attribute = CypherClientService.RELATIONS_CREATED, description = "Number of relationships created"),
    @WritesAttribute(attribute = CypherClientService.RELATIONS_DELETED, description = "Number of relationships deleted"),
    @WritesAttribute(attribute = CypherClientService.ROWS_RETURNED, description = "Number of rows returned"),
    })
public class ExecuteCypherQuery extends AbstractCypherExecutor {

    private static final Set<Relationship> relationships;
    private static final List<PropertyDescriptor> propertyDescriptors;

    static {
        final Set<Relationship> tempRelationships = new HashSet<>();
        tempRelationships.add(REL_SUCCESS);
        tempRelationships.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(tempRelationships);

        final List<PropertyDescriptor> tempDescriptors = new ArrayList<>();
        tempDescriptors.add(CLIENT_SERVICE);
        tempDescriptors.add(QUERY);

        propertyDescriptors = Collections.unmodifiableList(tempDescriptors);
    }

    protected ObjectMapper mapper = new ObjectMapper();

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    private volatile CypherClientService clientService;

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        super.onScheduled(context);
        clientService = context.getProperty(CLIENT_SERVICE).asControllerService(CypherClientService.class);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        String query = context.getProperty(QUERY).evaluateAttributeExpressions(flowFile).getValue();

        try (OutputStream os = session.write(flowFile)) {
            long startTimeMillis = System.currentTimeMillis();

            os.write("[".getBytes());
            Map<String, String> resultAttrs = clientService.executeQuery(query, getParameters(context, flowFile), (record, hasMore) -> {
                try {
                    String obj = mapper.writeValueAsString(record);
                    os.write(obj.getBytes());
                    if (hasMore) {
                        os.write(",".getBytes());
                    }
                } catch (Exception ex) {
                    throw new ProcessException(ex);
                }
            });
            os.write("]".getBytes());
            os.close();

            final long endTimeMillis = System.currentTimeMillis();

            flowFile = session.putAllAttributes(flowFile, resultAttrs);

            session.transfer(flowFile, REL_SUCCESS);
            session.getProvenanceReporter().send(flowFile, clientService.getTransitUrl(), (endTimeMillis - startTimeMillis));
        } catch (Exception exception) {
            getLogger().error("Failed to execute Cypher statement due to {}",
                    new Object[]{exception.getLocalizedMessage()}, exception);
            flowFile = session.putAttribute(flowFile, ERROR_MESSAGE, String.valueOf(exception.getMessage()));
            session.transfer(flowFile, REL_FAILURE);
            context.yield();
        }
    }
}