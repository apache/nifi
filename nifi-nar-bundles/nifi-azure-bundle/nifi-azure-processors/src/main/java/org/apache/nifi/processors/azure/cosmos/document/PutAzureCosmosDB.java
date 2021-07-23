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
package org.apache.nifi.processors.azure.cosmos.document;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.configuration.DefaultSettings;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.stream.io.StreamUtils;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

@EventDriven
@Tags({ "azure", "cosmos", "document", "insert", "put" })
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Writes schema-less json document to Azure Cosmos DB with Cosmos DB with Core SQL API.")
@SystemResourceConsideration(resource = SystemResource.MEMORY)
@DefaultSettings(yieldDuration = "30 sec", penaltyDuration = "1 min")
public class PutAzureCosmosDB extends AbstractAzureCosmosDBProcessor {

    private final static Set<Relationship> relationships;
    private final static List<PropertyDescriptor> propertyDescriptors;

    private final static ObjectMapper mapper;

    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.addAll(descriptors);
        _propertyDescriptors.add(CHARACTER_SET);
        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);

        final Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(_relationships);
        mapper = new ObjectMapper();
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }


    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();

        final Charset charset = Charset.forName(context.getProperty(CHARACTER_SET).getValue());
        final String partitionKeyField = context.getProperty(PARTITION_KEY).getValue();
        JsonNode doc = null;
        boolean error = false, yield = false;
        try {
            // Read the contents of the FlowFile into a byte array
            final byte[] content = new byte[(int) flowFile.getSize()];
            session.read(flowFile, in -> StreamUtils.fillBuffer(in, content, true));

            // parse conten into JsonNode object
            doc = mapper.readTree(new String(content, charset));
            // make sure type of id is String type if exists

            if(doc.has("id")) {
                JsonNode idNode = doc.get("id");
                if(idNode.isNumber()) {
                    //coverting number id into string...
                    ((ObjectNode) doc).put("id", doc.get("id").asText());
                }
            } else {
                ((ObjectNode) doc).put("id", UUID.randomUUID().toString());
            }
            if(!doc.has(partitionKeyField)){
                error = true;
                logger.error("Required parition key field value not found from input flow file content with doc.id=" + doc.get("id"));
            } else {
                // insert/update doc with upsertItem API
                final CosmosContainer container = getContainer();
                final JsonNode partitionKeyNode = doc.get(partitionKeyField);
                final String partitionKeyVal = partitionKeyNode.asText();
                final CosmosItemRequestOptions cosmosItemRequestOptions = new CosmosItemRequestOptions();
                cosmosItemRequestOptions.setContentResponseOnWriteEnabled(false);
                if (!StringUtils.isBlank(partitionKeyVal)) {
                    container.upsertItem(doc, new PartitionKey(partitionKeyVal), cosmosItemRequestOptions);
                } else {
                    container.upsertItem(doc, cosmosItemRequestOptions);
                }
            }
        } catch (CosmosException e)  {
            final int statusCode =  e.getStatusCode();
            logger.error("statusCode: " + statusCode + ", subStatusCode: "  + e.getSubStatusCode());

            if (statusCode == 429) {
                logger.error("Failure due to server-side throttling. Increase RU setting for your workload");
                yield = true;
            } else if (statusCode == 410) {
                logger.error("A request to change the throughput is currently in progress");
                yield = true;
            } else {
                error = true;
            }
        } catch (JsonProcessingException e) {
            logger.error("JsonProcessingException error");
            error = true;
        } finally {
            if (yield) {
                context.yield();
            } else if (!error) {
                session.getProvenanceReporter().send(flowFile, getURI(context));
                session.transfer(flowFile, REL_SUCCESS);
            } else {
                session.transfer(flowFile, REL_FAILURE);
            }
        }
    }

    @Override
    protected void doPostActionOnSchedule(final ProcessContext context) {
        // No-Op  as of now, since Put does not need to warmup connection for initial performance gain
    }


}