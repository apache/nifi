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

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

@EventDriven
@Tags({ "azure", "cosmos", "document", "insert", "update", "write", "put" })
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Writes the contents of a FlowFile to Azure Cosmos DB with Core SQL API. For CosmosDB with Mongo API, use PutMongo instead.")
@SystemResourceConsideration(resource = SystemResource.MEMORY)
public class PutAzureCosmosDBDocument extends AbstractAzureCosmosDBProcessor {

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
                    logger.debug("coverting number id into string...");
                    ((ObjectNode) doc).put("id", doc.get("id").asText());
                }
            } else {
                ((ObjectNode) doc).put("id",flowFile.getAttribute("uuid"));
            }
            logger.debug("after document ID verification");
            if(!doc.has(partitionKeyField)){
                logger.error("Required parition key field value not found from input flow file content with doc.id=" + doc.get("id"));
                session.transfer(flowFile, REL_FAILURE);
                context.yield();
            } else {
                // insert doc if this is a new, otherwise update the existing document with upsertDocument API
                if (logger.isErrorEnabled()) {
                    logger.debug("Inserting/Updating document with upsertItem");
                }
                this.container.upsertItem(doc);

                session.getProvenanceReporter().send(flowFile, getURI(context));
                session.transfer(flowFile, REL_SUCCESS);
                if (logger.isErrorEnabled()) {
                    logger.debug("Inserting/Updating document completed...");
                }
            }
        } catch (Exception e) {
            logger.error("Failed to upsertDocument {} into Azure CosmosDB due to {}", new Object[] {flowFile, e}, e);
            // send out CosmosDB.Document to output for debugging
            session.transfer(flowFile, REL_FAILURE);
            context.yield();
        }
    }

    @Override
    protected void doPostActionOnSchedule(final ProcessContext context) {
        // No-Op  as of now, since Put does not need to warmup connection for initial performance gain
    }


}
