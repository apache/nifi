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

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;

import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.transport.ReceiveTimeoutTransportException;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


@EventDriven
@Tags({"elasticsearch", "insert", "update", "write", "put"})
@CapabilityDescription("Writes the contents of a FlowFile to Elasticsearch")
public class PutElasticsearch extends AbstractElasticsearchProcessor {

    static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("All FlowFiles that are written to Elasticsearch are routed to this relationship").build();

    static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("All FlowFiles that cannot be written to Elasticsearch are routed to this relationship").build();

    static final Relationship REL_RETRY = new Relationship.Builder()
            .name("retry")
            .description("A FlowFile is routed to this relationship if the database cannot be updated but attempting the operation again may succeed")
            .build();

    public static final PropertyDescriptor ID_ATTRIBUTE = new PropertyDescriptor.Builder()
            .name("Identifier attribute")
            .description("The name of the attribute containing the identifier for each FlowFile")
            .required(true)
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor INDEX = new PropertyDescriptor.Builder()
            .name("Index")
            .description("The name of the index to insert into")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(
                    AttributeExpression.ResultType.STRING, true))
            .build();

    public static final PropertyDescriptor TYPE = new PropertyDescriptor.Builder()
            .name("Type")
            .description("The type of this document (used by Elasticsearch for indexing and searching)")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(
                    AttributeExpression.ResultType.STRING, true))
            .build();

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
            .description("The preferred number of FlowFiles to put to the database in a single transaction")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("100")
            .build();


    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        relationships.add(REL_RETRY);
        return Collections.unmodifiableSet(relationships);
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(CLIENT_TYPE);
        descriptors.add(CLUSTER_NAME);
        descriptors.add(HOSTS);
        descriptors.add(PING_TIMEOUT);
        descriptors.add(SAMPLER_INTERVAL);
        descriptors.add(ID_ATTRIBUTE);
        descriptors.add(INDEX);
        descriptors.add(TYPE);
        descriptors.add(BATCH_SIZE);

        return Collections.unmodifiableList(descriptors);
    }


    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final int batchSize = context.getProperty(BATCH_SIZE).asInteger();
        final String index = context.getProperty(INDEX).evaluateAttributeExpressions().getValue();
        final String id_attribute = context.getProperty(ID_ATTRIBUTE).getValue();
        final String docType = context.getProperty(TYPE).getValue();

        final List<FlowFile> flowFiles = session.get(batchSize);
        if (flowFiles.isEmpty()) {
            return;
        }

        final ProcessorLog logger = getLogger();

        try {
            final BulkRequestBuilder bulk = esClient.prepareBulk();
            for (FlowFile file : flowFiles) {
                final String id = file.getAttribute(id_attribute);
                if (id == null) {
                    getLogger().error("no value in identifier attribute {}", new Object[]{id_attribute});
                    throw new ProcessException("No value in identifier attribute " + id_attribute);
                }
                session.read(file, new InputStreamCallback() {
                    @Override
                    public void process(final InputStream in) throws IOException {

                        final InputStreamReader input = new InputStreamReader(in);
                        final JsonParser parser = new JsonParser();
                        final JsonObject json = parser.parse(input).getAsJsonObject();
                        bulk.add(esClient.prepareIndex(index, docType, id)
                                .setSource(getSource(json)));
                    }
                });
            }

            final BulkResponse response = bulk.execute().actionGet();
            if (response.hasFailures()) {
                for (final BulkItemResponse item : response.getItems()) {
                    final FlowFile flowFile = flowFiles.get(item.getItemId());
                    if (item.isFailed()) {
                        logger.error("Failed to insert {} into Elasticsearch due to {}",
                                new Object[]{flowFile, item.getFailure()}, new Exception());
                        session.transfer(flowFile, REL_FAILURE);

                    } else {
                        session.transfer(flowFile, REL_SUCCESS);

                    }
                }
            } else {
                for (final FlowFile flowFile : flowFiles) {
                    session.transfer(flowFile, REL_SUCCESS);
                }
            }


        } catch (NoNodeAvailableException nne) {
            logger.error("Failed to insert {} into Elasticsearch No Node Available {}", new Object[]{nne}, nne);
            for (final FlowFile flowFile : flowFiles) {
                session.transfer(flowFile, REL_RETRY);
            }
            context.yield();

        } catch (ElasticsearchTimeoutException ete) {
            logger.error("Failed to insert {} into Elasticsearch Timeout to {}", new Object[]{ete}, ete);
            for (final FlowFile flowFile : flowFiles) {
                session.transfer(flowFile, REL_RETRY);
            }
            context.yield();

        } catch (ReceiveTimeoutTransportException rtt) {
            logger.error("Failed to insert {} into Elasticsearch ReceiveTimeoutTransportException to {}", new Object[]{rtt}, rtt);
            for (final FlowFile flowFile : flowFiles) {
                session.transfer(flowFile, REL_RETRY);
            }
            context.yield();

        } catch (ElasticsearchParseException esp) {
            logger.error("Failed to insert {} into Elasticsearch Parse Exception {}", new Object[]{esp}, esp);

            for (final FlowFile flowFile : flowFiles) {
                session.transfer(flowFile, REL_FAILURE);
            }
            context.yield();

        } catch (Exception e) {
            logger.error("Failed to insert {} into Elasticsearch due to {}", new Object[]{e}, e);

            for (final FlowFile flowFile : flowFiles) {
                session.transfer(flowFile, REL_FAILURE);
            }
            context.yield();

        }
    }
}
