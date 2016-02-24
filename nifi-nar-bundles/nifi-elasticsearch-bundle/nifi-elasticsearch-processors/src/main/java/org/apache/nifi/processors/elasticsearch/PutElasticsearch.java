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

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
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
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;

import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.transport.ReceiveTimeoutTransportException;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@EventDriven
@SupportsBatching
@Tags({"elasticsearch", "insert", "update", "write", "put"})
@CapabilityDescription("Writes the contents of a FlowFile to Elasticsearch, using the specified parameters such as "
        + "the index to insert into and the type of the document. If the cluster has been configured for authorization "
        + "and/or secure transport (SSL/TLS) and the Shield plugin is available, secure connections can be made. This processor "
        + "supports Elasticsearch 2.x clusters.")
public class PutElasticsearch extends AbstractElasticsearchProcessor {

    static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("All FlowFiles that are written to Elasticsearch are routed to this relationship").build();

    static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("All FlowFiles that cannot be written to Elasticsearch are routed to this relationship").build();

    static final Relationship REL_RETRY = new Relationship.Builder().name("retry")
            .description("A FlowFile is routed to this relationship if the database cannot be updated but attempting the operation again may succeed")
            .build();

    public static final PropertyDescriptor ID_ATTRIBUTE = new PropertyDescriptor.Builder()
            .name("Identifier Attribute")
            .description("The name of the attribute containing the identifier for each FlowFile")
            .required(true)
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.ATTRIBUTE_KEY_VALIDATOR)
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
        descriptors.add(CLUSTER_NAME);
        descriptors.add(HOSTS);
        descriptors.add(PROP_SSL_CONTEXT_SERVICE);
        descriptors.add(PROP_SHIELD_LOCATION);
        descriptors.add(USERNAME);
        descriptors.add(PASSWORD);
        descriptors.add(PING_TIMEOUT);
        descriptors.add(SAMPLER_INTERVAL);
        descriptors.add(ID_ATTRIBUTE);
        descriptors.add(INDEX);
        descriptors.add(TYPE);
        descriptors.add(CHARSET);
        descriptors.add(BATCH_SIZE);

        return Collections.unmodifiableList(descriptors);
    }

    @OnScheduled
    public void setup(ProcessContext context) {
        super.setup(context);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final int batchSize = context.getProperty(BATCH_SIZE).asInteger();
        final String index = context.getProperty(INDEX).evaluateAttributeExpressions().getValue();
        final String id_attribute = context.getProperty(ID_ATTRIBUTE).getValue();
        final String docType = context.getProperty(TYPE).evaluateAttributeExpressions().getValue();
        final Charset charset = Charset.forName(context.getProperty(CHARSET).getValue());

        final List<FlowFile> flowFiles = session.get(batchSize);
        if (flowFiles.isEmpty()) {
            return;
        }

        final ProcessorLog logger = getLogger();

        try {
            final BulkRequestBuilder bulk = esClient.get().prepareBulk();
            if (authToken != null) {
                bulk.putHeader("Authorization", authToken);
            }
            for (FlowFile file : flowFiles) {
                final String id = file.getAttribute(id_attribute);
                if (id == null) {
                    logger.error("No value in identifier attribute {} for {}", new Object[]{id_attribute, file});
                    session.transfer(file, REL_FAILURE);
                }
                session.read(file, new InputStreamCallback() {
                    @Override
                    public void process(final InputStream in) throws IOException {
                        String json = IOUtils.toString(in, charset)
                                .replace("\r\n", " ").replace('\n', ' ').replace('\r', ' ');
                        bulk.add(esClient.get().prepareIndex(index, docType, id)
                                .setSource(json.getBytes(charset)));
                    }
                });
            }

            final BulkResponse response = bulk.execute().actionGet();
            if (response.hasFailures()) {
                for (final BulkItemResponse item : response.getItems()) {
                    final FlowFile flowFile = flowFiles.get(item.getItemId());
                    if (item.isFailed()) {
                        logger.error("Failed to insert {} into Elasticsearch due to {}",
                                new Object[]{flowFile, item.getFailure().getMessage()});
                        session.transfer(flowFile, REL_FAILURE);

                    } else {
                        session.transfer(flowFile, REL_SUCCESS);
                    }
                }
            } else {
                session.transfer(flowFiles, REL_SUCCESS);
            }


        } catch (NoNodeAvailableException
                | ElasticsearchTimeoutException
                | ReceiveTimeoutTransportException
                | NodeClosedException exceptionToRetry) {

            // Authorization errors and other problems are often returned as NoNodeAvailableExceptions without a
            // traceable cause. However the cause seems to be logged, just not available to this caught exception.
            // Since the error message will show up as a bulletin, we make specific mention to check the logs for
            // more details.
            logger.error("Failed to insert into Elasticsearch due to {}. More detailed information may be available in " +
                            "the NiFi logs.",
                    new Object[]{exceptionToRetry.getLocalizedMessage()}, exceptionToRetry);
            session.transfer(flowFiles, REL_RETRY);
            context.yield();

        } catch (Exception exceptionToFail) {
            logger.error("Failed to insert into Elasticsearch due to {}",
                    new Object[]{exceptionToFail.getLocalizedMessage()}, exceptionToFail);

            session.transfer(flowFiles, REL_FAILURE);
            context.yield();
        }
    }

    /**
     * Dispose of ElasticSearch client
     */
    @OnStopped
    public void closeClient() {
        super.closeClient();
    }
}
