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
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
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
import java.util.LinkedList;
import java.util.List;
import java.util.Set;


@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@EventDriven
@SupportsBatching
@Tags({"elasticsearch", "elasticsearch 5","insert", "update", "write", "put"})
@CapabilityDescription("Writes the contents of a FlowFile to Elasticsearch, using the specified parameters such as "
        + "the index to insert into and the type of the document. If the cluster has been configured for authorization "
        + "and/or secure transport (SSL/TLS), and the X-Pack plugin is available, secure connections can be made. This processor "
        + "supports Elasticsearch 5.x clusters.")
@SeeAlso({FetchElasticsearch5.class,PutElasticsearch5.class})
public class PutElasticsearch5 extends AbstractElasticsearch5TransportClientProcessor {

    private static final Validator NON_EMPTY_EL_VALIDATOR = (subject, value, context) -> {
        if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(value)) {
            return new ValidationResult.Builder().subject(subject).input(value).explanation("Expression Language Present").valid(true).build();
        }
        return new ValidationResult.Builder().subject(subject).input(value).valid(value != null && !value.isEmpty()).explanation(subject + " cannot be empty").build();
    };

    static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("All FlowFiles that are written to Elasticsearch are routed to this relationship").build();

    static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("All FlowFiles that cannot be written to Elasticsearch are routed to this relationship").build();

    static final Relationship REL_RETRY = new Relationship.Builder().name("retry")
            .description("A FlowFile is routed to this relationship if the database cannot be updated but attempting the operation again may succeed")
            .build();

    public static final PropertyDescriptor ID_ATTRIBUTE = new PropertyDescriptor.Builder()
            .name("el5-put-id-attribute")
            .displayName("Identifier Attribute")
            .description("The name of the attribute containing the identifier for each FlowFile")
            .required(true)
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.ATTRIBUTE_KEY_VALIDATOR)
            .build();

    public static final PropertyDescriptor INDEX = new PropertyDescriptor.Builder()
            .name("el5-put-index")
            .displayName("Index")
            .description("The name of the index to insert into")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(NON_EMPTY_EL_VALIDATOR)
            .build();

    public static final PropertyDescriptor TYPE = new PropertyDescriptor.Builder()
            .name("el5-put-type")
            .displayName("Type")
            .description("The type of this document (used by Elasticsearch for indexing and searching)")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(NON_EMPTY_EL_VALIDATOR)
            .build();

    public static final PropertyDescriptor INDEX_OP = new PropertyDescriptor.Builder()
            .name("el5-put-index-op")
            .displayName("Index Operation")
            .description("The type of the operation used to index (index, update, upsert)")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(NON_EMPTY_EL_VALIDATOR)
            .defaultValue("index")
            .build();

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("el5-put-batch-size")
            .displayName("Batch Size")
            .description("The preferred number of FlowFiles to put to the database in a single transaction")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("100")
            .expressionLanguageSupported(true)
            .build();

    private static final Set<Relationship> relationships;
    private static final List<PropertyDescriptor> propertyDescriptors;

    static {
        final Set<Relationship> _rels = new HashSet<>();
        _rels.add(REL_SUCCESS);
        _rels.add(REL_FAILURE);
        _rels.add(REL_RETRY);
        relationships = Collections.unmodifiableSet(_rels);

        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(CLUSTER_NAME);
        descriptors.add(HOSTS);
        descriptors.add(PROP_SSL_CONTEXT_SERVICE);
        descriptors.add(PROP_XPACK_LOCATION);
        descriptors.add(USERNAME);
        descriptors.add(PASSWORD);
        descriptors.add(PING_TIMEOUT);
        descriptors.add(SAMPLER_INTERVAL);
        descriptors.add(ID_ATTRIBUTE);
        descriptors.add(INDEX);
        descriptors.add(TYPE);
        descriptors.add(CHARSET);
        descriptors.add(BATCH_SIZE);
        descriptors.add(INDEX_OP);

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

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        synchronized (esClient) {
            if(esClient.get() == null) {
                super.setup(context);
            }
        }

        final String id_attribute = context.getProperty(ID_ATTRIBUTE).getValue();
        final int batchSize = context.getProperty(BATCH_SIZE).evaluateAttributeExpressions().asInteger();

        final List<FlowFile> flowFiles = session.get(batchSize);
        if (flowFiles.isEmpty()) {
            return;
        }

        final ComponentLog logger = getLogger();
        // Keep track of the list of flow files that need to be transferred. As they are transferred, remove them from the list.
        List<FlowFile> flowFilesToTransfer = new LinkedList<>(flowFiles);
        try {
            final BulkRequestBuilder bulk = esClient.get().prepareBulk();

            for (FlowFile file : flowFiles) {
                final String index = context.getProperty(INDEX).evaluateAttributeExpressions(file).getValue();
                final String docType = context.getProperty(TYPE).evaluateAttributeExpressions(file).getValue();
                final String indexOp = context.getProperty(INDEX_OP).evaluateAttributeExpressions(file).getValue();
                final Charset charset = Charset.forName(context.getProperty(CHARSET).evaluateAttributeExpressions(file).getValue());


                final String id = file.getAttribute(id_attribute);
                if (id == null) {
                    logger.warn("No value in identifier attribute {} for {}, transferring to failure", new Object[]{id_attribute, file});
                    flowFilesToTransfer.remove(file);
                    session.transfer(file, REL_FAILURE);
                } else {
                    session.read(file, new InputStreamCallback() {
                        @Override
                        public void process(final InputStream in) throws IOException {
                            // For the bulk insert, each document has to be on its own line, so remove all CRLF
                            String json = IOUtils.toString(in, charset)
                                    .replace("\r\n", " ").replace('\n', ' ').replace('\r', ' ');

                            if (indexOp.equalsIgnoreCase("index")) {
                                bulk.add(esClient.get().prepareIndex(index, docType, id)
                                        .setSource(json.getBytes(charset)));
                            } else if (indexOp.equalsIgnoreCase("upsert")) {
                                bulk.add(esClient.get().prepareUpdate(index, docType, id)
                                        .setDoc(json.getBytes(charset))
                                        .setDocAsUpsert(true));
                            } else if (indexOp.equalsIgnoreCase("update")) {
                                bulk.add(esClient.get().prepareUpdate(index, docType, id)
                                        .setDoc(json.getBytes(charset)));
                            } else {
                                throw new IOException("Index operation: " + indexOp + " not supported.");
                            }
                        }
                    });
                }
            }

            if (bulk.numberOfActions() > 0) {
                final BulkResponse response = bulk.execute().actionGet();
                if (response.hasFailures()) {
                    // Responses are guaranteed to be in order, remove them in reverse order
                    BulkItemResponse[] responses = response.getItems();
                    if (responses != null && responses.length > 0) {
                        for (int i = responses.length - 1; i >= 0; i--) {
                            final BulkItemResponse item = responses[i];
                            final FlowFile flowFile = flowFilesToTransfer.get(item.getItemId());
                            if (item.isFailed()) {
                                logger.warn("Failed to insert {} into Elasticsearch due to {}, transferring to failure",
                                        new Object[]{flowFile, item.getFailure().getMessage()});
                                session.transfer(flowFile, REL_FAILURE);

                            } else {
                                session.getProvenanceReporter().send(flowFile, response.remoteAddress().getAddress());
                                session.transfer(flowFile, REL_SUCCESS);
                            }
                            flowFilesToTransfer.remove(flowFile);
                        }
                    }
                }

                // Transfer any remaining flowfiles to success
                for (FlowFile ff : flowFilesToTransfer) {
                    session.getProvenanceReporter().send(ff, response.remoteAddress().getAddress());
                    session.transfer(ff, REL_SUCCESS);
                }
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
            session.transfer(flowFilesToTransfer, REL_RETRY);
            context.yield();

        } catch (Exception exceptionToFail) {
            logger.error("Failed to insert into Elasticsearch due to {}, transferring to failure",
                    new Object[]{exceptionToFail.getLocalizedMessage()}, exceptionToFail);

            session.transfer(flowFilesToTransfer, REL_FAILURE);
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
