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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.ReceiveTimeoutTransportException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@EventDriven
@Tags({"elasticsearch", "elasticsearch 5", "delete", "remove"})
@CapabilityDescription("Delete a document from Elasticsearch 5.0 by document id. If the cluster has been configured for authorization and/or secure "
        + "transport (SSL/TLS), and the X-Pack plugin is available, secure connections can be made.")
@WritesAttributes({
        @WritesAttribute(attribute = DeleteElasticsearch5.ES_ERROR_MESSAGE, description = "The message attribute in case of error"),
        @WritesAttribute(attribute = DeleteElasticsearch5.ES_FILENAME, description = "The filename attribute which is set to the document identifier"),
        @WritesAttribute(attribute = DeleteElasticsearch5.ES_INDEX, description = "The Elasticsearch index containing the document"),
        @WritesAttribute(attribute = DeleteElasticsearch5.ES_TYPE, description = "The Elasticsearch document type"),
        @WritesAttribute(attribute = DeleteElasticsearch5.ES_REST_STATUS, description = "The filename attribute with rest status")
})
@SeeAlso({FetchElasticsearch5.class,PutElasticsearch5.class})
public class DeleteElasticsearch5 extends AbstractElasticsearch5TransportClientProcessor {

    public static final String UNABLE_TO_DELETE_DOCUMENT_MESSAGE = "Unable to delete document";

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("All FlowFile corresponding to the deleted document from Elasticsearch are routed to this relationship").build();

    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("All FlowFile corresponding to delete document that failed from Elasticsearch are routed to this relationship").build();

    public static final Relationship REL_RETRY = new Relationship.Builder().name("retry")
            .description("A FlowFile is routed to this relationship if the document cannot be deleted because or retryable exception like timeout or node not available")
            .build();

    public static final Relationship REL_NOT_FOUND = new Relationship.Builder().name("not found")
            .description("A FlowFile is routed to this relationship if the specified document was not found in elasticsearch")
            .build();

    public static final PropertyDescriptor DOCUMENT_ID = new PropertyDescriptor.Builder()
            .name("el5-delete-document-id")
            .displayName("Document Identifier")
            .description("The identifier for the document to be deleted")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor INDEX = new PropertyDescriptor.Builder()
            .name("el5-delete-index")
            .displayName("Index")
            .description("The name of the index to delete the document from")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor TYPE = new PropertyDescriptor.Builder()
            .name("el5-delete-type")
            .displayName("Type")
            .description("The type of this document to be deleted")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final Set<Relationship> relationships;
    private static final List<PropertyDescriptor> propertyDescriptors;

    public static final String ES_ERROR_MESSAGE = "es.error.message";
    public static final String ES_FILENAME = "filename";
    public static final String ES_INDEX = "es.index";
    public static final String ES_TYPE = "es.type";
    public static final String ES_REST_STATUS = "es.rest.status";

    static {
        final Set<Relationship> relations = new HashSet<>();
        relations.add(REL_SUCCESS);
        relations.add(REL_FAILURE);
        relations.add(REL_RETRY);
        relations.add(REL_NOT_FOUND);
        relationships = Collections.unmodifiableSet(relations);

        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(CLUSTER_NAME);
        descriptors.add(HOSTS);
        descriptors.add(PROP_SSL_CONTEXT_SERVICE);
        descriptors.add(PROP_XPACK_LOCATION);
        descriptors.add(USERNAME);
        descriptors.add(PASSWORD);
        descriptors.add(PING_TIMEOUT);
        descriptors.add(SAMPLER_INTERVAL);
        descriptors.add(DOCUMENT_ID);
        descriptors.add(INDEX);
        descriptors.add(TYPE);

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
                setup(context);
            }
        }

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String index = context.getProperty(INDEX).evaluateAttributeExpressions(flowFile).getValue();
        final String documentId = context.getProperty(DOCUMENT_ID).evaluateAttributeExpressions(flowFile).getValue();
        final String documentType = context.getProperty(TYPE).evaluateAttributeExpressions(flowFile).getValue();

        final ComponentLog logger = getLogger();

        if ( StringUtils.isBlank(index) ) {
            logger.debug("Index is required but was empty {}", new Object [] { index });
            flowFile = session.putAttribute(flowFile, ES_ERROR_MESSAGE, "Index is required but was empty");
            session.transfer(flowFile,REL_FAILURE);
            return;
        }
        if ( StringUtils.isBlank(documentType) ) {
            logger.debug("Document type is required but was empty {}", new Object [] { documentType });
            flowFile = session.putAttribute(flowFile, ES_ERROR_MESSAGE, "Document type is required but was empty");
            session.transfer(flowFile,REL_FAILURE);
            return;
        }
        if ( StringUtils.isBlank(documentId) ) {
            logger.debug("Document id is required but was empty {}", new Object [] { documentId });
            flowFile = session.putAttribute(flowFile, ES_ERROR_MESSAGE, "Document id is required but was empty");
            session.transfer(flowFile,REL_FAILURE);
            return;
        }

        flowFile = session.putAllAttributes(flowFile, new HashMap<String, String>() {{
            put(ES_FILENAME, documentId);
            put(ES_INDEX, index);
            put(ES_TYPE, documentType);
        }});

        try {

            logger.debug("Deleting document {}/{}/{} from Elasticsearch", new Object[]{index, documentType, documentId});
            DeleteRequestBuilder requestBuilder = prepareDeleteRequest(index, documentId, documentType);
            final DeleteResponse response = doDelete(requestBuilder);

            if (response.status() != RestStatus.OK)  {
                logger.warn("Failed to delete document {}/{}/{} from Elasticsearch: Status {}",
                        new Object[]{index, documentType, documentId, response.status()});
                flowFile = session.putAttribute(flowFile, ES_ERROR_MESSAGE, UNABLE_TO_DELETE_DOCUMENT_MESSAGE);
                flowFile = session.putAttribute(flowFile, ES_REST_STATUS, response.status().toString());
                context.yield();
                if ( response.status() ==  RestStatus.NOT_FOUND ) {
                       session.transfer(flowFile, REL_NOT_FOUND);
                } else {
                    session.transfer(flowFile, REL_FAILURE);
                }
            } else {
                logger.debug("Elasticsearch document " + documentId + " deleted");
                session.transfer(flowFile, REL_SUCCESS);
            }
        } catch ( ElasticsearchTimeoutException
                | ReceiveTimeoutTransportException exception) {
            logger.error("Failed to delete document {} from Elasticsearch due to {}",
                    new Object[]{documentId, exception.getLocalizedMessage()}, exception);
            flowFile = session.putAttribute(flowFile, ES_ERROR_MESSAGE, exception.getLocalizedMessage());
            session.transfer(flowFile, REL_RETRY);
            context.yield();

        } catch (Exception e) {
            logger.error("Failed to delete document {} from Elasticsearch due to {}", new Object[]{documentId, e.getLocalizedMessage()}, e);
            flowFile = session.putAttribute(flowFile, ES_ERROR_MESSAGE, e.getLocalizedMessage());
            session.transfer(flowFile, REL_FAILURE);
            context.yield();
        }
    }

    protected DeleteRequestBuilder prepareDeleteRequest(final String index, final String documentId, final String documentType) {
        return esClient.get().prepareDelete(index, documentType, documentId);
    }

    protected DeleteResponse doDelete(DeleteRequestBuilder requestBuilder)
            throws InterruptedException, ExecutionException {
        return requestBuilder.execute().get();
    }

    @OnStopped
    public void closeClient() {
        super.closeClient();
    }
}
