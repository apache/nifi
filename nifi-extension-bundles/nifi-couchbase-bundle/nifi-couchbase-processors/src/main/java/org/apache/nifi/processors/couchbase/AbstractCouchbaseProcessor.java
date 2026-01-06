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
package org.apache.nifi.processors.couchbase;

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.services.couchbase.CouchbaseClient;
import org.apache.nifi.services.couchbase.CouchbaseConnectionService;
import org.apache.nifi.services.couchbase.exception.CouchbaseException;
import org.apache.nifi.services.couchbase.exception.ExceptionCategory;
import org.apache.nifi.services.couchbase.utils.CouchbaseContext;
import org.apache.nifi.services.couchbase.utils.DocumentType;
import org.apache.nifi.stream.io.StreamUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.nifi.processors.couchbase.utils.CouchbaseAttributes.BUCKET_ATTRIBUTE;
import static org.apache.nifi.processors.couchbase.utils.CouchbaseAttributes.CAS_ATTRIBUTE;
import static org.apache.nifi.processors.couchbase.utils.CouchbaseAttributes.COLLECTION_ATTRIBUTE;
import static org.apache.nifi.processors.couchbase.utils.CouchbaseAttributes.DEFAULT_BUCKET;
import static org.apache.nifi.processors.couchbase.utils.CouchbaseAttributes.DEFAULT_COLLECTION;
import static org.apache.nifi.processors.couchbase.utils.CouchbaseAttributes.DEFAULT_SCOPE;
import static org.apache.nifi.processors.couchbase.utils.CouchbaseAttributes.DOCUMENT_ID_ATTRIBUTE;
import static org.apache.nifi.processors.couchbase.utils.CouchbaseAttributes.SCOPE_ATTRIBUTE;

public abstract class AbstractCouchbaseProcessor extends AbstractProcessor {

    protected CouchbaseConnectionService connectionService;

    public static final PropertyDescriptor DOCUMENT_ID = new PropertyDescriptor.Builder()
            .name("Document ID")
            .description("Couchbase document identifier, or an expression to construct the Couchbase document identifier.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor COUCHBASE_CONNECTION_SERVICE = new PropertyDescriptor.Builder()
            .name("Couchbase Connection Service")
            .description("A Couchbase Connection Service which manages connections to a Couchbase cluster.")
            .required(true)
            .identifiesControllerService(CouchbaseConnectionService.class)
            .build();

    public static final PropertyDescriptor BUCKET_NAME = new PropertyDescriptor.Builder()
            .name("Bucket Name")
            .description("The name of the bucket where documents will be stored. Each bucket contains a hierarchy of scopes and collections to group keys and values logically.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .defaultValue(DEFAULT_BUCKET)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor SCOPE_NAME = new PropertyDescriptor.Builder()
            .name("Scope Name")
            .description("The name of the scope  which is a logical namespace within a bucket, serving to categorize and organize related collections.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .defaultValue(DEFAULT_SCOPE)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor COLLECTION_NAME = new PropertyDescriptor.Builder()
            .name("Collection Name")
            .description("The name of collection which is a logical container within a scope, used to hold documents.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .defaultValue(DEFAULT_COLLECTION)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor DOCUMENT_TYPE = new PropertyDescriptor.Builder()
            .name("Document Type")
            .description("The content type for storing the document.")
            .required(true)
            .allowableValues(DocumentType.values())
            .defaultValue(DocumentType.JSON.toString())
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile is routed to this relationship after the data ingestion was successful.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is routed to this relationship if the operation failed and retrying the operation will also fail, such as an invalid data or schema.")
            .build();

    public static final Relationship REL_RETRY = new Relationship.Builder()
            .name("retry")
            .description("All FlowFile that fail due to server/cluster availability go to this relationship.")
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = List.of(
            COUCHBASE_CONNECTION_SERVICE,
            BUCKET_NAME,
            SCOPE_NAME,
            COLLECTION_NAME,
            DOCUMENT_TYPE,
            DOCUMENT_ID
    );

    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS, REL_FAILURE, REL_RETRY);

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        connectionService = context.getProperty(COUCHBASE_CONNECTION_SERVICE).asControllerService(CouchbaseConnectionService.class);
    }

    protected byte[] readFlowFileContent(ProcessSession session, FlowFile flowFile) {
        final byte[] content = new byte[(int) flowFile.getSize()];
        session.read(flowFile, in -> StreamUtils.fillBuffer(in, content, true));

        return content;
    }

    protected String createTransitUri(String serviceLocation, CouchbaseContext context, String documentId) {
        return String.join("/", serviceLocation, context.bucket(), context.scope(), context.collection(), documentId);
    }

    protected CouchbaseContext getCouchbaseContext(ProcessContext context, FlowFile flowFile) {
        final String bucketName = context.getProperty(BUCKET_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String scopeName = context.getProperty(SCOPE_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String collectionName = context.getProperty(COLLECTION_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final DocumentType documentType = DocumentType.valueOf(context.getProperty(DOCUMENT_TYPE).getValue());

        return new CouchbaseContext(bucketName, scopeName, collectionName, documentType);
    }

    protected Map<String, String> getFlowfileAttributes(CouchbaseContext context, String documentId, String cas) {
        return new HashMap<>(Map.of(
                BUCKET_ATTRIBUTE, context.bucket(),
                SCOPE_ATTRIBUTE, context.scope(),
                COLLECTION_ATTRIBUTE, context.collection(),
                DOCUMENT_ID_ATTRIBUTE, documentId,
                CAS_ATTRIBUTE, cas
        ));
    }

    protected void handleCouchbaseException(CouchbaseClient couchbaseClient, ProcessContext context, ProcessSession session,
                                            ComponentLog logger, FlowFile flowFile, CouchbaseException e, String errorMessage) {
        final Throwable throwable = (e.getCause() != null) ? e.getCause() : e;
        logger.error(errorMessage, throwable);
        final ExceptionCategory exceptionCategory = couchbaseClient.getExceptionCategory(throwable);
        switch (exceptionCategory) {
            case ROLLBACK -> session.rollback();
            case FAILURE -> {
                context.yield();
                session.transfer(flowFile, REL_FAILURE);
            }
            case RETRY -> session.transfer(flowFile, REL_RETRY);
        }
    }
}
