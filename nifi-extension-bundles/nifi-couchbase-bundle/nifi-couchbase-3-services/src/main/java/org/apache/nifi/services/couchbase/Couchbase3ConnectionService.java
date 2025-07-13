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
package org.apache.nifi.services.couchbase;

import com.couchbase.client.core.error.AuthenticationFailureException;
import com.couchbase.client.core.error.BucketNotFoundException;
import com.couchbase.client.core.error.CasMismatchException;
import com.couchbase.client.core.error.CollectionNotFoundException;
import com.couchbase.client.core.error.ConfigException;
import com.couchbase.client.core.error.DatasetNotFoundException;
import com.couchbase.client.core.error.DecodingFailureException;
import com.couchbase.client.core.error.DocumentExistsException;
import com.couchbase.client.core.error.DocumentLockedException;
import com.couchbase.client.core.error.DocumentMutationLostException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.error.DocumentNotLockedException;
import com.couchbase.client.core.error.DocumentUnretrievableException;
import com.couchbase.client.core.error.DurableWriteInProgressException;
import com.couchbase.client.core.error.DurableWriteReCommitInProgressException;
import com.couchbase.client.core.error.FeatureNotAvailableException;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.error.RequestCanceledException;
import com.couchbase.client.core.error.ScopeNotFoundException;
import com.couchbase.client.core.error.ServerOutOfMemoryException;
import com.couchbase.client.core.error.ServiceNotAvailableException;
import com.couchbase.client.core.error.TemporaryFailureException;
import com.couchbase.client.core.error.ValueTooLargeException;
import com.couchbase.client.core.error.subdoc.PathExistsException;
import com.couchbase.client.core.error.subdoc.PathNotFoundException;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.kv.GetOptions;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.ReplicateTo;
import com.couchbase.client.java.kv.UpsertOptions;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.services.couchbase.exception.CouchbaseErrorHandler;
import org.apache.nifi.services.couchbase.exception.CouchbaseException;
import org.apache.nifi.services.couchbase.utils.CouchbaseGetResult;
import org.apache.nifi.services.couchbase.utils.CouchbasePutResult;
import org.apache.nifi.services.couchbase.utils.DocumentType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Map.entry;
import static org.apache.nifi.services.couchbase.exception.CouchbaseErrorHandler.ErrorHandlingStrategy.FAILURE;
import static org.apache.nifi.services.couchbase.exception.CouchbaseErrorHandler.ErrorHandlingStrategy.RETRY;
import static org.apache.nifi.services.couchbase.exception.CouchbaseErrorHandler.ErrorHandlingStrategy.ROLLBACK;
import static org.apache.nifi.services.couchbase.utils.CouchbaseAttributes.BUCKET_ATTRIBUTE;
import static org.apache.nifi.services.couchbase.utils.CouchbaseAttributes.CAS_ATTRIBUTE;
import static org.apache.nifi.services.couchbase.utils.CouchbaseAttributes.COLLECTION_ATTRIBUTE;
import static org.apache.nifi.services.couchbase.utils.CouchbaseAttributes.DOCUMENT_ID_ATTRIBUTE;
import static org.apache.nifi.services.couchbase.utils.CouchbaseAttributes.EXPIRY_ATTRIBUTE;
import static org.apache.nifi.services.couchbase.utils.CouchbaseAttributes.SCOPE_ATTRIBUTE;

@CapabilityDescription("Provides a Couchbase SDK 3 based implementation.")
@Tags({"nosql", "couchbase", "database", "connection"})
public class Couchbase3ConnectionService extends AbstractControllerService implements CouchbaseConnectionService {

    public static final PropertyDescriptor COUCHBASE_CLUSTER_SERVICE = new PropertyDescriptor.Builder()
            .name("Couchbase Cluster Service")
            .description("A Couchbase Cluster Service which manages connections to a Couchbase cluster.")
            .required(true)
            .identifiesControllerService(CouchbaseClusterService.class)
            .build();

    public static final PropertyDescriptor BUCKET_NAME = new PropertyDescriptor.Builder()
            .name("Bucket Name")
            .description("The name of bucket to access.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .defaultValue("default")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor SCOPE_NAME = new PropertyDescriptor.Builder()
            .name("Scope Name")
            .description("The name of scope.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .defaultValue(CollectionIdentifier.DEFAULT_SCOPE)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor COLLECTION_NAME = new PropertyDescriptor.Builder()
            .name("Collection Name")
            .description("The name of collection.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .defaultValue(CollectionIdentifier.DEFAULT_COLLECTION)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor PERSIST_TO = new PropertyDescriptor.Builder()
            .name("Persist To")
            .description("Durability constraint about disk persistence.")
            .required(true)
            .allowableValues(PersistTo.values())
            .defaultValue(PersistTo.NONE.toString())
            .build();

    public static final PropertyDescriptor REPLICATE_TO = new PropertyDescriptor.Builder()
            .name("Replicate To")
            .description("Durability constraint about replication.")
            .required(true)
            .allowableValues(ReplicateTo.values())
            .defaultValue(ReplicateTo.NONE.toString())
            .build();

    public static final PropertyDescriptor DOCUMENT_TYPE = new PropertyDescriptor.Builder()
            .name("Document Type")
            .description("The type of contents.")
            .required(true)
            .allowableValues(DocumentType.values())
            .defaultValue(DocumentType.Json.toString())
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = List.of(
            COUCHBASE_CLUSTER_SERVICE, BUCKET_NAME, SCOPE_NAME, COLLECTION_NAME, PERSIST_TO, REPLICATE_TO, DOCUMENT_TYPE);

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    private Collection collection;
    private PersistTo persistTo;
    private ReplicateTo replicateTo;
    private DocumentType documentType;

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        final CouchbaseClusterService<Cluster> clusterService = context.getProperty(COUCHBASE_CLUSTER_SERVICE).asControllerService(CouchbaseClusterService.class);
        final Cluster cluster = clusterService.getCluster();

        final String bucketName = context.getProperty(BUCKET_NAME).evaluateAttributeExpressions().getValue();
        final String scopeName = context.getProperty(SCOPE_NAME).evaluateAttributeExpressions().getValue();
        final String collectionName = context.getProperty(COLLECTION_NAME).evaluateAttributeExpressions().getValue();

        documentType = DocumentType.valueOf(context.getProperty(DOCUMENT_TYPE).getValue());
        persistTo = PersistTo.valueOf(context.getProperty(PERSIST_TO).getValue());
        replicateTo = ReplicateTo.valueOf(context.getProperty(REPLICATE_TO).getValue());
        collection = cluster.bucket(bucketName).scope(scopeName).collection(collectionName);
    }

    @Override
    public CouchbaseGetResult getDocument(String documentId) throws CouchbaseException {
        try {
            final GetResult result = collection.get(documentId, GetOptions.getOptions()
                    .transcoder(documentType.getTranscoder()));

            final Map<String, String> attributes = Map.ofEntries(
                    entry(BUCKET_ATTRIBUTE, collection.bucketName()),
                    entry(SCOPE_ATTRIBUTE, collection.scopeName()),
                    entry(COLLECTION_ATTRIBUTE, collection.name()),
                    entry(DOCUMENT_ID_ATTRIBUTE, documentId),
                    entry(CAS_ATTRIBUTE, String.valueOf(result.cas())),
                    entry(EXPIRY_ATTRIBUTE, String.valueOf(result.expiryTime().orElse(null))),
                    entry(CoreAttributes.MIME_TYPE.key(), documentType.getMimeType())
            );

            return new CouchbaseGetResult(result.contentAsBytes(), attributes, createTransitUrl(documentId));
        } catch (Exception e) {
            throw new CouchbaseException(e);
        }
    }

    @Override
    public CouchbasePutResult putDocument(String documentId, byte[] content) throws CouchbaseException {
        try {
            if (!documentType.getValidator().test(content)) {
                throw new CouchbaseException("The provided input is invalid.");
            }

            final MutationResult result = collection.upsert(documentId, content,
                    UpsertOptions.upsertOptions()
                            .durability(persistTo, replicateTo)
                            .transcoder(documentType.getTranscoder())
                            .clientContext(new HashMap<>()));

            final Map<String, String> attributes = Map.ofEntries(
                    entry(BUCKET_ATTRIBUTE, collection.bucketName()),
                    entry(SCOPE_ATTRIBUTE, collection.scopeName()),
                    entry(COLLECTION_ATTRIBUTE, collection.name()),
                    entry(DOCUMENT_ID_ATTRIBUTE, documentId),
                    entry(CAS_ATTRIBUTE, String.valueOf(result.cas()))
            );

            return new CouchbasePutResult(attributes, createTransitUrl(documentId));
        } catch (Exception e) {
            throw new CouchbaseException(e);
        }
    }

    @Override
    public CouchbaseErrorHandler getErrorHandler() {
        final Map<Class<? extends Exception>, CouchbaseErrorHandler.ErrorHandlingStrategy> exceptionMapping = Map.ofEntries(
                entry(AuthenticationFailureException.class, ROLLBACK),
                entry(BucketNotFoundException.class, ROLLBACK),
                entry(ScopeNotFoundException.class, ROLLBACK),
                entry(CollectionNotFoundException.class, ROLLBACK),
                entry(DatasetNotFoundException.class, ROLLBACK),
                entry(ServiceNotAvailableException.class, ROLLBACK),
                entry(FeatureNotAvailableException.class, ROLLBACK),
                entry(InvalidArgumentException.class, ROLLBACK),
                entry(ConfigException.class, ROLLBACK),

                entry(RequestCanceledException.class, RETRY),
                entry(TemporaryFailureException.class, RETRY),
                entry(DurableWriteInProgressException.class, RETRY),
                entry(DurableWriteReCommitInProgressException.class, RETRY),
                entry(ServerOutOfMemoryException.class, RETRY),
                entry(DocumentLockedException.class, RETRY),
                entry(DocumentMutationLostException.class, RETRY),
                entry(DocumentUnretrievableException.class, RETRY),
                entry(DocumentNotLockedException.class, RETRY),

                entry(DocumentNotFoundException.class, FAILURE),
                entry(DocumentExistsException.class, FAILURE),
                entry(CasMismatchException.class, FAILURE),
                entry(DecodingFailureException.class, FAILURE),
                entry(PathNotFoundException.class, FAILURE),
                entry(PathExistsException.class, FAILURE),
                entry(ValueTooLargeException.class, FAILURE)
        );

        return new CouchbaseErrorHandler(exceptionMapping);
    }

    @Override
    public String createTransitUrl(String documentId) {
        return String.join(".", collection.bucketName(), collection.scopeName(), collection.name(), documentId);
    }
}
