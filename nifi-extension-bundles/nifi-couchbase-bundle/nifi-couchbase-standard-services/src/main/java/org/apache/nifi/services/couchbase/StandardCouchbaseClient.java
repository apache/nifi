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
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.codec.RawBinaryTranscoder;
import com.couchbase.client.java.codec.RawJsonTranscoder;
import com.couchbase.client.java.codec.Transcoder;
import com.couchbase.client.java.kv.GetOptions;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.ReplicateTo;
import com.couchbase.client.java.kv.UpsertOptions;
import org.apache.nifi.services.couchbase.exception.CouchbaseException;
import org.apache.nifi.services.couchbase.exception.ExceptionCategory;
import org.apache.nifi.services.couchbase.utils.CouchbaseGetResult;
import org.apache.nifi.services.couchbase.utils.CouchbaseUpsertResult;
import org.apache.nifi.services.couchbase.utils.DocumentType;
import org.apache.nifi.services.couchbase.utils.JsonValidator;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

import static java.util.Map.entry;
import static org.apache.nifi.services.couchbase.exception.ExceptionCategory.FAILURE;
import static org.apache.nifi.services.couchbase.exception.ExceptionCategory.RETRY;
import static org.apache.nifi.services.couchbase.exception.ExceptionCategory.ROLLBACK;

class StandardCouchbaseClient implements CouchbaseClient {

    private final Collection collection;
    private final DocumentType documentType;
    private final PersistTo persistTo;
    private final ReplicateTo replicateTo;

    private final Map<Class<? extends Exception>, ExceptionCategory> exceptionMapping = Map.ofEntries(
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

    StandardCouchbaseClient(Collection collection, DocumentType documentType, PersistTo persistTo, ReplicateTo replicateTo) {
        this.collection = collection;
        this.documentType = documentType;
        this.persistTo = persistTo;
        this.replicateTo = replicateTo;
    }

    @Override
    public CouchbaseGetResult getDocument(String documentId) throws CouchbaseException {
        try {
            final GetResult result = collection.get(documentId, GetOptions.getOptions().transcoder(getTranscoder(documentType)));

            return new CouchbaseGetResult(result.contentAsBytes(), result.cas());
        } catch (Exception e) {
            throw new CouchbaseException("Failed to get document [%s] from Couchbase".formatted(documentId), e);
        }
    }

    @Override
    public CouchbaseUpsertResult upsertDocument(String documentId, byte[] content) throws CouchbaseException {
        try {
            if (!getInputValidator(documentType).test(content)) {
                throw new CouchbaseException("The provided input is invalid");
            }

            final MutationResult result = collection.upsert(documentId, content,
                    UpsertOptions.upsertOptions()
                            .durability(persistTo, replicateTo)
                            .transcoder(getTranscoder(documentType))
                            .clientContext(new HashMap<>()));

            return new CouchbaseUpsertResult(result.cas());
        } catch (Exception e) {
            throw new CouchbaseException("Failed to upsert document [%s] in Couchbase".formatted(documentId), e);
        }
    }

    @Override
    public ExceptionCategory getExceptionCategory(Throwable throwable) {
        return exceptionMapping.getOrDefault(throwable.getClass(), FAILURE);
    }

    private Transcoder getTranscoder(DocumentType documentType) {
        return switch (documentType) {
            case JSON -> RawJsonTranscoder.INSTANCE;
            case BINARY -> RawBinaryTranscoder.INSTANCE;
        };
    }

    private Predicate<byte[]> getInputValidator(DocumentType documentType) {
        return switch (documentType) {
            case JSON -> new JsonValidator();
            case BINARY -> v -> true;
        };
    }
}
