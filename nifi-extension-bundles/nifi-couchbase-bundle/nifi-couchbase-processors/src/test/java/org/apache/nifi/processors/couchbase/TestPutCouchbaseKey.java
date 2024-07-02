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

import com.couchbase.client.core.Core;
import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.cnc.tracing.NoopRequestSpan;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.PasswordAuthenticator;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.DurabilityImpossibleException;
import com.couchbase.client.core.error.ServiceNotAvailableException;
import com.couchbase.client.core.error.context.KeyValueErrorContext;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.kv.GetRequest;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.core.util.ConnectionString;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.ReplicateTo;
import com.couchbase.client.java.kv.UpsertOptions;
import org.apache.nifi.attribute.expression.language.exception.AttributeExpressionLanguageException;
import org.apache.nifi.couchbase.CouchbaseClusterControllerService;
import org.apache.nifi.couchbase.DocumentType;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.nifi.couchbase.CouchbaseConfigurationProperties.BUCKET_NAME;
import static org.apache.nifi.couchbase.CouchbaseConfigurationProperties.COUCHBASE_CLUSTER_SERVICE;
import static org.apache.nifi.couchbase.CouchbaseConfigurationProperties.DOCUMENT_TYPE;
import static org.apache.nifi.processors.couchbase.AbstractCouchbaseProcessor.DOC_ID;
import static org.apache.nifi.processors.couchbase.AbstractCouchbaseProcessor.REL_FAILURE;
import static org.apache.nifi.processors.couchbase.AbstractCouchbaseProcessor.REL_RETRY;
import static org.apache.nifi.processors.couchbase.AbstractCouchbaseProcessor.REL_SUCCESS;
import static org.apache.nifi.processors.couchbase.CouchbaseAttributes.Exception;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class TestPutCouchbaseKey {

    private static final String SERVICE_ID = "couchbaseClusterService";
    private TestRunner testRunner;

    private final CoreEnvironment coreEnvironment = CoreEnvironment.builder().build();
    private final PasswordAuthenticator passwordAuthenticator =
            new PasswordAuthenticator.Builder().username("couchbase").password("b1password").build();
    private final ConnectionString connectionString = ConnectionString.create("couchbase://192.168.99.100");
    private final Core core = Core.create(coreEnvironment, passwordAuthenticator, connectionString);
    private final CoreContext coreContext = new CoreContext(core, 1, coreEnvironment, passwordAuthenticator);

    @BeforeEach
    public void init() throws Exception {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi.processors.couchbase.PutCouchbaseKey", "debug");
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi.processors.couchbase.TestPutCouchbaseKey", "debug");

        testRunner = TestRunners.newTestRunner(PutCouchbaseKey.class);
    }

    private void setupMockBucket(Bucket bucket) throws InitializationException {
        CouchbaseClusterControllerService service = mock(CouchbaseClusterControllerService.class);
        when(service.getIdentifier()).thenReturn(SERVICE_ID);
        when(service.openBucket(anyString())).thenReturn(bucket);
        when(bucket.name()).thenReturn("bucket-1");
        testRunner.addControllerService(SERVICE_ID, service);
        testRunner.enableControllerService(service);
        testRunner.setProperty(COUCHBASE_CLUSTER_SERVICE, SERVICE_ID);
    }

    @Test
    public void testStaticDocId() throws Exception {
        String bucketName = "bucket-1";
        String docId = "doc-a";
        long cas = 200L;

        String inFileData = "{\"key\":\"value\"}";
        byte[] inFileDataBytes = inFileData.getBytes(StandardCharsets.UTF_8);

        Bucket bucket = mock(Bucket.class);
        Collection collection = mock(Collection.class);
        MutationResult result = mock(MutationResult.class);
        when(result.cas()).thenReturn(cas);
        when(bucket.collection(anyString())).thenReturn(collection);
        when(collection.upsert(anyString(), any(), any(UpsertOptions.class)))
                .thenReturn(result);
        setupMockBucket(bucket);

        testRunner.enqueue(inFileDataBytes);
        testRunner.setProperty(BUCKET_NAME, bucketName);
        testRunner.setProperty(DOC_ID, docId);
        testRunner.run();

        verify(collection, times(1))
                .upsert(anyString(), any(), any(UpsertOptions.class));

        testRunner.assertAllFlowFilesTransferred(REL_SUCCESS);
        testRunner.assertTransferCount(REL_SUCCESS, 1);
        testRunner.assertTransferCount(REL_RETRY, 0);
        testRunner.assertTransferCount(REL_FAILURE, 0);
        MockFlowFile outFile = testRunner.getFlowFilesForRelationship(REL_SUCCESS).get(0);
        outFile.assertContentEquals(inFileData);
        outFile.assertAttributeEquals(CouchbaseAttributes.Cluster.key(), SERVICE_ID);
        outFile.assertAttributeEquals(CouchbaseAttributes.Bucket.key(), bucketName);
        outFile.assertAttributeEquals(CouchbaseAttributes.DocId.key(), docId);
        outFile.assertAttributeEquals(CouchbaseAttributes.Cas.key(), String.valueOf(cas));
    }

    @Test
    public void testBinaryDoc() throws Exception {
        String bucketName = "bucket-1";
        String docId = "doc-a";
        long cas = 200L;

        String inFileData = "12345";
        byte[] inFileDataBytes = inFileData.getBytes(StandardCharsets.UTF_8);

        Bucket bucket = mock(Bucket.class);
        Collection collection = mock(Collection.class);
        MutationResult result = mock(MutationResult.class);
        when(result.cas()).thenReturn(cas);
        when(bucket.collection(anyString())).thenReturn(collection);
        when(collection.upsert(anyString(), any(), any(UpsertOptions.class)))
                .thenReturn(result);
        setupMockBucket(bucket);

        testRunner.enqueue(inFileDataBytes);
        testRunner.setProperty(BUCKET_NAME, bucketName);
        testRunner.setProperty(DOC_ID, docId);
        testRunner.setProperty(DOCUMENT_TYPE, DocumentType.Binary.name());
        testRunner.run();

        verify(collection, times(1))
                .upsert(anyString(), any(), any(UpsertOptions.class));

        testRunner.assertAllFlowFilesTransferred(REL_SUCCESS);
        testRunner.assertTransferCount(REL_SUCCESS, 1);
        testRunner.assertTransferCount(REL_RETRY, 0);
        testRunner.assertTransferCount(REL_FAILURE, 0);
        MockFlowFile outFile = testRunner.getFlowFilesForRelationship(REL_SUCCESS).get(0);
        outFile.assertContentEquals(inFileData);
        outFile.assertAttributeEquals(CouchbaseAttributes.Cluster.key(), SERVICE_ID);
        outFile.assertAttributeEquals(CouchbaseAttributes.Bucket.key(), bucketName);
        outFile.assertAttributeEquals(CouchbaseAttributes.DocId.key(), docId);
        outFile.assertAttributeEquals(CouchbaseAttributes.Cas.key(), String.valueOf(cas));
    }

    @Test
    public void testDurabilityConstraint() throws Exception {
        String docId = "doc-a";

        String inFileData = "{\"key\":\"value\"}";
        byte[] inFileDataBytes = inFileData.getBytes(StandardCharsets.UTF_8);

        Bucket bucket = mock(Bucket.class);
        Collection collection = mock(Collection.class);
        MutationResult result = mock(MutationResult.class);
        when(bucket.collection(anyString())).thenReturn(collection);
        when(collection.upsert(anyString(), any(), any(UpsertOptions.class)))
                .thenReturn(result);
        setupMockBucket(bucket);

        testRunner.enqueue(inFileDataBytes);
        testRunner.setProperty(DOC_ID, docId);
        testRunner.setProperty(PutCouchbaseKey.PERSIST_TO, PersistTo.ACTIVE.toString());
        testRunner.setProperty(PutCouchbaseKey.REPLICATE_TO, ReplicateTo.ONE.toString());
        testRunner.run();

        verify(collection, times(1))
                .upsert(anyString(), any(), any(UpsertOptions.class));

        testRunner.assertAllFlowFilesTransferred(REL_SUCCESS);
        testRunner.assertTransferCount(REL_SUCCESS, 1);
        testRunner.assertTransferCount(REL_RETRY, 0);
        testRunner.assertTransferCount(REL_FAILURE, 0);
        MockFlowFile outFile = testRunner.getFlowFilesForRelationship(REL_SUCCESS).get(0);
        outFile.assertContentEquals(inFileData);
    }

    @Test
    public void testDocIdExp() throws Exception {
        String docIdExp = "${'someProperty'}";
        String somePropertyValue = "doc-p";

        String inFileData = "{\"key\":\"value\"}";
        byte[] inFileDataBytes = inFileData.getBytes(StandardCharsets.UTF_8);
        long cas = 200L;

        Bucket bucket = mock(Bucket.class);
        Collection collection = mock(Collection.class);
        MutationResult result = mock(MutationResult.class);
        when(result.cas()).thenReturn(cas);
        when(bucket.collection(anyString())).thenReturn(collection);
        when(collection.upsert(anyString(), any(), any(UpsertOptions.class)))
                .thenReturn(result);
        setupMockBucket(bucket);

        testRunner.setProperty(DOC_ID, docIdExp);

        Map<String, String> properties = new HashMap<>();
        properties.put("someProperty", somePropertyValue);
        testRunner.enqueue(inFileDataBytes, properties);
        testRunner.run();

        ArgumentCaptor<String> capture = ArgumentCaptor.forClass(String.class);
        verify(collection, times(1))
                .upsert(capture.capture(), any(), any(UpsertOptions.class));
        assertEquals(somePropertyValue, capture.getValue());

        testRunner.assertTransferCount(REL_SUCCESS, 1);
        testRunner.assertTransferCount(REL_RETRY, 0);
        testRunner.assertTransferCount(REL_FAILURE, 0);
        MockFlowFile outFile = testRunner.getFlowFilesForRelationship(REL_SUCCESS).get(0);
        outFile.assertContentEquals(inFileData);
    }

    @Test
    public void testInvalidDocIdExp() throws Exception {
        String docIdExp = "${invalid_function(someProperty)}";
        String somePropertyValue = "doc-p";

        String inFileData = "{\"key\":\"value\"}";
        byte[] inFileDataBytes = inFileData.getBytes(StandardCharsets.UTF_8);

        Bucket bucket = mock(Bucket.class);
        Collection collection = mock(Collection.class);
        MutationResult result = mock(MutationResult.class);
        when(bucket.collection(anyString())).thenReturn(collection);
        when(collection.upsert(anyString(), any(UpsertOptions.class)))
                .thenReturn(result);
        setupMockBucket(bucket);

        testRunner.setProperty(DOC_ID, docIdExp);

        Map<String, String> properties = new HashMap<>();
        properties.put("someProperty", somePropertyValue);
        testRunner.enqueue(inFileDataBytes, properties);

        AssertionError e = assertThrows(AssertionError.class, () -> testRunner.run());
        assertEquals(e.getCause().getClass(), AttributeExpressionLanguageException.class);

        testRunner.assertTransferCount(REL_SUCCESS, 0);
        testRunner.assertTransferCount(REL_RETRY, 0);
        testRunner.assertTransferCount(REL_FAILURE, 0);
    }

    @Test
    public void testCouchbaseFailure() throws Exception {

        String docId = "doc-a";

        String inFileData = "{\"key\":\"value\"}";
        byte[] inFileDataBytes = inFileData.getBytes(StandardCharsets.UTF_8);

        Bucket bucket = mock(Bucket.class);
        Collection collection = mock(Collection.class);
        when(bucket.collection(anyString())).thenReturn(collection);
        when(collection.upsert(anyString(), any(), any(UpsertOptions.class)))
                .thenThrow(new ServiceNotAvailableException("test",
                        new KeyValueErrorContext(
                                createNewGetRequest("bucket-a", "collection-a"),
                                mock(ResponseStatus.class),
                                mock(MemcacheProtocol.FlexibleExtras.class))));
        setupMockBucket(bucket);

        testRunner.enqueue(inFileDataBytes);
        testRunner.setProperty(DOC_ID, docId);
        testRunner.setProperty(PutCouchbaseKey.REPLICATE_TO, ReplicateTo.ONE.toString());

        AssertionError e = assertThrows(AssertionError.class, () -> testRunner.run());
        assertEquals(e.getCause().getClass(), ProcessException.class);

        verify(collection, times(1))
                .upsert(anyString(), any(), any(UpsertOptions.class));

        testRunner.assertTransferCount(REL_SUCCESS, 0);
        testRunner.assertTransferCount(REL_RETRY, 0);
        testRunner.assertTransferCount(REL_FAILURE, 0);
    }

    @Test
    public void testCouchbaseTempFlowFileError() throws Exception {

        String docId = "doc-a";

        String inFileData = "{\"key\":\"value\"}";
        byte[] inFileDataBytes = inFileData.getBytes(StandardCharsets.UTF_8);

        CouchbaseException exception = new DurabilityImpossibleException(new KeyValueErrorContext(
                createNewGetRequest("bucket-a", "collection-a"),
                mock(ResponseStatus.class),
                mock(MemcacheProtocol.FlexibleExtras.class)));
        Bucket bucket = mock(Bucket.class);
        Collection collection = mock(Collection.class);
        when(bucket.collection(anyString())).thenReturn(collection);
        when(collection.upsert(anyString(), any(), any(UpsertOptions.class)))
                .thenThrow(exception);
        setupMockBucket(bucket);

        testRunner.enqueue(inFileDataBytes);
        testRunner.setProperty(DOC_ID, docId);
        testRunner.setProperty(PutCouchbaseKey.REPLICATE_TO, ReplicateTo.ONE.toString());
        testRunner.run();

        verify(collection, times(1))
                .upsert(anyString(), any(), any(UpsertOptions.class));

        testRunner.assertTransferCount(REL_SUCCESS, 0);
        testRunner.assertTransferCount(REL_RETRY, 1);
        testRunner.assertTransferCount(REL_FAILURE, 0);
        MockFlowFile orgFile = testRunner.getFlowFilesForRelationship(REL_RETRY).get(0);
        orgFile.assertContentEquals(inFileData);
        orgFile.assertAttributeEquals(Exception.key(), exception.getClass().getName());
    }

    private GetRequest createNewGetRequest(final String bucketName, final String collectionName) {
        return new GetRequest(
                collectionName,
                Duration.ofSeconds(3),
                coreContext,
                new CollectionIdentifier(
                        bucketName,
                        Optional.of(CollectionIdentifier.DEFAULT_COLLECTION),
                        Optional.of(CollectionIdentifier.DEFAULT_SCOPE)),
                BestEffortRetryStrategy.INSTANCE,
                NoopRequestSpan.INSTANCE);
    }
}
