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
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.PasswordAuthenticator;
import com.couchbase.client.core.error.AmbiguousTimeoutException;
import com.couchbase.client.core.error.AuthenticationFailureException;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.error.DurabilityImpossibleException;
import com.couchbase.client.core.error.InvalidRequestException;
import com.couchbase.client.core.error.ServiceNotAvailableException;
import com.couchbase.client.core.error.context.CancellationErrorContext;
import com.couchbase.client.core.error.context.GenericRequestErrorContext;
import com.couchbase.client.core.error.context.KeyValueErrorContext;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.kv.GetRequest;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.core.util.ConnectionString;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.kv.GetOptions;
import com.couchbase.client.java.kv.GetResult;
import org.apache.nifi.attribute.expression.language.exception.AttributeExpressionLanguageException;
import org.apache.nifi.couchbase.CouchbaseClusterControllerService;
import org.apache.nifi.couchbase.CouchbaseConfigurationProperties;
import org.apache.nifi.couchbase.DocumentType;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.nifi.couchbase.CouchbaseConfigurationProperties.BUCKET_NAME;
import static org.apache.nifi.couchbase.CouchbaseConfigurationProperties.COUCHBASE_CLUSTER_SERVICE;
import static org.apache.nifi.couchbase.CouchbaseConfigurationProperties.DOCUMENT_TYPE;
import static org.apache.nifi.processors.couchbase.AbstractCouchbaseProcessor.DOC_ID;
import static org.apache.nifi.processors.couchbase.AbstractCouchbaseProcessor.REL_FAILURE;
import static org.apache.nifi.processors.couchbase.AbstractCouchbaseProcessor.REL_ORIGINAL;
import static org.apache.nifi.processors.couchbase.AbstractCouchbaseProcessor.REL_RETRY;
import static org.apache.nifi.processors.couchbase.AbstractCouchbaseProcessor.REL_SUCCESS;
import static org.apache.nifi.processors.couchbase.CouchbaseAttributes.Exception;
import static org.apache.nifi.processors.couchbase.GetCouchbaseKey.PUT_VALUE_TO_ATTRIBUTE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class TestGetCouchbaseKey {

    private static final String SERVICE_ID = "couchbaseClusterService";

    private final CoreEnvironment coreEnvironment = CoreEnvironment.builder().build();
    private final PasswordAuthenticator passwordAuthenticator =
            new PasswordAuthenticator.Builder().username("couchbase").password("b1password").build();
    private final ConnectionString connectionString = ConnectionString.create("couchbase://192.168.99.100");
    private final Core core = Core.create(coreEnvironment, passwordAuthenticator, connectionString);
    private final CoreContext coreContext = new CoreContext(core, 1, coreEnvironment, passwordAuthenticator);

    private TestRunner testRunner;

    @BeforeEach
    public void init() throws Exception {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi.processors.couchbase.GetCouchbaseKey", "debug");
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi.processors.couchbase.TestGetCouchbaseKey", "debug");

        testRunner = TestRunners.newTestRunner(GetCouchbaseKey.class);
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

        Bucket bucket = mock(Bucket.class);
        Collection collection = mock(Collection.class);
        GetResult getResult = mock(GetResult.class);
        String content = "{\"key\":\"value\"}";
        long cas = 200L;
        setupMockBucket(bucket);
        when(bucket.collection(anyString())).thenReturn(collection);
        when(getResult.cas()).thenReturn(cas);
        Optional<Instant> expiryTime = Optional.of(Instant.now());
        when(getResult.expiryTime()).thenReturn(expiryTime);
        when(getResult.contentAsBytes()).thenReturn(content.getBytes(StandardCharsets.UTF_8));
        when(collection.get(any(), any(GetOptions.class))).thenReturn(getResult);

        testRunner.setProperty(BUCKET_NAME, bucketName);
        testRunner.setProperty(DOC_ID, docId);
        testRunner.enqueue(new byte[0]);
        testRunner.run();

        testRunner.assertTransferCount(REL_SUCCESS, 1);
        testRunner.assertTransferCount(REL_ORIGINAL, 1);
        testRunner.assertTransferCount(REL_RETRY, 0);
        testRunner.assertTransferCount(REL_FAILURE, 0);
        MockFlowFile outFile = testRunner.getFlowFilesForRelationship(REL_SUCCESS).get(0);
        outFile.assertContentEquals(content);

        outFile.assertAttributeEquals(CouchbaseAttributes.Cluster.key(), SERVICE_ID);
        outFile.assertAttributeEquals(CouchbaseAttributes.Bucket.key(), bucketName);
        outFile.assertAttributeEquals(CouchbaseAttributes.DocId.key(), docId);
        outFile.assertAttributeEquals(CouchbaseAttributes.Cas.key(), String.valueOf(cas));
        outFile.assertAttributeEquals(CouchbaseAttributes.Expiry.key(), String.valueOf(expiryTime.get()));
    }


    @Test
    public void testDocIdExp() throws Exception {
        String docIdExp = "${'someProperty'}";
        String somePropertyValue = "doc-p";

        Bucket bucket = mock(Bucket.class);
        Collection collection = mock(Collection.class);
        GetResult getResult = mock(GetResult.class);
        String content = "{\"key\":\"value\"}";
        setupMockBucket(bucket);
        when(bucket.collection(anyString())).thenReturn(collection);
        when(getResult.expiryTime()).thenReturn(Optional.of(Instant.now()));
        when(getResult.contentAsBytes()).thenReturn(content.getBytes(StandardCharsets.UTF_8));
        when(collection.get(any(), any(GetOptions.class))).thenReturn(getResult);

        testRunner.setProperty(DOC_ID, docIdExp);

        byte[] inFileData = "input FlowFile data".getBytes(StandardCharsets.UTF_8);
        Map<String, String> properties = new HashMap<>();
        properties.put("someProperty", somePropertyValue);
        testRunner.enqueue(inFileData, properties);
        testRunner.run();

        testRunner.assertTransferCount(REL_SUCCESS, 1);
        testRunner.assertTransferCount(REL_ORIGINAL, 1);
        testRunner.assertTransferCount(REL_RETRY, 0);
        testRunner.assertTransferCount(REL_FAILURE, 0);
        MockFlowFile outFile = testRunner.getFlowFilesForRelationship(REL_SUCCESS).get(0);
        outFile.assertContentEquals(content);
    }

    @Test
    public void testDocIdExpWithEmptyFlowFile() throws Exception {
        String docIdExp = "doc-s";
        String docId = "doc-s";

        Bucket bucket = mock(Bucket.class);
        Collection collection = mock(Collection.class);
        GetResult getResult = mock(GetResult.class);
        String content = "{\"key\":\"value\"}";
        setupMockBucket(bucket);
        when(bucket.collection(anyString())).thenReturn(collection);
        when(getResult.expiryTime()).thenReturn(Optional.of(Instant.now()));
        when(getResult.contentAsBytes()).thenReturn(content.getBytes(StandardCharsets.UTF_8));
        when(collection.get(any(), any(GetOptions.class))).thenReturn(getResult);

        testRunner.setProperty(DOC_ID, docIdExp);

        testRunner.enqueue(new byte[0]);
        testRunner.run();

        testRunner.assertTransferCount(REL_SUCCESS, 1);
        testRunner.assertTransferCount(REL_ORIGINAL, 1);
        testRunner.assertTransferCount(REL_RETRY, 0);
        testRunner.assertTransferCount(REL_FAILURE, 0);
        MockFlowFile outFile = testRunner.getFlowFilesForRelationship(REL_SUCCESS).get(0);
        outFile.assertContentEquals(content);
    }

    @Test
    public void testDocIdExpWithInvalidExpression() throws Exception {
        String docIdExp = "${nonExistingFunction('doc-s')}";
        String docId = "doc-s";

        Bucket bucket = mock(Bucket.class);
        Collection collection = mock(Collection.class);
        GetResult getResult = mock(GetResult.class);
        setupMockBucket(bucket);
        when(bucket.collection(anyString())).thenReturn(collection);
        when(getResult.expiryTime()).thenReturn(Optional.of(Instant.now()));
        when(collection.get(any(), any(GetOptions.class))).thenReturn(getResult);

        testRunner.setProperty(DOC_ID, docIdExp);
        testRunner.enqueue(new byte[0]);

        AssertionError e = assertThrows(AssertionError.class, () -> testRunner.run());
        assertEquals(e.getCause().getClass(), AttributeExpressionLanguageException.class);

        testRunner.assertTransferCount(REL_SUCCESS, 0);
        testRunner.assertTransferCount(REL_ORIGINAL, 0);
        testRunner.assertTransferCount(REL_RETRY, 0);
        testRunner.assertTransferCount(REL_FAILURE, 0);
    }

    @Test
    public void testDocIdExpWithInvalidExpressionOnFlowFile() throws Exception {
        String docIdExp = "${nonExistingFunction(someProperty)}";

        Bucket bucket = mock(Bucket.class);
        Collection collection = mock(Collection.class);
        GetResult getResult = mock(GetResult.class);
        setupMockBucket(bucket);
        when(bucket.collection(anyString())).thenReturn(collection);
        when(getResult.expiryTime()).thenReturn(Optional.of(Instant.now()));
        when(collection.get(anyString(), any(GetOptions.class))).thenReturn(getResult);

        testRunner.setProperty(DOC_ID, docIdExp);

        String inputFileDataStr = "input FlowFile data";
        byte[] inFileData = inputFileDataStr.getBytes(StandardCharsets.UTF_8);
        Map<String, String> properties = new HashMap<>();
        properties.put("someProperty", "someValue");
        testRunner.enqueue(inFileData, properties);

        AssertionError e = assertThrows(AssertionError.class, () -> testRunner.run());
        assertEquals(e.getCause().getClass(), AttributeExpressionLanguageException.class);

        testRunner.assertTransferCount(REL_SUCCESS, 0);
        testRunner.assertTransferCount(REL_ORIGINAL, 0);
        testRunner.assertTransferCount(REL_RETRY, 0);
        testRunner.assertTransferCount(REL_FAILURE, 0);
    }

    @Test
    public void testInputFlowFileContent() throws Exception {

        Bucket bucket = mock(Bucket.class);
        Collection collection = mock(Collection.class);
        GetResult getResult = mock(GetResult.class);
        String inFileDataStr = "doc-in";
        String content = "{\"key\":\"value\"}";
        setupMockBucket(bucket);
        when(bucket.collection(anyString())).thenReturn(collection);
        when(getResult.expiryTime()).thenReturn(Optional.of(Instant.now()));
        when(getResult.contentAsBytes()).thenReturn(content.getBytes(StandardCharsets.UTF_8));
        when(collection.get(anyString(), any(GetOptions.class))).thenReturn(getResult);


        byte[] inFileData = inFileDataStr.getBytes(StandardCharsets.UTF_8);
        testRunner.enqueue(inFileData);
        testRunner.run();

        testRunner.assertTransferCount(REL_SUCCESS, 1);
        testRunner.assertTransferCount(REL_ORIGINAL, 1);
        testRunner.assertTransferCount(REL_RETRY, 0);
        testRunner.assertTransferCount(REL_FAILURE, 0);
        MockFlowFile outFile = testRunner.getFlowFilesForRelationship(REL_SUCCESS).get(0);
        outFile.assertContentEquals(content);
        MockFlowFile orgFile = testRunner.getFlowFilesForRelationship(REL_ORIGINAL).get(0);
        orgFile.assertContentEquals(inFileDataStr);
    }

    @Test
    public void testPutToAttribute() throws Exception {

        Bucket bucket = mock(Bucket.class);
        Collection collection = mock(Collection.class);
        GetResult getResult = mock(GetResult.class);
        String inFileDataStr = "doc-in";
        String content = "some-value";
        setupMockBucket(bucket);
        when(bucket.collection(anyString())).thenReturn(collection);
        Optional<Instant> expiryTime = Optional.of(Instant.now());
        when(getResult.expiryTime()).thenReturn(expiryTime);
        final ByteBuf contents = Unpooled.copiedBuffer(content.getBytes(StandardCharsets.UTF_8));
        when(getResult.contentAs(ByteBuf.class)).thenReturn(contents);
        when(getResult.contentAsBytes()).thenReturn(content.getBytes(StandardCharsets.UTF_8));

        when(collection.get(anyString(), any(GetOptions.class))).thenReturn(getResult);

        byte[] inFileData = inFileDataStr.getBytes(StandardCharsets.UTF_8);
        testRunner.setProperty(PUT_VALUE_TO_ATTRIBUTE, "targetAttribute");
        testRunner.setProperty(CouchbaseConfigurationProperties.DOCUMENT_TYPE, "Binary");
        testRunner.enqueue(inFileData);
        testRunner.run();

        testRunner.assertTransferCount(REL_SUCCESS, 1);
        // Result is put to Attribute, so no need to pass it to original.
        testRunner.assertTransferCount(REL_ORIGINAL, 0);
        testRunner.assertTransferCount(REL_RETRY, 0);
        testRunner.assertTransferCount(REL_FAILURE, 0);
        MockFlowFile outFile = testRunner.getFlowFilesForRelationship(REL_SUCCESS).get(0);
        outFile.assertContentEquals(inFileDataStr);
        outFile.assertAttributeEquals("targetAttribute", content);

        assertEquals(1, testRunner.getProvenanceEvents().size());
        assertEquals(ProvenanceEventType.FETCH, testRunner.getProvenanceEvents().get(0).getEventType());
    }

    @Test
    public void testPutToAttributeNoTargetAttribute() throws Exception {

        Bucket bucket = mock(Bucket.class);
        Collection collection = mock(Collection.class);
        GetResult getResult = mock(GetResult.class);
        String inFileDataStr = "doc-in";
        String content = "some-value";
        setupMockBucket(bucket);
        when(bucket.collection(anyString())).thenReturn(collection);
        when(getResult.expiryTime()).thenReturn(Optional.of(Instant.now()));
        when(collection.get(anyString(), any(GetOptions.class))).thenReturn(getResult);

        byte[] inFileData = inFileDataStr.getBytes(StandardCharsets.UTF_8);
        testRunner.setProperty(PUT_VALUE_TO_ATTRIBUTE, "${expressionReturningNoValue}");
        testRunner.enqueue(inFileData);
        testRunner.run();

        testRunner.assertTransferCount(REL_SUCCESS, 0);
        testRunner.assertTransferCount(REL_ORIGINAL, 0);
        testRunner.assertTransferCount(REL_RETRY, 0);
        testRunner.assertTransferCount(REL_FAILURE, 1);
        MockFlowFile outFile = testRunner.getFlowFilesForRelationship(REL_FAILURE).get(0);
        outFile.assertContentEquals(inFileDataStr);
    }

    @Test
    public void testBinaryDocument() throws Exception {

        Bucket bucket = mock(Bucket.class);
        Collection collection = mock(Collection.class);
        GetResult getResult = mock(GetResult.class);
        String inFileDataStr = "doc-in";
        String content = "binary";
        ByteBuf buf = Unpooled.copiedBuffer(content.getBytes(StandardCharsets.UTF_8));
        setupMockBucket(bucket);
        when(bucket.collection(anyString())).thenReturn(collection);
        when(getResult.expiryTime()).thenReturn(Optional.of(Instant.now()));
        when(collection.get(anyString(), any(GetOptions.class))).thenReturn(getResult);
        when(getResult.contentAs(ByteBuf.class)).thenReturn(buf);

        byte[] inFileData = inFileDataStr.getBytes(StandardCharsets.UTF_8);
        testRunner.enqueue(inFileData);
        testRunner.setProperty(DOCUMENT_TYPE, DocumentType.Binary.toString());
        testRunner.run();

        testRunner.assertTransferCount(REL_SUCCESS, 1);
        testRunner.assertTransferCount(REL_ORIGINAL, 1);
        testRunner.assertTransferCount(REL_RETRY, 0);
        testRunner.assertTransferCount(REL_FAILURE, 0);
        MockFlowFile outFile = testRunner.getFlowFilesForRelationship(REL_SUCCESS).get(0);
        outFile.assertContentEquals(content);
        MockFlowFile orgFile = testRunner.getFlowFilesForRelationship(REL_ORIGINAL).get(0);
        orgFile.assertContentEquals(inFileDataStr);
    }

    @Test
    public void testBinaryDocumentToAttribute() throws Exception {

        Bucket bucket = mock(Bucket.class);
        Collection collection = mock(Collection.class);
        GetResult getResult = mock(GetResult.class);
        String inFileDataStr = "doc-in";
        String content = "binary";
        ByteBuf buf = Unpooled.copiedBuffer(content.getBytes(StandardCharsets.UTF_8));
        setupMockBucket(bucket);
        when(bucket.collection(anyString())).thenReturn(collection);
        when(getResult.expiryTime()).thenReturn(Optional.of(Instant.now()));
        when(collection.get(anyString(), any(GetOptions.class))).thenReturn(getResult);
        when(getResult.contentAs(ByteBuf.class)).thenReturn(buf);
        when(getResult.contentAsBytes()).thenReturn(content.getBytes(StandardCharsets.UTF_8));

        byte[] inFileData = inFileDataStr.getBytes(StandardCharsets.UTF_8);
        testRunner.enqueue(inFileData);
        testRunner.setProperty(DOCUMENT_TYPE, DocumentType.Binary.toString());
        testRunner.setProperty(PUT_VALUE_TO_ATTRIBUTE, "targetAttribute");
        testRunner.run();

        testRunner.assertTransferCount(REL_SUCCESS, 1);
        testRunner.assertTransferCount(REL_ORIGINAL, 0);
        testRunner.assertTransferCount(REL_RETRY, 0);
        testRunner.assertTransferCount(REL_FAILURE, 0);
        MockFlowFile outFile = testRunner.getFlowFilesForRelationship(REL_SUCCESS).get(0);
        outFile.assertContentEquals(inFileDataStr);
        outFile.assertAttributeEquals("targetAttribute", "binary");
    }


    @Test
    public void testCouchbaseFailure() throws Exception {

        Bucket bucket = mock(Bucket.class);
        Collection collection = mock(Collection.class);
        String inFileDataStr = "doc-in";
        when(bucket.collection(anyString())).thenReturn(collection);
        when(collection.get(anyString(), any(GetOptions.class)))
                .thenThrow(new ServiceNotAvailableException("test",
                        new GenericRequestErrorContext(
                                createNewGetRequest("bucket-a", "collection-a")
                        )));
        setupMockBucket(bucket);


        byte[] inFileData = inFileDataStr.getBytes(StandardCharsets.UTF_8);
        testRunner.enqueue(inFileData);

        AssertionError e = assertThrows(AssertionError.class, () -> testRunner.run());
        assertEquals(e.getCause().getClass(), ProcessException.class);

        testRunner.assertTransferCount(REL_SUCCESS, 0);
        testRunner.assertTransferCount(REL_ORIGINAL, 0);
        testRunner.assertTransferCount(REL_RETRY, 0);
        testRunner.assertTransferCount(REL_FAILURE, 0);
    }

    @Test
    public void testCouchbaseConfigurationError() throws Exception {
        String docIdExp = "doc-c";

        Bucket bucket = mock(Bucket.class);
        Collection collection = mock(Collection.class);
        when(bucket.collection(anyString())).thenReturn(collection);
        when(collection.get(anyString(), any(GetOptions.class)))
                .thenThrow(new AuthenticationFailureException("test", new KeyValueErrorContext(
                        createNewGetRequest("bucket-a", "bucket-c"),
                        mock(ResponseStatus.class),
                        mock(MemcacheProtocol.FlexibleExtras.class)),
                        new RuntimeException()));
        setupMockBucket(bucket);

        testRunner.setProperty(DOC_ID, docIdExp);

        String inputFileDataStr = "input FlowFile data";
        byte[] inFileData = inputFileDataStr.getBytes(StandardCharsets.UTF_8);
        testRunner.enqueue(inFileData);

        AssertionError e = assertThrows(AssertionError.class, () -> testRunner.run());
        assertEquals(e.getCause().getClass(), ProcessException.class);
        assertEquals(e.getCause().getCause().getClass(), AuthenticationFailureException.class);

        testRunner.assertTransferCount(REL_SUCCESS, 0);
        testRunner.assertTransferCount(REL_ORIGINAL, 0);
        testRunner.assertTransferCount(REL_RETRY, 0);
        testRunner.assertTransferCount(REL_FAILURE, 0);
    }

    @Test
    public void testCouchbaseInvalidInputError() throws Exception {
        String docIdExp = "doc-c";

        Bucket bucket = mock(Bucket.class);
        Collection collection = mock(Collection.class);
        CouchbaseException exception = new InvalidRequestException(new KeyValueErrorContext(
                createNewGetRequest("bucket-a", "collection-a"),
                mock(ResponseStatus.class),
                mock(MemcacheProtocol.FlexibleExtras.class)));
        when(bucket.collection(anyString())).thenReturn(collection);
        when(collection.get(any(), any(GetOptions.class)))
                .thenThrow(exception);
        setupMockBucket(bucket);

        testRunner.setProperty(DOC_ID, docIdExp);

        String inputFileDataStr = "input FlowFile data";
        byte[] inFileData = inputFileDataStr.getBytes(StandardCharsets.UTF_8);
        testRunner.enqueue(inFileData);
        testRunner.run();

        testRunner.assertTransferCount(REL_SUCCESS, 0);
        testRunner.assertTransferCount(REL_ORIGINAL, 0);
        testRunner.assertTransferCount(REL_RETRY, 0);
        testRunner.assertTransferCount(REL_FAILURE, 1);
        MockFlowFile orgFile = testRunner.getFlowFilesForRelationship(REL_FAILURE).get(0);
        orgFile.assertContentEquals(inputFileDataStr);
        orgFile.assertAttributeEquals(Exception.key(), exception.getClass().getName());
    }

    @Test
    public void testCouchbaseTempClusterError() throws Exception {
        String docIdExp = "doc-c";

        Bucket bucket = mock(Bucket.class);
        Collection collection = mock(Collection.class);
        CouchbaseException exception = new AmbiguousTimeoutException(
                "test",
                new CancellationErrorContext(new KeyValueErrorContext(
                        createNewGetRequest("bucket-a", "collection-a"),
                        mock(ResponseStatus.class),
                        mock(MemcacheProtocol.FlexibleExtras.class)))
        );
        when(bucket.collection(anyString())).thenReturn(collection);
        when(collection.get(anyString(), any(GetOptions.class)))
                .thenThrow(exception);
        setupMockBucket(bucket);

        testRunner.setProperty(DOC_ID, docIdExp);

        String inputFileDataStr = "input FlowFile data";
        byte[] inFileData = inputFileDataStr.getBytes(StandardCharsets.UTF_8);
        testRunner.enqueue(inFileData);
        testRunner.run();

        testRunner.assertTransferCount(REL_SUCCESS, 0);
        testRunner.assertTransferCount(REL_ORIGINAL, 0);
        testRunner.assertTransferCount(REL_RETRY, 1);
        testRunner.assertTransferCount(REL_FAILURE, 0);
        MockFlowFile orgFile = testRunner.getFlowFilesForRelationship(REL_RETRY).get(0);
        orgFile.assertContentEquals(inputFileDataStr);
        orgFile.assertAttributeEquals(Exception.key(), exception.getClass().getName());
    }


    @Test
    public void testCouchbaseTempFlowFileError() throws Exception {
        String docIdExp = "doc-c";

        Bucket bucket = mock(Bucket.class);
        Collection collection = mock(Collection.class);
        CouchbaseException exception = new DurabilityImpossibleException(new KeyValueErrorContext(
                createNewGetRequest("bucket-a", "collection-a"),
                mock(ResponseStatus.class),
                mock(MemcacheProtocol.FlexibleExtras.class)));
        when(bucket.collection(anyString())).thenReturn(collection);
        when(collection.get(anyString(), any(GetOptions.class)))
                .thenThrow(exception);
        setupMockBucket(bucket);

        testRunner.setProperty(DOC_ID, docIdExp);

        String inputFileDataStr = "input FlowFile data";
        byte[] inFileData = inputFileDataStr.getBytes(StandardCharsets.UTF_8);
        testRunner.enqueue(inFileData);
        testRunner.run();

        testRunner.assertTransferCount(REL_SUCCESS, 0);
        testRunner.assertTransferCount(REL_ORIGINAL, 0);
        testRunner.assertTransferCount(REL_RETRY, 1);
        testRunner.assertTransferCount(REL_FAILURE, 0);
        MockFlowFile orgFile = testRunner.getFlowFilesForRelationship(REL_RETRY).get(0);
        orgFile.assertContentEquals(inputFileDataStr);
        orgFile.assertAttributeEquals(Exception.key(), exception.getClass().getName());
        assertTrue(orgFile.isPenalized());
    }

    @Test
    public void testDocumentNotFound() throws Exception {
        String docIdExp = "doc-n";

        Bucket bucket = mock(Bucket.class);
        Collection collection = mock(Collection.class);
        when(bucket.collection(anyString())).thenReturn(collection);
        when(collection.get(anyString(), any(GetOptions.class)))
                .thenReturn(null);
        setupMockBucket(bucket);

        testRunner.setProperty(DOC_ID, docIdExp);

        String inputFileDataStr = "input FlowFile data";
        byte[] inFileData = inputFileDataStr.getBytes(StandardCharsets.UTF_8);
        testRunner.enqueue(inFileData);
        testRunner.run();

        testRunner.assertTransferCount(REL_SUCCESS, 0);
        testRunner.assertTransferCount(REL_ORIGINAL, 0);
        testRunner.assertTransferCount(REL_RETRY, 0);
        testRunner.assertTransferCount(REL_FAILURE, 1);
        MockFlowFile orgFile = testRunner.getFlowFilesForRelationship(REL_FAILURE).get(0);
        orgFile.assertContentEquals(inputFileDataStr);
        orgFile.assertAttributeEquals(Exception.key(), DocumentNotFoundException.class.getName());
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
