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

import static org.apache.nifi.couchbase.CouchbaseConfigurationProperties.BUCKET_NAME;
import static org.apache.nifi.couchbase.CouchbaseConfigurationProperties.COUCHBASE_CLUSTER_SERVICE;
import static org.apache.nifi.couchbase.CouchbaseConfigurationProperties.DOCUMENT_TYPE;
import static org.apache.nifi.processors.couchbase.CouchbaseAttributes.Exception;
import static org.apache.nifi.processors.couchbase.AbstractCouchbaseProcessor.DOC_ID;
import static org.apache.nifi.processors.couchbase.AbstractCouchbaseProcessor.REL_FAILURE;
import static org.apache.nifi.processors.couchbase.AbstractCouchbaseProcessor.REL_RETRY;
import static org.apache.nifi.processors.couchbase.AbstractCouchbaseProcessor.REL_SUCCESS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import com.couchbase.client.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.java.document.BinaryDocument;
import org.apache.nifi.attribute.expression.language.exception.AttributeExpressionLanguageException;
import org.apache.nifi.couchbase.CouchbaseClusterControllerService;
import org.apache.nifi.couchbase.DocumentType;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.core.ServiceNotAvailableException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.ReplicateTo;
import com.couchbase.client.java.document.RawJsonDocument;
import com.couchbase.client.java.error.DurabilityException;


public class TestPutCouchbaseKey {

    private static final String SERVICE_ID = "couchbaseClusterService";
    private TestRunner testRunner;

    @Before
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
        int expiry = 100;
        long cas = 200L;

        String inFileData = "{\"key\":\"value\"}";
        byte[] inFileDataBytes = inFileData.getBytes(StandardCharsets.UTF_8);

        Bucket bucket = mock(Bucket.class);
        when(bucket.upsert(any(RawJsonDocument.class), eq(PersistTo.NONE), eq(ReplicateTo.NONE)))
            .thenReturn(RawJsonDocument.create(docId, expiry, inFileData, cas));
        setupMockBucket(bucket);

        testRunner.enqueue(inFileDataBytes);
        testRunner.setProperty(BUCKET_NAME, bucketName);
        testRunner.setProperty(DOC_ID, docId);
        testRunner.run();

        verify(bucket, times(1)).upsert(any(RawJsonDocument.class), eq(PersistTo.NONE), eq(ReplicateTo.NONE));

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
        outFile.assertAttributeEquals(CouchbaseAttributes.Expiry.key(), String.valueOf(expiry));
    }

    @Test
    public void testBinaryDoc() throws Exception {
        String bucketName = "bucket-1";
        String docId = "doc-a";
        int expiry = 100;
        long cas = 200L;

        String inFileData = "12345";
        byte[] inFileDataBytes = inFileData.getBytes(StandardCharsets.UTF_8);

        Bucket bucket = mock(Bucket.class);
        when(bucket.upsert(any(BinaryDocument.class), eq(PersistTo.NONE), eq(ReplicateTo.NONE)))
                .thenReturn(BinaryDocument.create(docId, expiry, Unpooled.copiedBuffer(inFileData.getBytes(StandardCharsets.UTF_8)), cas));
        setupMockBucket(bucket);

        testRunner.enqueue(inFileDataBytes);
        testRunner.setProperty(BUCKET_NAME, bucketName);
        testRunner.setProperty(DOC_ID, docId);
        testRunner.setProperty(DOCUMENT_TYPE, DocumentType.Binary.name());
        testRunner.run();

        verify(bucket, times(1)).upsert(any(BinaryDocument.class), eq(PersistTo.NONE), eq(ReplicateTo.NONE));

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
        outFile.assertAttributeEquals(CouchbaseAttributes.Expiry.key(), String.valueOf(expiry));
    }

    @Test
    public void testDurabilityConstraint() throws Exception {
        String docId = "doc-a";

        String inFileData = "{\"key\":\"value\"}";
        byte[] inFileDataBytes = inFileData.getBytes(StandardCharsets.UTF_8);

        Bucket bucket = mock(Bucket.class);
        when(bucket.upsert(any(RawJsonDocument.class), eq(PersistTo.MASTER), eq(ReplicateTo.ONE)))
            .thenReturn(RawJsonDocument.create(docId, inFileData));
        setupMockBucket(bucket);

        testRunner.enqueue(inFileDataBytes);
        testRunner.setProperty(DOC_ID, docId);
        testRunner.setProperty(PutCouchbaseKey.PERSIST_TO, PersistTo.MASTER.toString());
        testRunner.setProperty(PutCouchbaseKey.REPLICATE_TO, ReplicateTo.ONE.toString());
        testRunner.run();

        verify(bucket, times(1)).upsert(any(RawJsonDocument.class), eq(PersistTo.MASTER), eq(ReplicateTo.ONE));

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

        Bucket bucket = mock(Bucket.class);
        when(bucket.upsert(any(RawJsonDocument.class), eq(PersistTo.NONE), eq(ReplicateTo.NONE)))
            .thenReturn(RawJsonDocument.create(somePropertyValue, inFileData));
        setupMockBucket(bucket);

        testRunner.setProperty(DOC_ID, docIdExp);

        Map<String, String> properties = new HashMap<>();
        properties.put("someProperty", somePropertyValue);
        testRunner.enqueue(inFileDataBytes, properties);
        testRunner.run();

        ArgumentCaptor<RawJsonDocument> capture = ArgumentCaptor.forClass(RawJsonDocument.class);
        verify(bucket, times(1)).upsert(capture.capture(), eq(PersistTo.NONE), eq(ReplicateTo.NONE));
        assertEquals(somePropertyValue, capture.getValue().id());
        assertEquals(inFileData, capture.getValue().content());

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
        when(bucket.upsert(any(RawJsonDocument.class), eq(PersistTo.NONE), eq(ReplicateTo.NONE)))
            .thenReturn(RawJsonDocument.create(somePropertyValue, inFileData));
        setupMockBucket(bucket);

        testRunner.setProperty(DOC_ID, docIdExp);

        Map<String, String> properties = new HashMap<>();
        properties.put("someProperty", somePropertyValue);
        testRunner.enqueue(inFileDataBytes, properties);
        try {
            testRunner.run();
            fail("Exception should be thrown.");
        } catch (AssertionError e){
            Assert.assertTrue(e.getCause().getClass().equals(AttributeExpressionLanguageException.class));
        }

        testRunner.assertTransferCount(REL_SUCCESS, 0);
        testRunner.assertTransferCount(REL_RETRY, 0);
        testRunner.assertTransferCount(REL_FAILURE, 0);
    }

    @Test
    public void testInputFlowFileUuid() throws Exception {

        String uuid = "00029362-5106-40e8-b8a9-bf2cecfbc0d7";
        String inFileData = "{\"key\":\"value\"}";
        byte[] inFileDataBytes = inFileData.getBytes(StandardCharsets.UTF_8);

        Bucket bucket = mock(Bucket.class);
        when(bucket.upsert(any(RawJsonDocument.class), eq(PersistTo.NONE), eq(ReplicateTo.NONE)))
            .thenReturn(RawJsonDocument.create(uuid, inFileData));
        setupMockBucket(bucket);

        Map<String, String> properties = new HashMap<>();
        properties.put(CoreAttributes.UUID.key(), uuid);
        testRunner.enqueue(inFileDataBytes, properties);
        testRunner.run();

        ArgumentCaptor<RawJsonDocument> capture = ArgumentCaptor.forClass(RawJsonDocument.class);
        verify(bucket, times(1)).upsert(capture.capture(), eq(PersistTo.NONE), eq(ReplicateTo.NONE));
        assertEquals(inFileData, capture.getValue().content());

        testRunner.assertTransferCount(REL_SUCCESS, 1);
        testRunner.assertTransferCount(REL_RETRY, 0);
        testRunner.assertTransferCount(REL_FAILURE, 0);
        MockFlowFile outFile = testRunner.getFlowFilesForRelationship(REL_SUCCESS).get(0);
        outFile.assertContentEquals(inFileData);
    }


    @Test
    public void testCouchbaseFailure() throws Exception {

        String docId = "doc-a";

        String inFileData = "{\"key\":\"value\"}";
        byte[] inFileDataBytes = inFileData.getBytes(StandardCharsets.UTF_8);

        Bucket bucket = mock(Bucket.class);
        when(bucket.upsert(any(RawJsonDocument.class), eq(PersistTo.NONE), eq(ReplicateTo.ONE)))
            .thenThrow(new ServiceNotAvailableException());
        setupMockBucket(bucket);

        testRunner.enqueue(inFileDataBytes);
        testRunner.setProperty(DOC_ID, docId);
        testRunner.setProperty(PutCouchbaseKey.REPLICATE_TO, ReplicateTo.ONE.toString());
        try {
            testRunner.run();
            fail("ProcessException should be thrown.");
        } catch (AssertionError e){
            Assert.assertTrue(e.getCause().getClass().equals(ProcessException.class));
        }

        verify(bucket, times(1)).upsert(any(RawJsonDocument.class), eq(PersistTo.NONE), eq(ReplicateTo.ONE));

        testRunner.assertAllFlowFilesTransferred(REL_FAILURE);
        testRunner.assertTransferCount(REL_SUCCESS, 0);
        testRunner.assertTransferCount(REL_RETRY, 0);
        testRunner.assertTransferCount(REL_FAILURE, 0);
    }

    @Test
    public void testCouchbaseTempFlowFileError() throws Exception {

        String docId = "doc-a";

        String inFileData = "{\"key\":\"value\"}";
        byte[] inFileDataBytes = inFileData.getBytes(StandardCharsets.UTF_8);

        Bucket bucket = mock(Bucket.class);
        CouchbaseException exception = new DurabilityException();
        when(bucket.upsert(any(RawJsonDocument.class), eq(PersistTo.NONE), eq(ReplicateTo.ONE)))
            .thenThrow(exception);
        setupMockBucket(bucket);

        testRunner.enqueue(inFileDataBytes);
        testRunner.setProperty(DOC_ID, docId);
        testRunner.setProperty(PutCouchbaseKey.REPLICATE_TO, ReplicateTo.ONE.toString());
        testRunner.run();

        verify(bucket, times(1)).upsert(any(RawJsonDocument.class), eq(PersistTo.NONE), eq(ReplicateTo.ONE));

        testRunner.assertTransferCount(REL_SUCCESS, 0);
        testRunner.assertTransferCount(REL_RETRY, 1);
        testRunner.assertTransferCount(REL_FAILURE, 0);
        MockFlowFile orgFile = testRunner.getFlowFilesForRelationship(REL_RETRY).get(0);
        orgFile.assertContentEquals(inFileData);
        orgFile.assertAttributeEquals(Exception.key(), exception.getClass().getName());
    }
}
