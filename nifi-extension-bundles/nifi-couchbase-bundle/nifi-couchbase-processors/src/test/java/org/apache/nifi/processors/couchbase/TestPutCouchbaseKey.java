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

import com.couchbase.client.core.CoreKeyspace;
import com.couchbase.client.core.api.kv.CoreMutationResult;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.DurabilityImpossibleException;
import com.couchbase.client.core.error.ServiceNotAvailableException;
import com.couchbase.client.core.error.context.ErrorContext;
import com.couchbase.client.core.error.context.KeyValueErrorContext;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.ReplicateTo;
import com.couchbase.client.java.kv.UpsertOptions;
import org.apache.nifi.attribute.expression.language.exception.AttributeExpressionLanguageException;
import org.apache.nifi.couchbase.AbstractCouchbaseNifiTest;
import org.apache.nifi.couchbase.DocumentType;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.MockFlowFile;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.nifi.couchbase.CouchbaseConfigurationProperties.BUCKET_NAME;
import static org.apache.nifi.couchbase.CouchbaseConfigurationProperties.COLLECTION_NAME;
import static org.apache.nifi.couchbase.CouchbaseConfigurationProperties.DOCUMENT_TYPE;
import static org.apache.nifi.couchbase.CouchbaseConfigurationProperties.SCOPE_NAME;
import static org.apache.nifi.processors.couchbase.AbstractCouchbaseProcessor.DOC_ID;
import static org.apache.nifi.processors.couchbase.AbstractCouchbaseProcessor.REL_FAILURE;
import static org.apache.nifi.processors.couchbase.AbstractCouchbaseProcessor.REL_RETRY;
import static org.apache.nifi.processors.couchbase.AbstractCouchbaseProcessor.REL_SUCCESS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class TestPutCouchbaseKey extends AbstractCouchbaseNifiTest {

    @Test
    public void testStaticDocId() throws Exception {
        String bucketName = "bucket-1";
        String docId = "doc-a";
        long cas = 200L;

        String inFileData = "{\"key\":\"value\"}";
        byte[] inFileDataBytes = inFileData.getBytes(StandardCharsets.UTF_8);

        Collection mcol = setupMockCouchbase(bucketName, scopeName, collectionName);
        when(mcol.upsert(eq(docId), any(), any(UpsertOptions.class)))
                .thenReturn(new MutationResult(
                        new CoreMutationResult(null,
                                CoreKeyspace.from(new CollectionIdentifier(bucketName, Optional.of(scopeName), Optional.of(collectionName))),
                                docId, cas, Optional.empty() )
                ));

        testRunner.enqueue(inFileDataBytes);
        testRunner.setProperty(BUCKET_NAME, bucketName);
        testRunner.setProperty(SCOPE_NAME, scopeName);
        testRunner.setProperty(COLLECTION_NAME, collectionName);
        testRunner.setProperty(DOC_ID, docId);
        testRunner.run();

        verify(mcol, times(1)).upsert(eq(docId), any(), any(UpsertOptions.class));

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

        Collection mcol = setupMockCouchbase(bucketName, scopeName, collectionName);
        when(mcol.upsert(eq(docId), any(), any(UpsertOptions.class)))
                .thenReturn(new MutationResult(new CoreMutationResult(null,
                        CoreKeyspace.from(new CollectionIdentifier(bucketName, Optional.of(scopeName), Optional.of(collectionName))),
                        docId, cas, Optional.empty()
                )));

        testRunner.enqueue(inFileDataBytes);
        testRunner.setProperty(BUCKET_NAME, bucketName);
        testRunner.setProperty(SCOPE_NAME, scopeName);
        testRunner.setProperty(COLLECTION_NAME, collectionName);
        testRunner.setProperty(DOC_ID, docId);
        testRunner.setProperty(DOCUMENT_TYPE, DocumentType.Binary.name());
        testRunner.run();

        verify(mcol, times(1)).upsert(eq(docId), any(), any(UpsertOptions.class));

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
        long cas = 200L;

        String inFileData = "{\"key\":\"value\"}";
        byte[] inFileDataBytes = inFileData.getBytes(StandardCharsets.UTF_8);

        Collection mcol = setupMockCouchbase(bucketName, scopeName, collectionName);
        when(mcol.upsert(eq(docId), any(), any(UpsertOptions.class)))
                .thenReturn(new MutationResult(new CoreMutationResult(null,
                        CoreKeyspace.from(new CollectionIdentifier(bucketName, Optional.of(scopeName), Optional.of(collectionName))),
                        docId, cas, Optional.empty()
                )));

        testRunner.enqueue(inFileDataBytes);
        testRunner.setProperty(BUCKET_NAME, bucketName);
        testRunner.setProperty(SCOPE_NAME, scopeName);
        testRunner.setProperty(COLLECTION_NAME, collectionName);
        testRunner.setProperty(DOC_ID, docId);
        testRunner.setProperty(PutCouchbaseKey.PERSIST_TO, PersistTo.ONE.toString());
        testRunner.setProperty(PutCouchbaseKey.REPLICATE_TO, ReplicateTo.ONE.toString());
        testRunner.run();

        verify(mcol, times(1)).upsert(eq(docId), any(), any(UpsertOptions.class));

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

        Collection mcol = setupMockCouchbase(bucketName, scopeName, collectionName);
        when(mcol.upsert(eq(somePropertyValue), eq(inFileDataBytes), any(UpsertOptions.class)))
                .thenReturn(new MutationResult(
                        new CoreMutationResult(null,
                                CoreKeyspace.from(new CollectionIdentifier(bucketName, Optional.of(scopeName), Optional.of(collectionName))),
                                somePropertyValue, 200L, Optional.empty()
                        )
                ));

        testRunner.setProperty(BUCKET_NAME, bucketName);
        testRunner.setProperty(SCOPE_NAME, scopeName);
        testRunner.setProperty(COLLECTION_NAME, collectionName);
        testRunner.setProperty(DOC_ID, docIdExp);

        Map<String, String> properties = new HashMap<>();
        properties.put("someProperty", somePropertyValue);
        testRunner.enqueue(inFileDataBytes, properties);
        testRunner.run();

        verify(mcol, times(1)).upsert(eq(somePropertyValue), eq(inFileDataBytes), any(UpsertOptions.class));

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

        Collection mcol = setupMockCouchbase(bucketName, scopeName, collectionName);
        when(mcol.upsert(eq(somePropertyValue), any(), any(UpsertOptions.class)))
                .thenReturn(new MutationResult(new CoreMutationResult(null,
                        CoreKeyspace.from(new CollectionIdentifier(bucketName, Optional.of(scopeName), Optional.of(collectionName))),
                        somePropertyValue, 200L, Optional.empty()
                )));

        testRunner.setProperty(BUCKET_NAME, bucketName);
        testRunner.setProperty(SCOPE_NAME, scopeName);
        testRunner.setProperty(COLLECTION_NAME, collectionName);
        testRunner.setProperty(DOC_ID, docIdExp);

        Map<String, String> properties = new HashMap<>();
        properties.put("someProperty", somePropertyValue);
        testRunner.enqueue(inFileDataBytes, properties);

        AssertionError e = assertThrows(AssertionError.class, () -> testRunner.run());
        assertEquals(AttributeExpressionLanguageException.class, e.getCause().getClass());

        testRunner.assertTransferCount(REL_SUCCESS, 0);
        testRunner.assertTransferCount(REL_RETRY, 0);
        testRunner.assertTransferCount(REL_FAILURE, 0);
    }

    @Test
    public void testInputFlowFileUuid() throws Exception {

        String uuid = "00029362-5106-40e8-b8a9-bf2cecfbc0d7";
        String inFileData = "{\"key\":\"value\"}";
        byte[] inFileDataBytes = inFileData.getBytes(StandardCharsets.UTF_8);

        Collection mcol = setupMockCouchbase(bucketName, scopeName, collectionName);
        when(mcol.upsert(anyString(), eq(inFileDataBytes), any(UpsertOptions.class)))
                .thenReturn(new MutationResult(new CoreMutationResult(null,
                        CoreKeyspace.from(new CollectionIdentifier(bucketName, Optional.of(scopeName), Optional.of(collectionName))),
                        uuid, 200L, Optional.empty()
                )));

        Map<String, String> properties = new HashMap<>();
        testRunner.setProperty(BUCKET_NAME, bucketName);
        testRunner.setProperty(SCOPE_NAME, scopeName);
        testRunner.setProperty(COLLECTION_NAME, collectionName);
        properties.put(CoreAttributes.UUID.key(), uuid);
        testRunner.enqueue(inFileDataBytes, properties);
        testRunner.run();

        verify(mcol, times(1)).upsert(anyString(), eq(inFileDataBytes), any(UpsertOptions.class));

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

        Collection mcol = setupMockCouchbase(bucketName, scopeName, collectionName);
        when(mcol.upsert(eq(docId), eq(inFileDataBytes), any(UpsertOptions.class)))
                .thenThrow(new ServiceNotAvailableException("mock", mock(ErrorContext.class)));


        testRunner.enqueue(inFileDataBytes);
        testRunner.setProperty(BUCKET_NAME, bucketName);
        testRunner.setProperty(SCOPE_NAME, scopeName);
        testRunner.setProperty(COLLECTION_NAME, collectionName);
        testRunner.setProperty(DOC_ID, docId);
        testRunner.setProperty(PutCouchbaseKey.REPLICATE_TO, ReplicateTo.ONE.toString());

        AssertionError e = assertThrows(AssertionError.class, () -> testRunner.run());
        assertEquals(ProcessException.class, e.getCause().getClass());

        verify(mcol, times(1)).upsert(eq(docId), eq(inFileDataBytes), any(UpsertOptions.class));

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

        CouchbaseException exception = new DurabilityImpossibleException(mock(KeyValueErrorContext.class));
        Collection mcol = setupMockCouchbase(bucketName, scopeName, collectionName);
        when(mcol.upsert(eq(docId), eq(inFileDataBytes), any(UpsertOptions.class)))
                .thenThrow(exception);

        testRunner.enqueue(inFileDataBytes);
        testRunner.setProperty(BUCKET_NAME, bucketName);
        testRunner.setProperty(SCOPE_NAME, scopeName);
        testRunner.setProperty(COLLECTION_NAME, collectionName);
        testRunner.setProperty(DOC_ID, docId);
        testRunner.setProperty(PutCouchbaseKey.REPLICATE_TO, ReplicateTo.ONE.toString());
        testRunner.run();

        verify(mcol, times(1)).upsert(eq(docId), eq(inFileDataBytes), any(UpsertOptions.class));

        testRunner.assertTransferCount(REL_SUCCESS, 0);
        testRunner.assertTransferCount(REL_RETRY, 1);
        testRunner.assertTransferCount(REL_FAILURE, 0);
        MockFlowFile orgFile = testRunner.getFlowFilesForRelationship(REL_RETRY).get(0);
        orgFile.assertContentEquals(inFileData);
        orgFile.assertAttributeEquals(CouchbaseAttributes.Exception.key(), exception.getClass().getName());
    }

    @Override
    protected Class<? extends Processor> processor() {
        return PutCouchbaseKey.class;
    }
}
