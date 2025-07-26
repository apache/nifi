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

import com.couchbase.client.core.error.AuthenticationFailureException;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.error.DurabilityImpossibleException;
import com.couchbase.client.core.error.InvalidRequestException;
import com.couchbase.client.core.error.ServiceNotAvailableException;
import com.couchbase.client.core.error.TemporaryFailureException;
import com.couchbase.client.core.error.UnambiguousTimeoutException;
import com.couchbase.client.core.error.context.CancellationErrorContext;
import com.couchbase.client.core.error.context.ErrorContext;
import com.couchbase.client.core.error.context.KeyValueErrorContext;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.kv.GetOptions;
import com.couchbase.client.java.kv.GetResult;
import org.apache.nifi.attribute.expression.language.exception.AttributeExpressionLanguageException;
import org.apache.nifi.couchbase.DocumentType;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.util.MockFlowFile;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.nifi.couchbase.CouchbaseConfigurationProperties.BUCKET_NAME;
import static org.apache.nifi.couchbase.CouchbaseConfigurationProperties.COLLECTION_NAME;
import static org.apache.nifi.couchbase.CouchbaseConfigurationProperties.DOCUMENT_TYPE;
import static org.apache.nifi.couchbase.CouchbaseConfigurationProperties.SCOPE_NAME;
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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class TestGetCouchbaseKey extends org.apache.nifi.couchbase.AbstractCouchbaseNifiTest {

    @Override
    protected Class<? extends Processor> processor() {
        return GetCouchbaseKey.class;
    }

    @Test
    public void testStaticDocId() throws Exception {
        String docId = "doc-a";

        Collection mcol = setupMockCouchbase(bucketName, scopeName, collectionName);
        String content = "{\"key\":\"value\"}";
        Instant expiry = Instant.now().plusSeconds(10);
        long cas = 200L;
        when(mcol.get(eq(docId), any(GetOptions.class)))
                .thenReturn(new GetResult(content.getBytes(StandardCharsets.UTF_8), 0, cas, Optional.of(expiry), null));

        testRunner.setProperty(BUCKET_NAME, bucketName);
        testRunner.setProperty(SCOPE_NAME, scopeName);
        testRunner.setProperty(COLLECTION_NAME, collectionName);
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
        outFile.assertAttributeEquals(CouchbaseAttributes.Expiry.key(), String.valueOf(expiry));
    }


    @Test
    public void testDocIdExp() throws Exception {
        String docIdExp = "${'someProperty'}";
        String somePropertyValue = "doc-p";

        String content = "{\"key\":\"value\"}";
        Collection mcol = setupMockCouchbase(bucketName, scopeName, collectionName);
        when(mcol.get(eq(somePropertyValue), any(GetOptions.class)))
                .thenReturn(new GetResult(content.getBytes(StandardCharsets.UTF_8), 0, 200L, Optional.empty(), null));

        testRunner.setProperty(BUCKET_NAME, bucketName);
        testRunner.setProperty(SCOPE_NAME, scopeName);
        testRunner.setProperty(COLLECTION_NAME, collectionName);
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

        String content = "{\"key\":\"value\"}";
        Collection mcol = setupMockCouchbase(bucketName, scopeName, collectionName);
        when(mcol.get(eq(docId), any(GetOptions.class)))
                .thenReturn(new GetResult(content.getBytes(StandardCharsets.UTF_8), 0, 200L, Optional.empty(), null));

        testRunner.setProperty(DOC_ID, docIdExp);
        testRunner.setProperty(BUCKET_NAME, bucketName);
        testRunner.setProperty(SCOPE_NAME, scopeName);
        testRunner.setProperty(COLLECTION_NAME, collectionName);

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

        String content = "{\"key\":\"value\"}";
        Collection mcol = setupMockCouchbase(bucketName, scopeName, collectionName);
        when(mcol.get(eq(docId), any(GetOptions.class)))
                .thenReturn(new GetResult(content.getBytes(StandardCharsets.UTF_8), 0, 200L, Optional.empty(), null));

        testRunner.setProperty(DOC_ID, docIdExp);
        testRunner.setProperty(BUCKET_NAME, bucketName);
        testRunner.setProperty(SCOPE_NAME, scopeName);
        testRunner.setProperty(COLLECTION_NAME, collectionName);
        testRunner.enqueue(new byte[0]);

        AssertionError e = assertThrows(AssertionError.class, () -> testRunner.run());
        assertEquals(AttributeExpressionLanguageException.class, e.getCause().getClass());

        testRunner.assertTransferCount(REL_SUCCESS, 0);
        testRunner.assertTransferCount(REL_ORIGINAL, 0);
        testRunner.assertTransferCount(REL_RETRY, 0);
        testRunner.assertTransferCount(REL_FAILURE, 0);
    }

    @Test
    public void testDocIdExpWithInvalidExpressionOnFlowFile() throws Exception {
        String docIdExp = "${nonExistingFunction(someProperty)}";

        setupMockCouchbase(bucketName, scopeName, collectionName);

        testRunner.setProperty(BUCKET_NAME, bucketName);
        testRunner.setProperty(SCOPE_NAME, scopeName);
        testRunner.setProperty(COLLECTION_NAME, collectionName);
        testRunner.setProperty(DOC_ID, docIdExp);

        String inputFileDataStr = "input FlowFile data";
        byte[] inFileData = inputFileDataStr.getBytes(StandardCharsets.UTF_8);
        Map<String, String> properties = new HashMap<>();
        properties.put("someProperty", "someValue");
        testRunner.enqueue(inFileData, properties);

        AssertionError e = assertThrows(AssertionError.class, () -> testRunner.run());
        assertEquals(AttributeExpressionLanguageException.class, e.getCause().getClass());

        testRunner.assertTransferCount(REL_SUCCESS, 0);
        testRunner.assertTransferCount(REL_ORIGINAL, 0);
        testRunner.assertTransferCount(REL_RETRY, 0);
        testRunner.assertTransferCount(REL_FAILURE, 0);
    }

    @Test
    public void testInputFlowFileContent() throws Exception {

        String inFileDataStr = "doc-in";
        String content = "{\"key\":\"value\"}";
        Collection mcol = setupMockCouchbase(bucketName, scopeName, collectionName);
        when(mcol.get(eq(inFileDataStr), any(GetOptions.class)))
            .thenReturn(new GetResult(content.getBytes(StandardCharsets.UTF_8), 0, 200L, Optional.empty(), null));


        byte[] inFileData = inFileDataStr.getBytes(StandardCharsets.UTF_8);
        testRunner.setProperty(BUCKET_NAME, bucketName);
        testRunner.setProperty(SCOPE_NAME, scopeName);
        testRunner.setProperty(COLLECTION_NAME, collectionName);
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

        String inFileDataStr = "doc-in";
        String content = "some-value";
        Collection mcol = setupMockCouchbase(bucketName, scopeName, collectionName);
        when(mcol.get(eq(inFileDataStr), any(GetOptions.class)))
                .thenReturn(new GetResult(content.getBytes(StandardCharsets.UTF_8), 0, 200L, Optional.empty(), null));

        byte[] inFileData = inFileDataStr.getBytes(StandardCharsets.UTF_8);
        testRunner.setProperty(BUCKET_NAME, bucketName);
        testRunner.setProperty(SCOPE_NAME, scopeName);
        testRunner.setProperty(COLLECTION_NAME, collectionName);
        testRunner.setProperty(PUT_VALUE_TO_ATTRIBUTE, "targetAttribute");
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

        String inFileDataStr = "doc-in";
        String content = "some-value";
        Collection mcol = setupMockCouchbase(bucketName, scopeName, collectionName);
        when(mcol.get(eq(inFileDataStr), any(GetOptions.class)))
                .thenReturn(new GetResult(content.getBytes(StandardCharsets.UTF_8), 0, 200L, Optional.empty(), null));

        byte[] inFileData = inFileDataStr.getBytes(StandardCharsets.UTF_8);
        testRunner.setProperty(BUCKET_NAME, bucketName);
        testRunner.setProperty(SCOPE_NAME, scopeName);
        testRunner.setProperty(COLLECTION_NAME, collectionName);
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

        String inFileDataStr = "doc-in";
        String content = "binary";
        Collection mcol = setupMockCouchbase(bucketName, scopeName, collectionName);
        when(mcol.get(eq(inFileDataStr), any(GetOptions.class)))
                .thenReturn(new GetResult(content.getBytes(StandardCharsets.UTF_8), 0, 200L, Optional.empty(), null));


        byte[] inFileData = inFileDataStr.getBytes(StandardCharsets.UTF_8);
        testRunner.enqueue(inFileData);
        testRunner.setProperty(BUCKET_NAME, bucketName);
        testRunner.setProperty(SCOPE_NAME, scopeName);
        testRunner.setProperty(COLLECTION_NAME, collectionName);
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

        String inFileDataStr = "doc-in";
        String content = "binary";
        Collection mcol = setupMockCouchbase(bucketName, scopeName, collectionName);
        when(mcol.get(eq(inFileDataStr), any(GetOptions.class)))
                .thenReturn(new GetResult(content.getBytes(StandardCharsets.UTF_8), 0, 200L, Optional.empty(), null));

        byte[] inFileData = inFileDataStr.getBytes(StandardCharsets.UTF_8);
        testRunner.enqueue(inFileData);
        testRunner.setProperty(BUCKET_NAME, bucketName);
        testRunner.setProperty(SCOPE_NAME, scopeName);
        testRunner.setProperty(COLLECTION_NAME, collectionName);
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

        String inFileDataStr = "doc-in";
        Collection mcol = setupMockCouchbase(bucketName, scopeName, collectionName);
        when(mcol.get(eq(inFileDataStr), any(GetOptions.class)))
                .thenThrow(new ServiceNotAvailableException("Mock exception", mock(ErrorContext.class)));

        byte[] inFileData = inFileDataStr.getBytes(StandardCharsets.UTF_8);
        testRunner.setProperty(BUCKET_NAME, bucketName);
        testRunner.setProperty(SCOPE_NAME, scopeName);
        testRunner.setProperty(COLLECTION_NAME, collectionName);
        testRunner.enqueue(inFileData);

        AssertionError e = assertThrows(AssertionError.class, () -> testRunner.run());
        assertEquals(ProcessException.class, e.getCause().getClass());

        testRunner.assertTransferCount(REL_SUCCESS, 0);
        testRunner.assertTransferCount(REL_ORIGINAL, 0);
        testRunner.assertTransferCount(REL_RETRY, 0);
        testRunner.assertTransferCount(REL_FAILURE, 0);
    }

    @Test
    public void testCouchbaseConfigurationError() throws Exception {
        String docIdExp = "doc-c";

        Collection mcol = setupMockCouchbase(bucketName, scopeName, collectionName);
        when(mcol.get(eq(docIdExp), any(GetOptions.class)))
                .thenThrow(new AuthenticationFailureException("Test Exception", mock(ErrorContext.class), new ProcessException()));
//        when(bucket.get(docIdExp, RawJsonDocument.class))
//            .thenThrow(new AuthenticationException());
//        setupMockCouchbase(bucket);

        testRunner.setProperty(BUCKET_NAME, bucketName);
        testRunner.setProperty(SCOPE_NAME, scopeName);
        testRunner.setProperty(COLLECTION_NAME, collectionName);
        testRunner.setProperty(DOC_ID, docIdExp);

        String inputFileDataStr = "input FlowFile data";
        byte[] inFileData = inputFileDataStr.getBytes(StandardCharsets.UTF_8);
        testRunner.enqueue(inFileData);

        AssertionError e = assertThrows(AssertionError.class, () -> testRunner.run());
        assertEquals(ProcessException.class, e.getCause().getClass());
        /* assertTrue(e.getCause().getCause().getClass().equals(AuthenticationException.class)); */

        testRunner.assertTransferCount(REL_SUCCESS, 0);
        testRunner.assertTransferCount(REL_ORIGINAL, 0);
        testRunner.assertTransferCount(REL_RETRY, 0);
        testRunner.assertTransferCount(REL_FAILURE, 0);
    }

    @Test
    public void testCouchbaseInvalidInputError() throws Exception {
        String docIdExp = "doc-c";

        Collection mcol = setupMockCouchbase(bucketName, scopeName, collectionName);
        Exception exception = new InvalidRequestException(mock(ErrorContext.class));
        when(mcol.get(eq(docIdExp), any(GetOptions.class)))
                .thenThrow(exception);

        testRunner.setProperty(BUCKET_NAME, bucketName);
        testRunner.setProperty(SCOPE_NAME, scopeName);
        testRunner.setProperty(COLLECTION_NAME, collectionName);
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

        Collection mcol = setupMockCouchbase(bucketName, scopeName, collectionName);
        CouchbaseException exception = new TemporaryFailureException(mock(ErrorContext.class));
        when(mcol.get(eq(docIdExp), any(GetOptions.class)))
                .thenThrow(exception);

        testRunner.setProperty(BUCKET_NAME, bucketName);
        testRunner.setProperty(SCOPE_NAME, scopeName);
        testRunner.setProperty(COLLECTION_NAME, collectionName);
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

        Collection mcol = setupMockCouchbase(bucketName, scopeName, collectionName);
        // There is no suitable CouchbaseException for temp flowfile error, currently.
        Exception exception = new DurabilityImpossibleException(mock(KeyValueErrorContext.class));
        when(mcol.get(eq(docIdExp), any(GetOptions.class)))
                .thenThrow(exception);

        testRunner.setProperty(DOC_ID, docIdExp);
        testRunner.setProperty(BUCKET_NAME, bucketName);
        testRunner.setProperty(SCOPE_NAME, scopeName);
        testRunner.setProperty(COLLECTION_NAME, collectionName);

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
    public void testCouchbaseFatalError() throws Exception {
        String docIdExp = "doc-c";

        Collection mcol = setupMockCouchbase(bucketName, scopeName, collectionName);
        CouchbaseException exception = new UnambiguousTimeoutException("mock", mock(CancellationErrorContext.class));
        when(mcol.get(eq(docIdExp), any(GetOptions.class)))
                .thenThrow(exception);

        testRunner.setProperty(BUCKET_NAME, bucketName);
        testRunner.setProperty(SCOPE_NAME, scopeName);
        testRunner.setProperty(COLLECTION_NAME, collectionName);
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
    public void testDocumentNotFound() throws Exception {
        String docIdExp = "doc-n";

        Collection mcol = setupMockCouchbase(bucketName, scopeName, collectionName);
        when(mcol.get(eq(docIdExp), any(GetOptions.class)))
                .thenThrow(new DocumentNotFoundException(mock(ErrorContext.class)));

        testRunner.setProperty(BUCKET_NAME, bucketName);
        testRunner.setProperty(SCOPE_NAME, scopeName);
        testRunner.setProperty(COLLECTION_NAME, collectionName);
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
}
