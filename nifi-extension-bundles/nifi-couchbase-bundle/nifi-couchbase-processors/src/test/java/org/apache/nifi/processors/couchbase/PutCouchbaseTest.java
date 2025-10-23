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

import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.services.couchbase.CouchbaseClient;
import org.apache.nifi.services.couchbase.CouchbaseConnectionService;
import org.apache.nifi.services.couchbase.exception.CouchbaseException;
import org.apache.nifi.services.couchbase.exception.ExceptionCategory;
import org.apache.nifi.services.couchbase.utils.CouchbaseUpsertResult;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.StringUtils;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.nifi.processors.couchbase.AbstractCouchbaseProcessor.COUCHBASE_CONNECTION_SERVICE;
import static org.apache.nifi.processors.couchbase.AbstractCouchbaseProcessor.DOCUMENT_ID;
import static org.apache.nifi.processors.couchbase.AbstractCouchbaseProcessor.REL_FAILURE;
import static org.apache.nifi.processors.couchbase.AbstractCouchbaseProcessor.REL_RETRY;
import static org.apache.nifi.processors.couchbase.AbstractCouchbaseProcessor.REL_SUCCESS;
import static org.apache.nifi.processors.couchbase.utils.CouchbaseAttributes.BUCKET_ATTRIBUTE;
import static org.apache.nifi.processors.couchbase.utils.CouchbaseAttributes.CAS_ATTRIBUTE;
import static org.apache.nifi.processors.couchbase.utils.CouchbaseAttributes.COLLECTION_ATTRIBUTE;
import static org.apache.nifi.processors.couchbase.utils.CouchbaseAttributes.DEFAULT_BUCKET;
import static org.apache.nifi.processors.couchbase.utils.CouchbaseAttributes.DEFAULT_COLLECTION;
import static org.apache.nifi.processors.couchbase.utils.CouchbaseAttributes.DEFAULT_SCOPE;
import static org.apache.nifi.processors.couchbase.utils.CouchbaseAttributes.SCOPE_ATTRIBUTE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PutCouchbaseTest {

    private static final String SERVICE_ID = "couchbaseConnectionService";
    private static final String TEST_DOCUMENT_ID = "test-document-id";
    private static final String TEST_SERVICE_LOCATION = "couchbase://test-location";
    private static final long TEST_CAS = 1L;

    private TestRunner runner;

    @BeforeEach
    public void init() {
        runner = TestRunners.newTestRunner(PutCouchbase.class);
    }

    @Test
    public void testWithDocumentIdAsFlowFileAttribute() throws CouchbaseException, InitializationException {
        final CouchbaseClient client = mock(CouchbaseClient.class);
        when(client.upsertDocument(anyString(), any())).thenReturn(new CouchbaseUpsertResult(TEST_CAS));

        final CouchbaseConnectionService service = mock(CouchbaseConnectionService.class);
        when(service.getIdentifier()).thenReturn(SERVICE_ID);
        when(service.getClient(any())).thenReturn(client);
        when(service.getServiceLocation()).thenReturn(TEST_SERVICE_LOCATION);

        final MockFlowFile flowFile = new MockFlowFile(0);
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("flowfile_document_id", TEST_DOCUMENT_ID);
        flowFile.putAttributes(attributes);

        runner.addControllerService(SERVICE_ID, service);
        runner.enableControllerService(service);
        runner.setProperty(DOCUMENT_ID, "${flowfile_document_id}");
        runner.setProperty(COUCHBASE_CONNECTION_SERVICE, SERVICE_ID);
        runner.setValidateExpressionUsage(false);
        runner.enqueue(flowFile);
        runner.run();

        verify(client, times(1)).upsertDocument(eq(TEST_DOCUMENT_ID), any());

        runner.assertAllFlowFilesTransferred(REL_SUCCESS);
        runner.assertTransferCount(REL_SUCCESS, 1);
        runner.assertTransferCount(REL_FAILURE, 0);
        runner.assertTransferCount(REL_RETRY, 0);

        final MockFlowFile outFile = runner.getFlowFilesForRelationship(REL_SUCCESS).getFirst();
        outFile.assertAttributeEquals(BUCKET_ATTRIBUTE, DEFAULT_BUCKET);
        outFile.assertAttributeEquals(SCOPE_ATTRIBUTE, DEFAULT_SCOPE);
        outFile.assertAttributeEquals(COLLECTION_ATTRIBUTE, DEFAULT_COLLECTION);
        outFile.assertAttributeEquals(CAS_ATTRIBUTE, String.valueOf(TEST_CAS));

        final List<ProvenanceEventRecord> provenanceEvents = runner.getProvenanceEvents();
        assertEquals(1, provenanceEvents.size());
        final ProvenanceEventRecord receiveEvent = provenanceEvents.getFirst();
        assertEquals(ProvenanceEventType.SEND, receiveEvent.getEventType());
        assertEquals(StringUtils.join(Arrays.asList(TEST_SERVICE_LOCATION, DEFAULT_BUCKET, DEFAULT_SCOPE, DEFAULT_COLLECTION, TEST_DOCUMENT_ID), "/"), receiveEvent.getTransitUri());
    }

    @Test
    public void testWithFailure() throws CouchbaseException, InitializationException {
        final CouchbaseClient client = mock(CouchbaseClient.class);
        when(client.getExceptionCategory(any())).thenReturn(ExceptionCategory.FAILURE);
        when(client.upsertDocument(anyString(), any())).thenThrow(new CouchbaseException("", new TestCouchbaseException("Test exception")));

        final CouchbaseConnectionService service = mock(CouchbaseConnectionService.class);
        when(service.getIdentifier()).thenReturn(SERVICE_ID);
        when(service.getClient(any())).thenReturn(client);

        runner.addControllerService(SERVICE_ID, service);
        runner.enableControllerService(service);
        runner.setProperty(DOCUMENT_ID, TEST_DOCUMENT_ID);
        runner.setProperty(COUCHBASE_CONNECTION_SERVICE, SERVICE_ID);
        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertAllFlowFilesTransferred(REL_FAILURE);
        runner.assertTransferCount(REL_SUCCESS, 0);
        runner.assertTransferCount(REL_FAILURE, 1);
        runner.assertTransferCount(REL_RETRY, 0);
    }

    @Test
    public void testWithRetry() throws CouchbaseException, InitializationException {
        final CouchbaseClient client = mock(CouchbaseClient.class);
        when(client.getExceptionCategory(any())).thenReturn(ExceptionCategory.RETRY);
        when(client.upsertDocument(anyString(), any())).thenThrow(new CouchbaseException("", new TestCouchbaseException("Test exception")));

        final CouchbaseConnectionService service = mock(CouchbaseConnectionService.class);
        when(service.getIdentifier()).thenReturn(SERVICE_ID);
        when(service.getClient(any())).thenReturn(client);

        runner.addControllerService(SERVICE_ID, service);
        runner.enableControllerService(service);
        runner.setProperty(DOCUMENT_ID, TEST_DOCUMENT_ID);
        runner.setProperty(COUCHBASE_CONNECTION_SERVICE, SERVICE_ID);
        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertAllFlowFilesTransferred(REL_RETRY);
        runner.assertTransferCount(REL_SUCCESS, 0);
        runner.assertTransferCount(REL_FAILURE, 0);
        runner.assertTransferCount(REL_RETRY, 1);
    }

    @Test
    public void testWithRollback() throws CouchbaseException, InitializationException {
        final CouchbaseClient client = mock(CouchbaseClient.class);
        when(client.getExceptionCategory(any())).thenReturn(ExceptionCategory.ROLLBACK);
        when(client.upsertDocument(anyString(), any())).thenThrow(new CouchbaseException("", new TestCouchbaseException("Test exception")));

        final CouchbaseConnectionService service = mock(CouchbaseConnectionService.class);
        when(service.getIdentifier()).thenReturn(SERVICE_ID);
        when(service.getClient(any())).thenReturn(client);

        runner.addControllerService(SERVICE_ID, service);
        runner.enableControllerService(service);
        runner.setProperty(DOCUMENT_ID, TEST_DOCUMENT_ID);
        runner.setProperty(COUCHBASE_CONNECTION_SERVICE, SERVICE_ID);
        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertQueueNotEmpty();
        runner.assertTransferCount(REL_SUCCESS, 0);
        runner.assertTransferCount(REL_FAILURE, 0);
        runner.assertTransferCount(REL_RETRY, 0);
    }

    @Test
    public void testWithUnknownException() throws CouchbaseException, InitializationException {
        final CouchbaseClient client = mock(CouchbaseClient.class);
        when(client.getExceptionCategory(any())).thenReturn(ExceptionCategory.FAILURE);
        when(client.upsertDocument(anyString(), any())).thenThrow(new CouchbaseException("", new TestCouchbaseException("Test exception")));

        final CouchbaseConnectionService service = mock(CouchbaseConnectionService.class);
        when(service.getIdentifier()).thenReturn(SERVICE_ID);
        when(service.getClient(any())).thenReturn(client);

        runner.addControllerService(SERVICE_ID, service);
        runner.enableControllerService(service);
        runner.setProperty(DOCUMENT_ID, TEST_DOCUMENT_ID);
        runner.setProperty(COUCHBASE_CONNECTION_SERVICE, SERVICE_ID);
        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertAllFlowFilesTransferred(REL_FAILURE);
        runner.assertTransferCount(REL_SUCCESS, 0);
        runner.assertTransferCount(REL_FAILURE, 1);
        runner.assertTransferCount(REL_RETRY, 0);
    }
}
