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

import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.services.couchbase.CouchbaseConnectionService;
import org.apache.nifi.services.couchbase.exception.CouchbaseErrorHandler;
import org.apache.nifi.services.couchbase.exception.CouchbaseException;
import org.apache.nifi.services.couchbase.utils.CouchbasePutResult;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Map.entry;
import static org.apache.nifi.processors.couchbase.AbstractCouchbaseProcessor.COUCHBASE_CONNECTION_SERVICE;
import static org.apache.nifi.processors.couchbase.AbstractCouchbaseProcessor.DOCUMENT_ID;
import static org.apache.nifi.processors.couchbase.AbstractCouchbaseProcessor.REL_FAILURE;
import static org.apache.nifi.processors.couchbase.AbstractCouchbaseProcessor.REL_RETRY;
import static org.apache.nifi.processors.couchbase.AbstractCouchbaseProcessor.REL_SUCCESS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestPutCouchbase {

    private static final String SERVICE_ID = "couchbaseClusterService";
    private static final String TEST_TRANSIT_URL = "test.transit.url";

    private TestRunner runner;

    @BeforeEach
    public void init() {
        runner = TestRunners.newTestRunner(PutCouchbase.class);
    }

    @Test
    public void testWithProvidedDocumentId() throws CouchbaseException, InitializationException {
        final String documentId = "test-document-id";
        final Map<String, String> attributes = Map.ofEntries(
                entry("attribute_1", "value_1"),
                entry("attribute_2", "value_2"),
                entry("attribute_3", "value_3")
        );
        final CouchbasePutResult result = new CouchbasePutResult(attributes, TEST_TRANSIT_URL);

        final CouchbaseConnectionService service = mock(CouchbaseConnectionService.class);
        when(service.getIdentifier()).thenReturn(SERVICE_ID);
        when(service.getErrorHandler()).thenReturn(new CouchbaseErrorHandler(Collections.emptyMap()));
        when(service.putDocument(anyString(), any())).thenReturn(result);

        runner.addControllerService(SERVICE_ID, service);
        runner.enableControllerService(service);
        runner.setProperty(DOCUMENT_ID, documentId);
        runner.setProperty(COUCHBASE_CONNECTION_SERVICE, SERVICE_ID);
        runner.setValidateExpressionUsage(false);
        runner.enqueue(new byte[0]);
        runner.run();

        verify(service, times(1)).putDocument(eq(documentId), any());

        runner.assertAllFlowFilesTransferred(REL_SUCCESS);
        runner.assertTransferCount(REL_SUCCESS, 1);
        runner.assertTransferCount(REL_FAILURE, 0);
        runner.assertTransferCount(REL_RETRY, 0);

        final MockFlowFile outFile = runner.getFlowFilesForRelationship(REL_SUCCESS).getFirst();
        outFile.assertAttributeEquals("attribute_1", "value_1");
        outFile.assertAttributeEquals("attribute_2", "value_2");
        outFile.assertAttributeEquals("attribute_3", "value_3");

        final List<ProvenanceEventRecord> provenanceEvents = runner.getProvenanceEvents();
        assertEquals(1, provenanceEvents.size());
        final ProvenanceEventRecord receiveEvent = provenanceEvents.getFirst();
        assertEquals(ProvenanceEventType.SEND, receiveEvent.getEventType());
        assertEquals(TEST_TRANSIT_URL, receiveEvent.getTransitUri());
    }

    @Test
    public void testWithFlowFileUUIDAsDocumentId() throws CouchbaseException, InitializationException {
        final MockFlowFile flowFile = new MockFlowFile(1L);
        final String docId = flowFile.getAttribute(CoreAttributes.UUID.key());
        final CouchbasePutResult result = new CouchbasePutResult(Collections.emptyMap(), TEST_TRANSIT_URL);

        final CouchbaseConnectionService service = mock(CouchbaseConnectionService.class);
        when(service.getIdentifier()).thenReturn(SERVICE_ID);
        when(service.getErrorHandler()).thenReturn(new CouchbaseErrorHandler(Collections.emptyMap()));
        when(service.putDocument(anyString(), any())).thenReturn(result);

        runner.addControllerService(SERVICE_ID, service);
        runner.enableControllerService(service);
        runner.setProperty(COUCHBASE_CONNECTION_SERVICE, SERVICE_ID);
        runner.enqueue(flowFile);
        runner.run();

        verify(service, times(1)).putDocument(eq(docId), any());

        runner.assertAllFlowFilesTransferred(REL_SUCCESS);
        runner.assertTransferCount(REL_SUCCESS, 1);
        runner.assertTransferCount(REL_FAILURE, 0);
        runner.assertTransferCount(REL_RETRY, 0);
    }

    @Test
    public void testWithFailure() throws CouchbaseException, InitializationException {
        final Map<Class<? extends Exception>, CouchbaseErrorHandler.ErrorHandlingStrategy> exceptionMapping =
                Collections.singletonMap(CouchbaseException.class, CouchbaseErrorHandler.ErrorHandlingStrategy.FAILURE);

        final CouchbaseConnectionService service = mock(CouchbaseConnectionService.class);
        when(service.getIdentifier()).thenReturn(SERVICE_ID);
        when(service.getErrorHandler()).thenReturn(new CouchbaseErrorHandler(exceptionMapping));
        when(service.putDocument(anyString(), any())).thenThrow(new CouchbaseException("Test exception"));

        runner.addControllerService(SERVICE_ID, service);
        runner.enableControllerService(service);
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
        final Map<Class<? extends Exception>, CouchbaseErrorHandler.ErrorHandlingStrategy> exceptionMapping =
                Collections.singletonMap(CouchbaseException.class, CouchbaseErrorHandler.ErrorHandlingStrategy.RETRY);

        final CouchbaseConnectionService service = mock(CouchbaseConnectionService.class);
        when(service.getIdentifier()).thenReturn(SERVICE_ID);
        when(service.getErrorHandler()).thenReturn(new CouchbaseErrorHandler(exceptionMapping));
        when(service.putDocument(anyString(), any())).thenThrow(new CouchbaseException("Test exception"));

        runner.addControllerService(SERVICE_ID, service);
        runner.enableControllerService(service);
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
        final Map<Class<? extends Exception>, CouchbaseErrorHandler.ErrorHandlingStrategy> exceptionMapping =
                Collections.singletonMap(CouchbaseException.class, CouchbaseErrorHandler.ErrorHandlingStrategy.ROLLBACK);

        final CouchbaseConnectionService service = mock(CouchbaseConnectionService.class);
        when(service.getIdentifier()).thenReturn(SERVICE_ID);
        when(service.getErrorHandler()).thenReturn(new CouchbaseErrorHandler(exceptionMapping));
        when(service.putDocument(anyString(), any())).thenThrow(new CouchbaseException("Test exception"));

        runner.addControllerService(SERVICE_ID, service);
        runner.enableControllerService(service);
        runner.setProperty(COUCHBASE_CONNECTION_SERVICE, SERVICE_ID);
        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertQueueNotEmpty();
        runner.assertTransferCount(REL_SUCCESS, 0);
        runner.assertTransferCount(REL_FAILURE, 0);
        runner.assertTransferCount(REL_RETRY, 0);
    }
}
