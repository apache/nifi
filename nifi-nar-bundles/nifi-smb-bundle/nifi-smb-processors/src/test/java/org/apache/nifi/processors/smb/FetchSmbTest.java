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
package org.apache.nifi.processors.smb;

import static org.apache.nifi.processors.smb.FetchSmb.ERROR_CODE_ATTRIBUTE;
import static org.apache.nifi.processors.smb.FetchSmb.ERROR_MESSAGE_ATTRIBUTE;
import static org.apache.nifi.processors.smb.FetchSmb.RECORD_READER;
import static org.apache.nifi.processors.smb.FetchSmb.REL_FAILURE;
import static org.apache.nifi.processors.smb.FetchSmb.REL_INPUT_FAILURE;
import static org.apache.nifi.processors.smb.FetchSmb.REL_SUCCESS;
import static org.apache.nifi.processors.smb.ListSmb.SMB_CLIENT_PROVIDER_SERVICE;
import static org.apache.nifi.util.TestRunners.newTestRunner;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.services.smb.SmbClientProviderService;
import org.apache.nifi.services.smb.SmbClientService;
import org.apache.nifi.services.smb.SmbException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class FetchSmbTest {

    public static final String CLIENT_SERVICE_PROVIDER_ID = "client-provider-service-id";
    public static final String RECORD_READER_SERVICE_ID = "record-reader-id";

    @Mock
    SmbClientService mockNifiSmbClientService;

    @Mock
    SmbClientProviderService clientProviderService;

    @Mock
    RecordReaderFactory recordReaderFactory;

    private static Stream<Arguments> schemaExceptions() {
        final String msg = "test exception";
        return Stream.of(
                new IOException(msg),
                new MalformedRecordException(msg),
                new SchemaNotFoundException(msg),
                new RuntimeException(msg)
        ).map(Arguments::of);
    }

    @BeforeEach
    public void beforeEach() throws Exception {
        MockitoAnnotations.initMocks(this);
        when(clientProviderService.getClient()).thenReturn(mockNifiSmbClientService);
        when(clientProviderService.getIdentifier()).thenReturn(CLIENT_SERVICE_PROVIDER_ID);
        when(clientProviderService.getServiceLocation()).thenReturn(URI.create("smb://localhost:445/share"));
        when(recordReaderFactory.getIdentifier()).thenReturn(RECORD_READER_SERVICE_ID);
    }

    @ParameterizedTest
    @MethodSource("schemaExceptions")
    public void shouldAddErrorMessageToAttributeWhenRecordReaderThrowsException(Exception exception) throws Exception {
        final TestRunner testRunner = createRunner();

        final MockFlowFile flowFile = new MockFlowFile(100L);

        when(recordReaderFactory.createRecordReader(eq(flowFile.getAttributes()), any(InputStream.class), anyLong(),
                eq(testRunner.getLogger())))
                .thenThrow(exception);
        testRunner.setProperty(RECORD_READER, RECORD_READER_SERVICE_ID);
        testRunner.addControllerService(RECORD_READER_SERVICE_ID, recordReaderFactory);
        testRunner.enableControllerService(recordReaderFactory);
        testRunner.enqueue(flowFile);
        testRunner.run();
        testRunner.assertTransferCount(REL_SUCCESS, 0);
        testRunner.assertTransferCount(REL_INPUT_FAILURE, 1);
        assertEquals("test exception",
                testRunner.getFlowFilesForRelationship(REL_INPUT_FAILURE).get(0).getAttribute(ERROR_MESSAGE_ATTRIBUTE));
    }

    @Test
    public void shouldAddErrorMessageToAttributeWhenSmbClientServiceThrowsException() throws Exception {
        final TestRunner testRunner = createRunner();

        final MockFlowFile flowFile = new MockFlowFile(100L);

        RecordReader reader = mock(RecordReader.class);
        testRunner.setProperty(RECORD_READER, RECORD_READER_SERVICE_ID);
        testRunner.addControllerService(RECORD_READER_SERVICE_ID, recordReaderFactory);
        testRunner.enableControllerService(recordReaderFactory);
        when(recordReaderFactory.createRecordReader(eq(flowFile.getAttributes()), any(InputStream.class), anyLong(),
                eq(testRunner.getLogger())))
                .thenReturn(reader);

        final Record firstRecord = mock(Record.class);
        final Record secondRecord = mock(Record.class);

        when(reader.nextRecord()).thenReturn(firstRecord).thenReturn(secondRecord).thenReturn(null);
        when(firstRecord.getAsString(eq("identifier"))).thenReturn("testDirectory/cannotReadThis");
        when(secondRecord.getAsString(eq("identifier"))).thenReturn("testDirectory/canReadThis");
        mockNifiSmbClientService();

        testRunner.enqueue(flowFile);
        testRunner.run();
        testRunner.assertTransferCount(REL_SUCCESS, 1);
        testRunner.assertTransferCount(REL_FAILURE, 1);
        assertEquals("content", testRunner.getFlowFilesForRelationship(REL_SUCCESS).get(0).getContent());
        assertEquals("test exception",
                testRunner.getFlowFilesForRelationship(REL_FAILURE).get(0).getAttribute(ERROR_MESSAGE_ATTRIBUTE));
        assertEquals("1",
                testRunner.getFlowFilesForRelationship(REL_FAILURE).get(0).getAttribute(ERROR_CODE_ATTRIBUTE));

    }

    @Test
    public void shouldUseSmbClientProperlyWhenNoRecordReaderConfigured() throws Exception {
        final TestRunner testRunner = createRunner();
        mockNifiSmbClientService();
        Map<String, String> attributes = new HashMap<>();
        attributes.put("identifier", "testDirectory/cannotReadThis");
        testRunner.enqueue("ignore", attributes);
        attributes = new HashMap<>();
        attributes.put("identifier", "testDirectory/canReadThis");
        testRunner.enqueue("ignore", attributes);
        testRunner.run();
        testRunner.assertTransferCount(REL_FAILURE, 1);
        assertEquals("test exception",
                testRunner.getFlowFilesForRelationship(REL_FAILURE).get(0).getAttribute(ERROR_MESSAGE_ATTRIBUTE));
        assertEquals("1",
                testRunner.getFlowFilesForRelationship(REL_FAILURE).get(0).getAttribute(ERROR_CODE_ATTRIBUTE));
        testRunner.run();
        testRunner.assertTransferCount(REL_SUCCESS, 1);
    }

    private void mockNifiSmbClientService() throws IOException {
        doThrow(new SmbException("test exception", 1L, new RuntimeException())).when(mockNifiSmbClientService)
                .read(eq("testDirectory/cannotReadThis"), any(OutputStream.class));
        doAnswer(invocation -> {
            final OutputStream o = invocation.getArgument(1);
            final ByteArrayInputStream bytes = new ByteArrayInputStream("content".getBytes());
            IOUtils.copy(bytes, o);
            return true;
        }).when(mockNifiSmbClientService)
                .read(eq("testDirectory/canReadThis"), any(OutputStream.class));
    }

    private TestRunner createRunner() throws Exception {
        final TestRunner testRunner = newTestRunner(FetchSmb.class);
        testRunner.setProperty(SMB_CLIENT_PROVIDER_SERVICE, CLIENT_SERVICE_PROVIDER_ID);
        testRunner.addControllerService(CLIENT_SERVICE_PROVIDER_ID, clientProviderService);
        testRunner.enableControllerService(clientProviderService);
        return testRunner;
    }

}
