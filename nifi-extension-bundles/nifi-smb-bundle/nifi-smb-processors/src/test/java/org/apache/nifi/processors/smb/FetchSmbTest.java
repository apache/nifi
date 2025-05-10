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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.services.smb.SmbClientProviderService;
import org.apache.nifi.services.smb.SmbClientService;
import org.apache.nifi.services.smb.SmbException;
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.apache.nifi.processors.smb.FetchSmb.ERROR_CODE_ATTRIBUTE;
import static org.apache.nifi.processors.smb.FetchSmb.ERROR_MESSAGE_ATTRIBUTE;
import static org.apache.nifi.processors.smb.FetchSmb.REL_FAILURE;
import static org.apache.nifi.processors.smb.FetchSmb.REL_SUCCESS;
import static org.apache.nifi.processors.smb.ListSmb.SMB_CLIENT_PROVIDER_SERVICE;
import static org.apache.nifi.util.TestRunners.newTestRunner;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

class FetchSmbTest {

    public static final String CLIENT_SERVICE_PROVIDER_ID = "client-provider-service-id";

    @Mock
    SmbClientService mockNifiSmbClientService;

    @Mock
    SmbClientProviderService clientProviderService;

    private AutoCloseable mockCloseable;

    @BeforeEach
    public void beforeEach() throws Exception {
        mockCloseable = MockitoAnnotations.openMocks(this);
        when(clientProviderService.getClient(any(ComponentLog.class))).thenReturn(mockNifiSmbClientService);
        when(clientProviderService.getIdentifier()).thenReturn(CLIENT_SERVICE_PROVIDER_ID);
        when(clientProviderService.getServiceLocation()).thenReturn(URI.create("smb://localhost:445/share"));
    }

    @AfterEach
    public void cleanup() throws Exception {
        if (mockCloseable != null) {
            mockCloseable.close();
        }
    }


    @Test
    public void shouldUseSmbClientProperly() throws Exception {
        final TestRunner testRunner = createRunner();
        mockNifiSmbClientService();
        Map<String, String> attributes = new HashMap<>();
        attributes.put("path", "testDirectory");
        attributes.put("filename", "cannotReadThis");
        testRunner.enqueue("ignore", attributes);
        attributes = new HashMap<>();
        attributes.put("path", "testDirectory");
        attributes.put("filename", "canReadThis");
        testRunner.enqueue("ignore", attributes);
        testRunner.run();
        testRunner.assertTransferCount(REL_FAILURE, 1);
        assertEquals("test exception",
                testRunner.getFlowFilesForRelationship(REL_FAILURE).get(0).getAttribute(ERROR_MESSAGE_ATTRIBUTE));
        assertEquals("1",
                testRunner.getFlowFilesForRelationship(REL_FAILURE).get(0).getAttribute(ERROR_CODE_ATTRIBUTE));
        testRunner.run();
        testRunner.assertTransferCount(REL_SUCCESS, 1);
        assertEquals("content",
                testRunner.getFlowFilesForRelationship(REL_SUCCESS).get(0).getContent());
        testRunner.assertValid();
    }

    @Test
    public void noSuchAttributeReferencedInELShouldResultInFailure() throws Exception {
        final TestRunner testRunner = createRunner();
        mockNifiSmbClientService();
        Map<String, String> attributes = new HashMap<>();
        attributes.put("different_field_name_than_what_EL_expect", "testDirectory/cannotFindThis");
        testRunner.enqueue("ignore", attributes);
        testRunner.run();
        testRunner.assertTransferCount(REL_SUCCESS, 0);
        testRunner.assertTransferCount(REL_FAILURE, 1);
        testRunner.assertValid();
    }

    private void mockNifiSmbClientService() throws IOException {
        doThrow(new SmbException("test exception", 1L, new RuntimeException())).when(mockNifiSmbClientService)
                .readFile(anyString(), any(OutputStream.class));
        doAnswer(invocation -> {
            final OutputStream o = invocation.getArgument(1);
            final ByteArrayInputStream bytes = new ByteArrayInputStream("content".getBytes());
            IOUtils.copy(bytes, o);
            return true;
        }).when(mockNifiSmbClientService)
                .readFile(eq("testDirectory/canReadThis"), any(OutputStream.class));
    }

    private TestRunner createRunner() throws Exception {
        final TestRunner testRunner = newTestRunner(FetchSmb.class);
        testRunner.setProperty(SMB_CLIENT_PROVIDER_SERVICE, CLIENT_SERVICE_PROVIDER_ID);
        testRunner.addControllerService(CLIENT_SERVICE_PROVIDER_ID, clientProviderService);
        testRunner.enableControllerService(clientProviderService);
        return testRunner;
    }

}
