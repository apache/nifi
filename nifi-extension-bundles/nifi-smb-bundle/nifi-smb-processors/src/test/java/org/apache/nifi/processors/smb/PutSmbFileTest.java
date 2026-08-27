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

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.services.smb.SmbClientProviderService;
import org.apache.nifi.services.smb.SmbClientService;
import org.apache.nifi.services.smb.SmbShareAccess;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.nifi.processors.smb.util.LocalSmbProperties.CONNECTION_CONFIGURATION_STRATEGY;
import static org.apache.nifi.processors.smb.util.LocalSmbProperties.ConnectionConfigurationStrategy;
import static org.apache.nifi.processors.smb.util.LocalSmbProperties.SMB_CLIENT_PROVIDER_SERVICE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PutSmbFileTest {

    private TestRunner testRunner;

    @Mock
    private SmbClientProviderService clientProviderService;

    @Mock
    private SmbClientService clientService;

    @Captor
    private ArgumentCaptor<Set<SmbShareAccess>> shareAccessCaptor;

    private static final String HOSTNAME = "smbhostname";
    private static final String SHARE = "smbshare";
    private static final String DIRECTORY = "smbdirectory";

    private static final String CLIENT_SERVICE_ID = "client-provider-service-id";

    private static final AtomicInteger FLOWFILE_ID_COUNTER = new AtomicInteger(0);

    private AutoCloseable mockCloseable;

    @BeforeEach
    public void init() throws Exception {
        mockCloseable = MockitoAnnotations.openMocks(this);

        when(clientProviderService.getIdentifier()).thenReturn(CLIENT_SERVICE_ID);
        when(clientProviderService.getServiceLocation(anyMap())).thenReturn(URI.create("smb://" + HOSTNAME + ":445/" + SHARE));
        when(clientProviderService.getClient(any(ComponentLog.class), anyMap())).thenReturn(clientService);

        testRunner = TestRunners.newTestRunner(PutSmbFile.class);
        testRunner.addControllerService(CLIENT_SERVICE_ID, clientProviderService);
        testRunner.enableControllerService(clientProviderService);
        testRunner.setProperty(CONNECTION_CONFIGURATION_STRATEGY, ConnectionConfigurationStrategy.CONTROLLER_SERVICE.getValue());
        testRunner.setProperty(SMB_CLIENT_PROVIDER_SERVICE, CLIENT_SERVICE_ID);
        testRunner.setProperty(PutSmbFile.DIRECTORY, DIRECTORY);
    }

    @AfterEach
    public void closeMocks() throws Exception {
        if (mockCloseable != null) {
            mockCloseable.close();
        }
    }

    private List<MockFlowFile> generateFlowFile(int numberOfFlowFiles, Map<String, String> attributes) {
        final List<MockFlowFile> result = new ArrayList<>();
        for (int i = 0; i < numberOfFlowFiles; i++) {
            final MockFlowFile flowFile = new MockFlowFile(FLOWFILE_ID_COUNTER.incrementAndGet());
            flowFile.putAttributes(attributes);
            result.add(flowFile);
        }
        return result;
    }

    private Set<SmbShareAccess> testWriteFileShareAccess() {
        when(clientService.folderExists(any())).thenReturn(true);
        testRunner.enqueue("data");
        testRunner.run();

        verify(clientService, times(1)).writeFile(anyString(), any(), shareAccessCaptor.capture());
        return shareAccessCaptor.getValue();
    }

    @Test
    public void testHostnameAndShareEL() {
        when(clientProviderService.getServiceLocation(anyMap())).thenAnswer(invocation -> {
            final Map<String, String> attrs = invocation.getArgument(0);
            final String host = attrs.getOrDefault("smb.hostname", HOSTNAME);
            final String share = attrs.getOrDefault("smb.share", SHARE);
            return URI.create("smb://" + host + ":445/" + share);
        });

        testRunner.setProperty(PutSmbFile.BATCH_SIZE, "20");

        // Add 10 FlowFiles with the same hostname and share property values
        final Map<String, String> attributes1 = new HashMap<>(Map.of(
                "smb.hostname", "test-host-1",
                "smb.share", "test-share-1"
        ));
        final List<MockFlowFile> flowFiles1 = generateFlowFile(10, attributes1);
        testRunner.enqueue(flowFiles1.toArray(new FlowFile[0]));

        // Add 20 FlowFiles with a different hostname and share property value than the first 10
        final Map<String, String> attributes2 = new HashMap<>(Map.of(
                "smb.hostname", "test-host-2",
                "smb.share", "test-share-2"
        ));
        final List<MockFlowFile> flowFiles2 = generateFlowFile(20, attributes2);
        testRunner.enqueue(flowFiles2.toArray(new FlowFile[0]));

        //trigger the processor only once
        testRunner.run(1);

        // Since 10 FlowFiles share the same hostname and share as the first processed FlowFile, 20 FlowFiles should remain in the queue.
        assertEquals(20, testRunner.getQueueSize().getObjectCount());
    }

    @Test
    public void testHostnameAndShareELWhenBatchsizeIsLowerThanAcceptableFlowFiles() {
        when(clientProviderService.getServiceLocation(anyMap())).thenAnswer(invocation -> {
            final Map<String, String> attrs = invocation.getArgument(0);
            final String host = attrs.getOrDefault("smb.hostname", HOSTNAME);
            final String share = attrs.getOrDefault("smb.share", SHARE);
            return URI.create("smb://" + host + ":445/" + share);
        });

        testRunner.setProperty(PutSmbFile.BATCH_SIZE, "10");

        // Add 20 FlowFiles with the same hostname and share property values
        final Map<String, String> attributes1 = new HashMap<>(Map.of(
                "smb.hostname", "test-host-1",
                "smb.share", "test-share-1"
        ));
        final List<MockFlowFile> flowFiles1 = generateFlowFile(20, attributes1);
        testRunner.enqueue(flowFiles1.toArray(new FlowFile[0]));

        // Add 20 FlowFiles with a different hostname and share property value than the first 20
        final Map<String, String> attributes2 = new HashMap<>(Map.of(
                "smb.hostname", "test-host-2",
                "smb.share", "test-share-2"
        ));
        final List<MockFlowFile> flowFiles2 = generateFlowFile(20, attributes2);
        testRunner.enqueue(flowFiles2.toArray(new FlowFile[0]));

        //trigger the processor only once
        testRunner.run(1);

        // 20 FlowFiles share the same hostname and share as the first processed FlowFile, but since the batch size is 10, 30 FlowFiles should remain in the queue
        assertEquals(30, testRunner.getQueueSize().getObjectCount());
    }

    @Test
    public void testDirExistsWithoutCreate() {
        when(clientService.folderExists(any())).thenReturn(false);
        testRunner.setProperty(PutSmbFile.DIRECTORY, "smbdirectory/subdir");
        testRunner.setProperty(PutSmbFile.CREATE_DIRS, "false");
        testRunner.enqueue("data");
        testRunner.run();

        verify(clientService, times(0)).ensureDirectory(anyString());
        testRunner.assertAllFlowFilesTransferred(PutSmbFile.REL_FAILURE);
    }

    @Test
    public void testDirExistsWithCreate() {
        when(clientService.folderExists(any())).thenReturn(false);
        testRunner.setProperty(PutSmbFile.DIRECTORY, "smbdirectory/subdir");
        testRunner.setProperty(PutSmbFile.CREATE_DIRS, "true");
        testRunner.enqueue("data");
        testRunner.run();

        verify(clientService, times(1)).ensureDirectory("smbdirectory/subdir");
        testRunner.assertAllFlowFilesTransferred(PutSmbFile.REL_SUCCESS);
    }

    @Test
    public void testDirectoriesCreatedWhenDontExists() {
        final String directory = "a\\b/c/b\\e";
        when(clientService.folderExists(any())).thenReturn(false);

        testRunner.setProperty(PutSmbFile.CREATE_DIRS, "true");
        testRunner.setProperty(PutSmbFile.DIRECTORY, directory);
        testRunner.enqueue("data");
        testRunner.run();

        verify(clientService, times(1)).ensureDirectory("a/b/c/b/e");
    }

    @Test
    public void testBatchCanContinueAfterDirectoryCreationFailure() {
        when(clientService.folderExists(any())).thenReturn(false);
        doThrow(new RuntimeException("Access denied")).when(clientService).ensureDirectory("dir2");

        FlowFile flowFile1 = createFlowFileWithDirectoryAttribute(1, "dir1");
        FlowFile flowFile2 = createFlowFileWithDirectoryAttribute(2, "dir2");
        FlowFile flowFile3 = createFlowFileWithDirectoryAttribute(3, "dir3");

        testRunner.setProperty(PutSmbFile.CREATE_DIRS, "true");
        testRunner.setProperty(PutSmbFile.DIRECTORY, "${directory}");
        testRunner.enqueue(flowFile1, flowFile2, flowFile3);
        testRunner.run();

        testRunner.assertTransferCount(PutSmbFile.REL_SUCCESS, 2);
        testRunner.assertTransferCount(PutSmbFile.REL_FAILURE, 1);
    }

    private FlowFile createFlowFileWithDirectoryAttribute(long id, String directory) {
        MockFlowFile flowFile = new MockFlowFile(id);
        flowFile.putAttributes(Map.of("directory", directory));
        return flowFile;
    }

    @Test
    public void testFileShareNone() {
        testRunner.setProperty(PutSmbFile.SHARE_ACCESS, PutSmbFile.SHARE_ACCESS_NONE);
        final Set<SmbShareAccess> shareAccess = testWriteFileShareAccess();
        assertTrue(shareAccess.isEmpty());
    }

    @Test
    public void testFileShareRead() {
        testRunner.setProperty(PutSmbFile.SHARE_ACCESS, PutSmbFile.SHARE_ACCESS_READ);
        final Set<SmbShareAccess> shareAccess = testWriteFileShareAccess();
        assertTrue(shareAccess.contains(SmbShareAccess.READ_ALLOWED));
    }

    @Test
    public void testFileShareReadWriteDelete() {
        testRunner.setProperty(PutSmbFile.SHARE_ACCESS, PutSmbFile.SHARE_ACCESS_READWRITEDELETE);
        final Set<SmbShareAccess> shareAccess = testWriteFileShareAccess();
        assertTrue(shareAccess.contains(SmbShareAccess.READ_ALLOWED));
        assertTrue(shareAccess.contains(SmbShareAccess.WRITE_ALLOWED));
        assertTrue(shareAccess.contains(SmbShareAccess.DELETE_ALLOWED));
    }

    @Test
    public void testFileExistsFail() {
        testRunner.setProperty(PutSmbFile.CONFLICT_RESOLUTION, PutSmbFile.FAIL_RESOLUTION);
        when(clientService.folderExists(any())).thenReturn(true);
        when(clientService.fileExists(any())).thenReturn(true);
        testRunner.enqueue("data");
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutSmbFile.REL_FAILURE);
    }

    @Test
    public void testFileExistsIgnore() {
        testRunner.setProperty(PutSmbFile.CONFLICT_RESOLUTION, PutSmbFile.IGNORE_RESOLUTION);
        when(clientService.folderExists(any())).thenReturn(true);
        when(clientService.fileExists(any())).thenReturn(true);
        testRunner.enqueue("data");
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutSmbFile.REL_SUCCESS);
    }

    @Test
    public void testTemporarySuffixIsUnset() {
        testRunner.enqueue("data");
        testRunner.run();

        verify(clientService, never()).renameFile(anyString(), anyString(), anyBoolean());
    }

    @Test
    public void testTemporarySuffixIsSet() {
        final String suffix = ".test";

        testRunner.setProperty(PutSmbFile.RENAME_SUFFIX, suffix);
        testRunner.setProperty(PutSmbFile.CREATE_DIRS, "true");
        testRunner.enqueue("data");
        testRunner.run();

        final ArgumentCaptor<String> writePath = ArgumentCaptor.forClass(String.class);
        verify(clientService, times(1)).writeFile(writePath.capture(), any(), anySet());
        assertTrue(writePath.getValue().endsWith(suffix), "Suffix is not present");
    }

    @Test
    public void testTemporarySuffixIsSetRenameIsCalled() {
        final String suffix = ".test";

        testRunner.setProperty(PutSmbFile.RENAME_SUFFIX, suffix);
        testRunner.setProperty(PutSmbFile.CREATE_DIRS, "true");
        testRunner.enqueue("data");
        testRunner.run();

        final ArgumentCaptor<String> writePath = ArgumentCaptor.forClass(String.class);
        final ArgumentCaptor<String> newPath = ArgumentCaptor.forClass(String.class);
        final ArgumentCaptor<Boolean> replace = ArgumentCaptor.forClass(Boolean.class);

        verify(clientService, times(1)).writeFile(writePath.capture(), any(), anySet());
        verify(clientService, times(1)).renameFile(anyString(), newPath.capture(), replace.capture());

        assertTrue(writePath.getValue().endsWith(suffix), "Suffix is not present and it should be");
        assertFalse(newPath.getValue().endsWith(suffix), "Suffix is present and it shouldn't be");
        assertTrue(replace.getValue(), "Replace flag should be true");
    }

    @Test
    public void testConnectionError() throws IOException {
        String emsg = "mock connection exception";
        when(clientProviderService.getClient(any(ComponentLog.class), anyMap())).thenThrow(new IOException(emsg));

        testRunner.enqueue("1");
        testRunner.enqueue("2");
        testRunner.enqueue("3");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutSmbFile.REL_FAILURE, 3);
    }

    @Test
    void testNormalizePath() {
        final PutSmbFile processor = new PutSmbFile();

        assertNull(processor.normalizePath(null));

        assertEquals("", processor.normalizePath("/"));
        assertEquals("", processor.normalizePath("\\"));

        assertEquals("d1/d2", processor.normalizePath("d1/d2"));
        assertEquals("d1/d2", processor.normalizePath("/d1/d2/"));
        assertEquals("d1/d2", processor.normalizePath("//d1//d2//"));

        assertEquals("d1/d2", processor.normalizePath("d1\\d2"));
        assertEquals("d1/d2", processor.normalizePath("\\d1\\d2\\"));
        assertEquals("d1/d2", processor.normalizePath("\\\\d1\\\\d2\\\\"));
    }

}
