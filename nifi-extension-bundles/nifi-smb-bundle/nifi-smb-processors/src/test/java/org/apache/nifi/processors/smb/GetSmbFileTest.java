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

import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.services.smb.SmbClientProviderService;
import org.apache.nifi.services.smb.SmbClientService;
import org.apache.nifi.services.smb.SmbListableEntity;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.net.URI;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.nifi.processors.smb.util.LocalSmbProperties.CONNECTION_CONFIGURATION_STRATEGY;
import static org.apache.nifi.processors.smb.util.LocalSmbProperties.ConnectionConfigurationStrategy;
import static org.apache.nifi.processors.smb.util.LocalSmbProperties.SMB_CLIENT_PROVIDER_SERVICE;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class GetSmbFileTest {

    private TestRunner testRunner;

    @Mock
    private SmbClientProviderService clientProviderService;

    @Mock
    private SmbClientService clientService;

    private static final String HOSTNAME = "host";
    private static final String SHARE = "share";
    private static final String DIRECTORY = "nifi\\input";

    private static final String CLIENT_SERVICE_ID = "client-provider-service-id";

    @BeforeEach
    public void init() throws Exception {
        when(clientProviderService.getIdentifier()).thenReturn(CLIENT_SERVICE_ID);
        when(clientProviderService.getServiceLocation()).thenReturn(URI.create("smb://" + HOSTNAME + ":445/" + SHARE));
        when(clientProviderService.getClient(any(ComponentLog.class))).thenReturn(clientService);

        testRunner = TestRunners.newTestRunner(GetSmbFile.class);
        testRunner.addControllerService(CLIENT_SERVICE_ID, clientProviderService);
        testRunner.enableControllerService(clientProviderService);
        testRunner.setProperty(CONNECTION_CONFIGURATION_STRATEGY, ConnectionConfigurationStrategy.CONTROLLER_SERVICE.getValue());
        testRunner.setProperty(SMB_CLIENT_PROVIDER_SERVICE, CLIENT_SERVICE_ID);
        testRunner.setProperty(GetSmbFile.DIRECTORY, DIRECTORY);
    }

    private SmbListableEntity fileEntity(String path, String filename, boolean hidden) {
        return SmbListableEntity.builder()
                .setName(filename)
                .setPath(path.replace('\\', '/'))
                .setHidden(hidden)
                .build();
    }

    private SmbListableEntity fileEntity(String path, String filename) {
        return fileEntity(path, filename, false);
    }

    private void mockListing(SmbListableEntity... entities) {
        when(clientService.listFiles(anyString(), anyBoolean())).thenAnswer(inv -> Stream.of(entities));
    }

    private void verifyReadFile(String path, String filename, int times) {
        verify(clientService, times(times)).readFile(eq(path + "\\" + filename), any(), any());
    }

    @Test
    public void testOpenFileCalled() {
        mockListing(fileEntity(DIRECTORY, "file1.txt"));
        testRunner.run();
        verifyReadFile(DIRECTORY, "file1.txt", 1);
        verifyReadFile(DIRECTORY, "file2.txt", 0);
    }

    @Test
    public void testHiddenFile() {
        testRunner.setProperty(GetSmbFile.IGNORE_HIDDEN_FILES, "true");
        mockListing(
                fileEntity(DIRECTORY, "file1.txt", true),
                fileEntity(DIRECTORY, "file2.txt", false)
        );
        testRunner.run();
        verifyReadFile(DIRECTORY, "file1.txt", 0);
        verifyReadFile(DIRECTORY, "file2.txt", 1);
    }

    @Test
    public void testFileFilter() {
        testRunner.setProperty(GetSmbFile.FILE_FILTER, "file[0-9]\\.txt");
        mockListing(
                fileEntity(DIRECTORY, "something_else.txt"),
                fileEntity(DIRECTORY, "file1.txt"),
                fileEntity(DIRECTORY, "file2.txt")
        );
        testRunner.run();
        verifyReadFile(DIRECTORY, "something_else.txt", 0);
        verifyReadFile(DIRECTORY, "file1.txt", 1);
        verifyReadFile(DIRECTORY, "file2.txt", 1);
        testRunner.assertTransferCount(GetSmbFile.REL_SUCCESS, 2);
    }

    @Test
    public void testNonRecurse() {
        testRunner.setProperty(GetSmbFile.RECURSE, "false");
        String subdir = DIRECTORY + "\\subdir1";
        mockListing(
                fileEntity(DIRECTORY, "file1.txt"),
                fileEntity(DIRECTORY, "file2.txt")
        );
        testRunner.run();
        verifyReadFile(DIRECTORY, "file1.txt", 1);
        verifyReadFile(DIRECTORY, "file2.txt", 1);
        verifyReadFile(subdir, "file3.txt", 0);
        testRunner.assertTransferCount(GetSmbFile.REL_SUCCESS, 2);
    }

    @Test
    public void testRecurse() {
        testRunner.setProperty(GetSmbFile.RECURSE, "true");
        String subdir = DIRECTORY + "\\subdir1";
        mockListing(
                fileEntity(DIRECTORY, "file1.txt"),
                fileEntity(DIRECTORY, "file2.txt"),
                fileEntity(subdir, "file3.txt")
        );
        testRunner.run();
        verifyReadFile(DIRECTORY, "file1.txt", 1);
        verifyReadFile(DIRECTORY, "file2.txt", 1);
        verifyReadFile(subdir, "file3.txt", 1);
        testRunner.assertTransferCount(GetSmbFile.REL_SUCCESS, 3);
    }

    @Test
    public void testPathFilter() {
        testRunner.setProperty(GetSmbFile.RECURSE, "true");
        testRunner.setProperty(GetSmbFile.PATH_FILTER, ".*\\\\subdir[0-9]");
        String subdir1 = DIRECTORY + "\\subdir1";
        String subdir2 = DIRECTORY + "\\subdir2";
        String subdir3 = DIRECTORY + "\\foo";
        mockListing(
                fileEntity(subdir1, "file1.txt"),
                fileEntity(subdir2, "file2.txt"),
                fileEntity(subdir3, "file3.txt")
        );
        testRunner.run();
        verifyReadFile(subdir1, "file1.txt", 1);
        verifyReadFile(subdir2, "file2.txt", 1);
        verifyReadFile(subdir3, "file3.txt", 0);
        testRunner.assertTransferCount(GetSmbFile.REL_SUCCESS, 2);
    }

    @Test
    public void testBatchSize() {
        final int batchSize = 10;
        final int totalSize = batchSize * 2;
        testRunner.setProperty(GetSmbFile.BATCH_SIZE, Integer.toString(batchSize));

        final SmbListableEntity[] entities = new SmbListableEntity[totalSize];
        for (int i = 0; i < totalSize; i++) {
            entities[i] = fileEntity(DIRECTORY, "file-" + i);
        }
        when(clientService.listFiles(anyString(), anyBoolean())).thenAnswer(inv -> Stream.of(entities));

        // Avoid stopping on finish and run initialization
        testRunner.run(1, false, true);
        testRunner.assertTransferCount(GetSmbFile.REL_SUCCESS, batchSize);
        // Stop on finish and skip initialization to avoid clearing internal queue of files
        testRunner.run(1, true, false);
        testRunner.assertTransferCount(GetSmbFile.REL_SUCCESS, totalSize);

        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(GetSmbFile.REL_SUCCESS);
        final List<String> flowFileNames = flowFiles.stream()
                .map(flowFile -> flowFile.getAttribute(CoreAttributes.FILENAME.key()))
                .toList();

        for (int i = 0; i < totalSize; i++) {
            final String flowFileName = flowFileNames.get(i);
            assertTrue(flowFileNames.contains(flowFileName), String.format("FlowFile Name [%s] not found", flowFileName));
        }
    }
}
