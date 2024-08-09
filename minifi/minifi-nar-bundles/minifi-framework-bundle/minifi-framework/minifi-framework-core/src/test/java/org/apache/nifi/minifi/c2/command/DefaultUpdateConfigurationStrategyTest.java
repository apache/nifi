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

package org.apache.nifi.minifi.c2.command;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.exists;
import static java.nio.file.Files.newOutputStream;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.apache.nifi.minifi.commons.api.MiNiFiConstants.BACKUP_EXTENSION;
import static org.apache.nifi.minifi.commons.api.MiNiFiConstants.RAW_EXTENSION;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.commons.io.FilenameUtils;
import org.apache.nifi.c2.client.service.operation.UpdateConfigurationStrategy;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.flow.VersionedDataflow;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.minifi.commons.service.FlowEnrichService;
import org.apache.nifi.minifi.commons.service.FlowPropertyEncryptor;
import org.apache.nifi.minifi.commons.service.FlowSerDeService;
import org.apache.nifi.services.FlowService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DefaultUpdateConfigurationStrategyTest {

    private static final String FLOW_CONFIG_FILE_NAME = "flow.config.gz";

    private static final byte[] ORIGINAL_RAW_FLOW_CONFIG_CONTENT = "original_raw_content".getBytes(UTF_8);
    private static final byte[] ORIGINAL_ENRICHED_FLOW_CONFIG_CONTENT = "original_enriched_content".getBytes(UTF_8);

    private static final byte[] NEW_RAW_FLOW_CONFIG_CONTENT = "new_raw_content".getBytes(UTF_8);
    private static final VersionedDataflow NEW_RAW_FLOW_CONFIG = new VersionedDataflow();
    private static final byte[] NEW_ENCRYPTED_FLOW_CONFIG_CONTENT = "original_encrypted_content".getBytes(UTF_8);
    private static final VersionedDataflow NEW_ENCRYPTED_FLOW_CONFIG = new VersionedDataflow();
    private static final byte[] NEW_ENRICHED_FLOW_CONFIG_CONTENT = "new_enriched_content".getBytes(UTF_8);
    private static final VersionedDataflow NEW_ENRICHED_FLOW_CONFIG = new VersionedDataflow();

    @TempDir
    private File tempDir;

    @Mock
    private FlowController mockFlowController;
    @Mock
    private FlowService mockFlowService;
    @Mock
    private FlowEnrichService mockFlowEnrichService;
    @Mock
    private FlowPropertyEncryptor mockFlowPropertyEncryptor;
    @Mock
    private FlowSerDeService mockFlowSerDeService;
    @Mock
    private FlowManager mockFlowManager;
    @Mock
    private ProcessGroup mockProcessGroup;

    private Path flowConfigurationFile;
    private Path backupFlowConfigurationFile;
    private Path rawFlowConfigurationFile;
    private Path backupRawFlowConfigurationFile;

    private UpdateConfigurationStrategy testUpdateConfigurationStrategy;

    @BeforeEach
    public void setup() throws IOException {
        flowConfigurationFile = Path.of(tempDir.getAbsolutePath(), FLOW_CONFIG_FILE_NAME).toAbsolutePath();
        backupFlowConfigurationFile = Path.of(flowConfigurationFile + BACKUP_EXTENSION);
        String flowConfigurationFileBaseName = FilenameUtils.getBaseName(flowConfigurationFile.toString());
        rawFlowConfigurationFile = flowConfigurationFile.getParent().resolve(flowConfigurationFileBaseName + RAW_EXTENSION);
        backupRawFlowConfigurationFile = flowConfigurationFile.getParent().resolve(flowConfigurationFileBaseName + BACKUP_EXTENSION + RAW_EXTENSION);

        testUpdateConfigurationStrategy = new DefaultUpdateConfigurationStrategy(mockFlowController, mockFlowService, mockFlowEnrichService,
            mockFlowPropertyEncryptor, mockFlowSerDeService, flowConfigurationFile.toString());

        writeGzipFile(flowConfigurationFile, ORIGINAL_ENRICHED_FLOW_CONFIG_CONTENT);
        writePlainTextFile(rawFlowConfigurationFile, ORIGINAL_RAW_FLOW_CONFIG_CONTENT);
    }

    @Test
    public void testFlowIsUpdatedAndBackupsAreClearedUp() throws IOException {
        // given
        when(mockFlowSerDeService.deserialize(NEW_RAW_FLOW_CONFIG_CONTENT)).thenReturn(NEW_RAW_FLOW_CONFIG);
        when(mockFlowPropertyEncryptor.encryptSensitiveProperties(NEW_RAW_FLOW_CONFIG)).thenReturn(NEW_ENCRYPTED_FLOW_CONFIG);
        when(mockFlowSerDeService.serialize(NEW_ENCRYPTED_FLOW_CONFIG)).thenReturn(NEW_ENCRYPTED_FLOW_CONFIG_CONTENT);
        when(mockFlowEnrichService.enrichFlow(NEW_ENCRYPTED_FLOW_CONFIG)).thenReturn(NEW_ENRICHED_FLOW_CONFIG);
        when(mockFlowSerDeService.serialize(NEW_ENRICHED_FLOW_CONFIG)).thenReturn(NEW_ENRICHED_FLOW_CONFIG_CONTENT);
        when(mockFlowController.getFlowManager()).thenReturn(mockFlowManager);
        when(mockFlowManager.getRootGroup()).thenReturn(mockProcessGroup);

        // when
        testUpdateConfigurationStrategy.update(NEW_RAW_FLOW_CONFIG_CONTENT);

        //then
        assertTrue(exists(flowConfigurationFile));
        assertTrue(exists(rawFlowConfigurationFile));
        assertArrayEquals(NEW_ENRICHED_FLOW_CONFIG_CONTENT, readGzipFile(flowConfigurationFile));
        assertArrayEquals(NEW_ENCRYPTED_FLOW_CONFIG_CONTENT, readPlainTextFile(rawFlowConfigurationFile));
        assertFalse(exists(backupFlowConfigurationFile));
        assertFalse(exists(backupRawFlowConfigurationFile));
        verify(mockFlowService, times(1)).load(null);
        verify(mockFlowController, times(1)).onFlowInitialized(true);
        verify(mockProcessGroup, times(1)).startProcessing();
    }

    @Test
    public void testFlowIsRevertedInCaseOfAnyErrorAndBackupsAreClearedUp() throws IOException {
        // given
        when(mockFlowSerDeService.deserialize(NEW_RAW_FLOW_CONFIG_CONTENT)).thenReturn(NEW_RAW_FLOW_CONFIG);
        when(mockFlowPropertyEncryptor.encryptSensitiveProperties(NEW_RAW_FLOW_CONFIG)).thenReturn(NEW_ENCRYPTED_FLOW_CONFIG);
        when(mockFlowSerDeService.serialize(NEW_ENCRYPTED_FLOW_CONFIG)).thenReturn(NEW_ENCRYPTED_FLOW_CONFIG_CONTENT);
        when(mockFlowEnrichService.enrichFlow(NEW_ENCRYPTED_FLOW_CONFIG)).thenReturn(NEW_ENRICHED_FLOW_CONFIG);
        when(mockFlowSerDeService.serialize(NEW_ENRICHED_FLOW_CONFIG)).thenReturn(NEW_ENRICHED_FLOW_CONFIG_CONTENT);
        when(mockFlowController.getFlowManager()).thenReturn(mockFlowManager);
        when(mockFlowManager.getRootGroup()).thenReturn(mockProcessGroup);
        doThrow(new IOException()).when(mockFlowService).load(null);

        // when
        try {
            testUpdateConfigurationStrategy.update(NEW_RAW_FLOW_CONFIG_CONTENT);
        } catch (Exception e) {
            //then
            assertTrue(exists(flowConfigurationFile));
            assertTrue(exists(rawFlowConfigurationFile));
            assertArrayEquals(ORIGINAL_ENRICHED_FLOW_CONFIG_CONTENT, readGzipFile(flowConfigurationFile));
            assertArrayEquals(ORIGINAL_RAW_FLOW_CONFIG_CONTENT, readPlainTextFile(rawFlowConfigurationFile));
            assertFalse(exists(backupFlowConfigurationFile));
            assertFalse(exists(backupRawFlowConfigurationFile));
            verify(mockFlowService, times(1)).load(null);
            verify(mockFlowController, times(0)).onFlowInitialized(true);
            verify(mockProcessGroup, times(0)).startProcessing();
        }
    }

    private void writeGzipFile(Path path, byte[] content) throws IOException {
        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(content);
             OutputStream outputStream = new GZIPOutputStream(newOutputStream(path, WRITE, CREATE, TRUNCATE_EXISTING))) {
            inputStream.transferTo(outputStream);
        }
    }

    private byte[] readGzipFile(Path path) throws IOException {
        try (InputStream inputStream = new GZIPInputStream(Files.newInputStream(path));
             ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            inputStream.transferTo(outputStream);
            outputStream.flush();
            return outputStream.toByteArray();
        }
    }

    private void writePlainTextFile(Path path, byte[] content) throws IOException {
        Files.write(path, content, WRITE, CREATE, TRUNCATE_EXISTING);
    }

    private byte[] readPlainTextFile(Path path) throws IOException {
        return Files.readAllBytes(path);
    }
}
