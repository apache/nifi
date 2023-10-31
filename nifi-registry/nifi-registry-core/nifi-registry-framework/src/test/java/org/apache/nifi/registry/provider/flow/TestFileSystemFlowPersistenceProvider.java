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
package org.apache.nifi.registry.provider.flow;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.registry.flow.FlowPersistenceProvider;
import org.apache.nifi.registry.flow.FlowSnapshotContext;
import org.apache.nifi.registry.provider.ProviderConfigurationContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

public class TestFileSystemFlowPersistenceProvider {

    private static final String BUCKET_ID = "b0000000-0000-0000-0000-000000000000";

    private static final String SECOND_BUCKET_ID = "b2000000-0000-0000-0000-000000000000";

    private static final String FLOW_ID = "f0000000-0000-0000-0000-000000000000";

    private static final String SECOND_FLOW_ID = "f2000000-0000-0000-0000-000000000000";

    private static final String FIRST_VERSION = "1.0.0";

    private static final String SECOND_VERSION = "1.1.0";

    static final String FLOW_STORAGE_DIR = "target/flow_storage";

    static final ProviderConfigurationContext CONFIGURATION_CONTEXT = () -> {
        final Map<String,String> props = new HashMap<>();
        props.put(FileSystemFlowPersistenceProvider.FLOW_STORAGE_DIR_PROP, FLOW_STORAGE_DIR);
        return props;
    };

    private File flowStorageDir;
    private FileSystemFlowPersistenceProvider fileSystemFlowProvider;

    @BeforeEach
    public void setup() throws IOException {
        flowStorageDir = new File(FLOW_STORAGE_DIR);
        if (flowStorageDir.exists()) {
            org.apache.commons.io.FileUtils.cleanDirectory(flowStorageDir);
            assertTrue(flowStorageDir.delete());
        }

        assertFalse(flowStorageDir.exists());

        fileSystemFlowProvider = new FileSystemFlowPersistenceProvider();
        fileSystemFlowProvider.onConfigured(CONFIGURATION_CONTEXT);
        assertTrue(flowStorageDir.exists());
    }

    @Test
    public void testSaveSuccessfully() throws IOException {
        createAndSaveSnapshot(fileSystemFlowProvider, 1, FIRST_VERSION);
        verifySnapshot(flowStorageDir, 1, FIRST_VERSION);

        createAndSaveSnapshot(fileSystemFlowProvider, 2, SECOND_VERSION);
        verifySnapshot(flowStorageDir, 2, SECOND_VERSION);
    }

    @Test
    public void testSaveWithExistingVersion() throws IOException {
        final FlowSnapshotContext context = Mockito.mock(FlowSnapshotContext.class);
        when(context.getBucketId()).thenReturn(BUCKET_ID);
        when(context.getFlowId()).thenReturn(FLOW_ID);
        when(context.getVersion()).thenReturn(1);

        final byte[] content = "flow1v1".getBytes(StandardCharsets.UTF_8);
        fileSystemFlowProvider.saveFlowContent(context, content);

        // save new content for an existing version
        final byte[] content2 = "XXX".getBytes(StandardCharsets.UTF_8);
        assertThrows(Exception.class, () -> fileSystemFlowProvider.saveFlowContent(context, content2));

        // verify the new content wasn't written
        final String path = String.format("%s/%s/1/1%s", BUCKET_ID, FLOW_ID, FileSystemFlowPersistenceProvider.SNAPSHOT_EXTENSION);
        final File flowSnapshotFile = new File(flowStorageDir, path);
        try (InputStream in = new FileInputStream(flowSnapshotFile)) {
            assertEquals("flow1v1", IOUtils.toString(in, StandardCharsets.UTF_8));
        }
    }

    @Test
    public void testSaveAndGet() {
        createAndSaveSnapshot(fileSystemFlowProvider, 1, FIRST_VERSION);
        createAndSaveSnapshot(fileSystemFlowProvider, 2, SECOND_VERSION);

        final byte[] flow1v1 = fileSystemFlowProvider.getFlowContent(BUCKET_ID, FLOW_ID, 1);
        assertEquals(FIRST_VERSION, new String(flow1v1, StandardCharsets.UTF_8));

        final byte[] flow1v2 = fileSystemFlowProvider.getFlowContent(BUCKET_ID, FLOW_ID, 2);
        assertEquals(SECOND_VERSION, new String(flow1v2, StandardCharsets.UTF_8));
    }

    @Test
    public void testGetWhenDoesNotExist() {
        final byte[] flow1v1 = fileSystemFlowProvider.getFlowContent(BUCKET_ID, FLOW_ID, 1);
        assertNull(flow1v1);
    }

    @Test
    public void testDeleteSnapshots() {
        createAndSaveSnapshot(fileSystemFlowProvider, 1, FIRST_VERSION);
        createAndSaveSnapshot(fileSystemFlowProvider, 2, SECOND_VERSION);

        assertNotNull(fileSystemFlowProvider.getFlowContent(BUCKET_ID, FLOW_ID, 1));
        assertNotNull(fileSystemFlowProvider.getFlowContent(BUCKET_ID, FLOW_ID, 2));

        fileSystemFlowProvider.deleteAllFlowContent(BUCKET_ID, FLOW_ID);

        assertNull(fileSystemFlowProvider.getFlowContent(BUCKET_ID, FLOW_ID, 1));
        assertNull(fileSystemFlowProvider.getFlowContent(BUCKET_ID, FLOW_ID, 2));

        // delete a flow that doesn't exist
        fileSystemFlowProvider.deleteAllFlowContent(BUCKET_ID, SECOND_FLOW_ID);

        // delete a bucket that doesn't exist
        fileSystemFlowProvider.deleteAllFlowContent(SECOND_BUCKET_ID, FLOW_ID);
    }

    @Test
    public void testDeleteSnapshot() {
        createAndSaveSnapshot(fileSystemFlowProvider, 1, FIRST_VERSION);
        createAndSaveSnapshot(fileSystemFlowProvider, 2, SECOND_VERSION);

        assertNotNull(fileSystemFlowProvider.getFlowContent(BUCKET_ID, FLOW_ID, 1));
        assertNotNull(fileSystemFlowProvider.getFlowContent(BUCKET_ID, FLOW_ID, 2));

        fileSystemFlowProvider.deleteFlowContent(BUCKET_ID, FLOW_ID, 1);

        assertNull(fileSystemFlowProvider.getFlowContent(BUCKET_ID, FLOW_ID, 1));
        assertNotNull(fileSystemFlowProvider.getFlowContent(BUCKET_ID, FLOW_ID, 2));

        fileSystemFlowProvider.deleteFlowContent(BUCKET_ID, FLOW_ID, 2);

        assertNull(fileSystemFlowProvider.getFlowContent(BUCKET_ID, FLOW_ID, 1));
        assertNull(fileSystemFlowProvider.getFlowContent(BUCKET_ID, FLOW_ID, 2));

        // delete a version that doesn't exist
        fileSystemFlowProvider.deleteFlowContent(BUCKET_ID, FLOW_ID, 3);

        // delete a flow that doesn't exist
        fileSystemFlowProvider.deleteFlowContent(BUCKET_ID, SECOND_FLOW_ID, 1);

        // delete a bucket that doesn't exist
        fileSystemFlowProvider.deleteFlowContent(SECOND_BUCKET_ID, FLOW_ID, 1);
    }

    private void createAndSaveSnapshot(final FlowPersistenceProvider flowPersistenceProvider, final int version, final String contentString) {
        final FlowSnapshotContext context = Mockito.mock(FlowSnapshotContext.class);
        when(context.getBucketId()).thenReturn(BUCKET_ID);
        when(context.getFlowId()).thenReturn(FLOW_ID);
        when(context.getVersion()).thenReturn(version);

        final byte[] content = contentString.getBytes(StandardCharsets.UTF_8);
        flowPersistenceProvider.saveFlowContent(context, content);
    }

    private void verifySnapshot(final File flowStorageDir, final int version, final String contentString) throws IOException {
        // verify the correct snapshot file was created
        final File flowSnapshotFile = new File(flowStorageDir,
                BUCKET_ID + "/" + FLOW_ID + "/" + version + "/" + version + FileSystemFlowPersistenceProvider.SNAPSHOT_EXTENSION);
        assertTrue(flowSnapshotFile.exists());

        try (InputStream in = new FileInputStream(flowSnapshotFile)) {
            assertEquals(contentString, IOUtils.toString(in, StandardCharsets.UTF_8));
        }
    }
}
