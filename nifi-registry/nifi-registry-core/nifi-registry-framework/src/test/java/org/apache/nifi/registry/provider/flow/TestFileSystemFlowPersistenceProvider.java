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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.when;

public class TestFileSystemFlowPersistenceProvider {

    static final String FLOW_STORAGE_DIR = "target/flow_storage";

    static final ProviderConfigurationContext CONFIGURATION_CONTEXT = new ProviderConfigurationContext() {
        @Override
        public Map<String, String> getProperties() {
            final Map<String,String> props = new HashMap<>();
            props.put(FileSystemFlowPersistenceProvider.FLOW_STORAGE_DIR_PROP, FLOW_STORAGE_DIR);
            return props;
        }
    };

    private File flowStorageDir;
    private FileSystemFlowPersistenceProvider fileSystemFlowProvider;

    @Before
    public void setup() throws IOException {
        flowStorageDir = new File(FLOW_STORAGE_DIR);
        if (flowStorageDir.exists()) {
            org.apache.commons.io.FileUtils.cleanDirectory(flowStorageDir);
            flowStorageDir.delete();
        }

        Assert.assertFalse(flowStorageDir.exists());

        fileSystemFlowProvider = new FileSystemFlowPersistenceProvider();
        fileSystemFlowProvider.onConfigured(CONFIGURATION_CONTEXT);
        Assert.assertTrue(flowStorageDir.exists());
    }

    @Test
    public void testSaveSuccessfully() throws IOException {
        createAndSaveSnapshot(fileSystemFlowProvider,"bucket1", "flow1", 1, "flow1v1");
        verifySnapshot(flowStorageDir, "bucket1", "flow1", 1, "flow1v1");

        createAndSaveSnapshot(fileSystemFlowProvider,"bucket1", "flow1", 2, "flow1v2");
        verifySnapshot(flowStorageDir, "bucket1", "flow1", 2, "flow1v2");

        createAndSaveSnapshot(fileSystemFlowProvider,"bucket1", "flow2", 1, "flow2v1");
        verifySnapshot(flowStorageDir, "bucket1", "flow2", 1, "flow2v1");

        createAndSaveSnapshot(fileSystemFlowProvider,"bucket2", "flow3", 1, "flow3v1");
        verifySnapshot(flowStorageDir, "bucket2", "flow3", 1, "flow3v1");
    }

    @Test
    public void testSaveWithExistingVersion() throws IOException {
        final FlowSnapshotContext context = Mockito.mock(FlowSnapshotContext.class);
        when(context.getBucketId()).thenReturn("bucket1");
        when(context.getFlowId()).thenReturn("flow1");
        when(context.getVersion()).thenReturn(1);

        final byte[] content = "flow1v1".getBytes(StandardCharsets.UTF_8);
        fileSystemFlowProvider.saveFlowContent(context, content);

        // save new content for an existing version
        final byte[] content2 = "XXX".getBytes(StandardCharsets.UTF_8);
        try {
            fileSystemFlowProvider.saveFlowContent(context, content2);
            Assert.fail("Should have thrown exception");
        } catch (Exception e) {

        }

        // verify the new content wasn't written
        final File flowSnapshotFile = new File(flowStorageDir, "bucket1/flow1/1/1" + FileSystemFlowPersistenceProvider.SNAPSHOT_EXTENSION);
        try (InputStream in = new FileInputStream(flowSnapshotFile)) {
            Assert.assertEquals("flow1v1", IOUtils.toString(in, StandardCharsets.UTF_8));
        }
    }

    @Test
    public void testSaveAndGet() throws IOException {
        createAndSaveSnapshot(fileSystemFlowProvider,"bucket1", "flow1", 1, "flow1v1");
        createAndSaveSnapshot(fileSystemFlowProvider,"bucket1", "flow1", 2, "flow1v2");

        final byte[] flow1v1 = fileSystemFlowProvider.getFlowContent("bucket1", "flow1", 1);
        Assert.assertEquals("flow1v1", new String(flow1v1, StandardCharsets.UTF_8));

        final byte[] flow1v2 = fileSystemFlowProvider.getFlowContent("bucket1", "flow1", 2);
        Assert.assertEquals("flow1v2", new String(flow1v2, StandardCharsets.UTF_8));
    }

    @Test
    public void testGetWhenDoesNotExist() {
        final byte[] flow1v1 = fileSystemFlowProvider.getFlowContent("bucket1", "flow1", 1);
        Assert.assertNull(flow1v1);
    }

    @Test
    public void testDeleteSnapshots() throws IOException {
        final String bucketId = "bucket1";
        final String flowId = "flow1";

        createAndSaveSnapshot(fileSystemFlowProvider, bucketId, flowId, 1, "flow1v1");
        createAndSaveSnapshot(fileSystemFlowProvider, bucketId, flowId, 2, "flow1v2");

        Assert.assertNotNull(fileSystemFlowProvider.getFlowContent(bucketId, flowId, 1));
        Assert.assertNotNull(fileSystemFlowProvider.getFlowContent(bucketId, flowId, 2));

        fileSystemFlowProvider.deleteAllFlowContent(bucketId, flowId);

        Assert.assertNull(fileSystemFlowProvider.getFlowContent(bucketId, flowId, 1));
        Assert.assertNull(fileSystemFlowProvider.getFlowContent(bucketId, flowId, 2));

        // delete a flow that doesn't exist
        fileSystemFlowProvider.deleteAllFlowContent(bucketId, "some-other-flow");

        // delete a bucket that doesn't exist
        fileSystemFlowProvider.deleteAllFlowContent("some-other-bucket", flowId);
    }

    @Test
    public void testDeleteSnapshot() throws IOException {
        final String bucketId = "bucket1";
        final String flowId = "flow1";

        createAndSaveSnapshot(fileSystemFlowProvider, bucketId, flowId, 1, "flow1v1");
        createAndSaveSnapshot(fileSystemFlowProvider, bucketId, flowId, 2, "flow1v2");

        Assert.assertNotNull(fileSystemFlowProvider.getFlowContent(bucketId, flowId, 1));
        Assert.assertNotNull(fileSystemFlowProvider.getFlowContent(bucketId, flowId, 2));

        fileSystemFlowProvider.deleteFlowContent(bucketId, flowId, 1);

        Assert.assertNull(fileSystemFlowProvider.getFlowContent(bucketId, flowId, 1));
        Assert.assertNotNull(fileSystemFlowProvider.getFlowContent(bucketId, flowId, 2));

        fileSystemFlowProvider.deleteFlowContent(bucketId, flowId, 2);

        Assert.assertNull(fileSystemFlowProvider.getFlowContent(bucketId, flowId, 1));
        Assert.assertNull(fileSystemFlowProvider.getFlowContent(bucketId, flowId, 2));

        // delete a version that doesn't exist
        fileSystemFlowProvider.deleteFlowContent(bucketId, flowId, 3);

        // delete a flow that doesn't exist
        fileSystemFlowProvider.deleteFlowContent(bucketId, "some-other-flow", 1);

        // delete a bucket that doesn't exist
        fileSystemFlowProvider.deleteFlowContent("some-other-bucket", flowId, 1);
    }

    private void createAndSaveSnapshot(final FlowPersistenceProvider flowPersistenceProvider, final String bucketId, final String flowId, final int version,
                                       final String contentString) throws IOException {
        final FlowSnapshotContext context = Mockito.mock(FlowSnapshotContext.class);
        when(context.getBucketId()).thenReturn(bucketId);
        when(context.getFlowId()).thenReturn(flowId);
        when(context.getVersion()).thenReturn(version);

        final byte[] content = contentString.getBytes(StandardCharsets.UTF_8);
        flowPersistenceProvider.saveFlowContent(context, content);
    }

    private void verifySnapshot(final File flowStorageDir, final String bucketId, final String flowId, final int version,
                                final String contentString) throws IOException {
        // verify the correct snapshot file was created
        final File flowSnapshotFile = new File(flowStorageDir,
                bucketId + "/" + flowId + "/" + version + "/" + version + FileSystemFlowPersistenceProvider.SNAPSHOT_EXTENSION);
        Assert.assertTrue(flowSnapshotFile.exists());

        try (InputStream in = new FileInputStream(flowSnapshotFile)) {
            Assert.assertEquals(contentString, IOUtils.toString(in, StandardCharsets.UTF_8));
        }
    }
}
