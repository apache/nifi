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
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.registry.flow.FlowPersistenceException;
import org.apache.nifi.registry.flow.FlowPersistenceProvider;
import org.apache.nifi.registry.flow.FlowSnapshotContext;
import org.apache.nifi.registry.provider.ProviderConfigurationContext;
import org.apache.nifi.registry.provider.ProviderCreationException;
import org.apache.nifi.registry.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

/**
 * A FlowPersistenceProvider that uses the local filesystem for storage.
 */
public class FileSystemFlowPersistenceProvider implements FlowPersistenceProvider {

    static final Logger LOGGER = LoggerFactory.getLogger(FileSystemFlowPersistenceProvider.class);

    static final String FLOW_STORAGE_DIR_PROP = "Flow Storage Directory";

    static final String SNAPSHOT_EXTENSION = ".snapshot";

    private File flowStorageDir;

    @Override
    public void onConfigured(final ProviderConfigurationContext configurationContext) throws ProviderCreationException {
        final Map<String,String> props = configurationContext.getProperties();
        if (!props.containsKey(FLOW_STORAGE_DIR_PROP)) {
            throw new ProviderCreationException("The property " + FLOW_STORAGE_DIR_PROP + " must be provided");
        }

        final String flowStorageDirValue = props.get(FLOW_STORAGE_DIR_PROP);
        if (StringUtils.isBlank(flowStorageDirValue)) {
            throw new ProviderCreationException("The property " + FLOW_STORAGE_DIR_PROP + " cannot be null or blank");
        }

        try {
            flowStorageDir = new File(flowStorageDirValue);
            FileUtils.ensureDirectoryExistAndCanReadAndWrite(flowStorageDir);
            LOGGER.info("Configured FileSystemFlowPersistenceProvider with Flow Storage Directory {}", new Object[] {flowStorageDir.getAbsolutePath()});
        } catch (IOException e) {
            throw new ProviderCreationException(e);
        }
    }

    @Override
    public synchronized void saveFlowContent(final FlowSnapshotContext context, final byte[] content) throws FlowPersistenceException {
        final File bucketDir = new File(flowStorageDir, context.getBucketId());
        try {
            FileUtils.ensureDirectoryExistAndCanReadAndWrite(bucketDir);
        } catch (IOException e) {
            throw new FlowPersistenceException("Error accessing bucket directory at " + bucketDir.getAbsolutePath(), e);
        }

        final File flowDir = new File(bucketDir, context.getFlowId());
        try {
            FileUtils.ensureDirectoryExistAndCanReadAndWrite(flowDir);
        } catch (IOException e) {
            throw new FlowPersistenceException("Error accessing flow directory at " + flowDir.getAbsolutePath(), e);
        }

        final String versionString = String.valueOf(context.getVersion());
        final File versionDir = new File(flowDir, versionString);
        try {
            FileUtils.ensureDirectoryExistAndCanReadAndWrite(versionDir);
        } catch (IOException e) {
            throw new FlowPersistenceException("Error accessing version directory at " + versionDir.getAbsolutePath(), e);
        }

        final File versionFile = new File(versionDir, versionString + SNAPSHOT_EXTENSION);
        if (versionFile.exists()) {
            throw new FlowPersistenceException("Unable to save, a snapshot already exists with version " + versionString);
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Saving snapshot with filename {}", new Object[] {versionFile.getAbsolutePath()});
        }

        try (final OutputStream out = new FileOutputStream(versionFile)) {
            out.write(content);
            out.flush();
        } catch (Exception e) {
            throw new FlowPersistenceException("Unable to write snapshot to disk due to " + e.getMessage(), e);
        }
    }

    @Override
    public synchronized byte[] getFlowContent(final String bucketId, final String flowId, final int version) throws FlowPersistenceException {
        final File snapshotFile = getSnapshotFile(bucketId, flowId, version);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Retrieving snapshot with filename {}", new Object[] {snapshotFile.getAbsolutePath()});
        }

        if (!snapshotFile.exists()) {
            return null;
        }

        try (final InputStream in = new FileInputStream(snapshotFile)){
            return IOUtils.toByteArray(in);
        } catch (IOException e) {
            throw new FlowPersistenceException("Error reading snapshot file: " + snapshotFile.getAbsolutePath(), e);
        }
    }

    @Override
    public synchronized void deleteAllFlowContent(final String bucketId, final String flowId) throws FlowPersistenceException {
        final File flowDir = new File(flowStorageDir, bucketId + "/" + flowId);
        if (!flowDir.exists()) {
            LOGGER.debug("Snapshot directory does not exist at {}", new Object[] {flowDir.getAbsolutePath()});
            return;
        }

        // delete everything under the flow directory
        try {
            org.apache.commons.io.FileUtils.cleanDirectory(flowDir);
        } catch (IOException e) {
            throw new FlowPersistenceException("Error deleting snapshots at " + flowDir.getAbsolutePath(), e);
        }

        // delete the directory for the flow
        final boolean flowDirDeleted = flowDir.delete();
        if (!flowDirDeleted) {
            LOGGER.error("Unable to delete flow directory: " + flowDir.getAbsolutePath());
        }

        // delete the directory for the bucket if there is nothing left
        final File bucketDir = new File(flowStorageDir, bucketId);
        final File[] bucketFiles = bucketDir.listFiles();
        if (bucketFiles.length == 0) {
            final boolean deletedBucket = bucketDir.delete();
            if (!deletedBucket) {
                LOGGER.error("Unable to delete bucket directory: " + flowDir.getAbsolutePath());
            }
        }
    }

    @Override
    public synchronized void deleteFlowContent(final String bucketId, final String flowId, final int version) throws FlowPersistenceException {
        final File snapshotFile = getSnapshotFile(bucketId, flowId, version);
        if (!snapshotFile.exists()) {
            LOGGER.debug("Snapshot file does not exist at {}", new Object[] {snapshotFile.getAbsolutePath()});
            return;
        }

        final boolean deleted = snapshotFile.delete();
        if (!deleted) {
            throw new FlowPersistenceException("Unable to delete snapshot at " + snapshotFile.getAbsolutePath());
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Deleted snapshot at {}", new Object[] {snapshotFile.getAbsolutePath()});
        }
    }

    protected File getSnapshotFile(final String bucketId, final String flowId, final int version) {
        final String snapshotFilename = bucketId + "/" + flowId + "/" + version + "/" + version + SNAPSHOT_EXTENSION;
        return new File(flowStorageDir, snapshotFilename);
    }

}
