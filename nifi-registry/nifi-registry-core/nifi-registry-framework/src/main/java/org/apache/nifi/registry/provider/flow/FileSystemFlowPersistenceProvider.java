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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A FlowPersistenceProvider that uses the local filesystem for storage.
 */
public class FileSystemFlowPersistenceProvider implements FlowPersistenceProvider {

    static final Logger LOGGER = LoggerFactory.getLogger(FileSystemFlowPersistenceProvider.class);

    static final String FLOW_STORAGE_DIR_PROP = "Flow Storage Directory";

    static final String SNAPSHOT_EXTENSION = ".snapshot";

    private static final Pattern NUMBER_PATTERN = Pattern.compile("\\d+");

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
            LOGGER.info("Configured FileSystemFlowPersistenceProvider with Flow Storage Directory {}", flowStorageDir.getAbsolutePath());
        } catch (IOException e) {
            throw new ProviderCreationException(e);
        }
    }

    @Override
    public synchronized void saveFlowContent(final FlowSnapshotContext context, final byte[] content) throws FlowPersistenceException {
        final File bucketDir = getChildLocation(flowStorageDir, getNormalizedIdPath(context.getBucketId()));
        try {
            FileUtils.ensureDirectoryExistAndCanReadAndWrite(bucketDir);
        } catch (IOException e) {
            throw new FlowPersistenceException("Error accessing bucket directory at " + bucketDir.getAbsolutePath(), e);
        }

        final File flowDir = getChildLocation(bucketDir, getNormalizedIdPath(context.getFlowId()));
        try {
            FileUtils.ensureDirectoryExistAndCanReadAndWrite(flowDir);
        } catch (IOException e) {
            throw new FlowPersistenceException("Error accessing flow directory at " + flowDir.getAbsolutePath(), e);
        }

        final String versionString = String.valueOf(context.getVersion());
        final File versionDir = getChildLocation(flowDir, Paths.get(versionString));
        try {
            FileUtils.ensureDirectoryExistAndCanReadAndWrite(versionDir);
        } catch (IOException e) {
            throw new FlowPersistenceException("Error accessing version directory at " + versionDir.getAbsolutePath(), e);
        }

        final String versionExtension = versionString + SNAPSHOT_EXTENSION;
        final File versionFile = getChildLocation(versionDir, Paths.get(versionExtension));
        if (versionFile.exists()) {
            throw new FlowPersistenceException("Unable to save, a snapshot already exists with version " + versionString);
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Saving snapshot with filename {}", versionFile.getAbsolutePath());
        }

        try (final OutputStream out = new FileOutputStream(versionFile)) {
            out.write(content);
            out.flush();
        } catch (Exception e) {
            throw new FlowPersistenceException("Unable to write snapshot to disk", e);
        }
    }

    @Override
    public synchronized byte[] getFlowContent(final String bucketId, final String flowId, final int version) throws FlowPersistenceException {
        final File snapshotFile = getSnapshotFile(bucketId, flowId, version);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Retrieving snapshot with filename {}", snapshotFile.getAbsolutePath());
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
        final Path bucketIdPath = getNormalizedIdPath(bucketId);
        final Path flowIdPath = getNormalizedIdPath(flowId);
        final Path bucketFlowPath = bucketIdPath.resolve(flowIdPath);
        final File flowDir = getChildLocation(flowStorageDir, bucketFlowPath);
        if (!flowDir.exists()) {
            LOGGER.debug("Snapshot directory does not exist at {}", flowDir.getAbsolutePath());
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
        final File bucketDir = getChildLocation(flowStorageDir, getNormalizedIdPath(bucketId));
        final File[] bucketFiles = bucketDir.listFiles();
        if (bucketFiles == null || bucketFiles.length == 0) {
            final boolean deletedBucket = bucketDir.delete();
            if (!deletedBucket) {
                LOGGER.error("Unable to delete bucket directory: {}", flowDir.getAbsolutePath());
            }
        }
    }

    @Override
    public synchronized void deleteFlowContent(final String bucketId, final String flowId, final int version) throws FlowPersistenceException {
        final File snapshotFile = getSnapshotFile(bucketId, flowId, version);
        if (!snapshotFile.exists()) {
            LOGGER.debug("Snapshot file does not exist at {}", snapshotFile.getAbsolutePath());
            return;
        }

        final boolean deleted = snapshotFile.delete();
        if (!deleted) {
            throw new FlowPersistenceException("Unable to delete snapshot at " + snapshotFile.getAbsolutePath());
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Deleted snapshot at {}", snapshotFile.getAbsolutePath());
        }
    }

    protected File getSnapshotFile(final String bucketId, final String flowId, final int version) {
        final String versionExtension = version + SNAPSHOT_EXTENSION;
        final Path snapshotLocation = Paths.get(getNormalizedId(bucketId), getNormalizedId(flowId), Integer.toString(version), versionExtension);
        return getChildLocation(flowStorageDir, snapshotLocation);
    }

    private File getChildLocation(final File parentDir, final Path childLocation) {
        final Path parentPath = parentDir.toPath().normalize();
        final Path childPathNormalized = childLocation.normalize();
        final Path childPath = parentPath.resolve(childPathNormalized);
        if (childPath.startsWith(parentPath)) {
            return childPath.toFile();
        }
        throw new IllegalArgumentException(String.format("Child location not valid [%s]", childLocation));
    }

    private Path getNormalizedIdPath(final String id) {
        final String normalizedId = getNormalizedId(id);
        return Paths.get(normalizedId).normalize();
    }

    private String getNormalizedId(final String input) {
        final String sanitized = FileUtils.sanitizeFilename(input).trim().toLowerCase();
        final Matcher matcher = NUMBER_PATTERN.matcher(sanitized);
        if (matcher.matches()) {
            final int normalized = Integer.parseInt(sanitized);
            return Integer.toString(normalized);
        } else {
            final UUID normalized = UUID.fromString(input);
            return normalized.toString();
        }
    }
}
