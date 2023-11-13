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
package org.apache.nifi.persistence;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.nifi.cluster.protocol.DataFlow;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.MissingBundleException;
import org.apache.nifi.controller.UninheritableFlowException;
import org.apache.nifi.controller.serialization.FlowSerializationException;
import org.apache.nifi.controller.serialization.FlowSerializer;
import org.apache.nifi.controller.serialization.FlowSynchronizationException;
import org.apache.nifi.controller.serialization.FlowSynchronizer;
import org.apache.nifi.controller.serialization.VersionedFlowSerializer;
import org.apache.nifi.controller.serialization.VersionedFlowSynchronizer;
import org.apache.nifi.groups.BundleUpdateStrategy;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.services.FlowService;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.file.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class StandardFlowConfigurationDAO implements FlowConfigurationDAO {
    private static final Logger LOG = LoggerFactory.getLogger(StandardFlowConfigurationDAO.class);
    private static final String CLUSTER_FLOW_SERIALIZATION_FORMAT = "nifi.cluster.flow.serialization.format";
    private static final String FLOW_SERIALIZATION_FORMAT_JSON = "JSON";

    private final File jsonFile;
    private final FlowConfigurationArchiveManager archiveManager;
    private final NiFiProperties nifiProperties;
    private final ExtensionManager extensionManager;

    private volatile boolean jsonFileExists;
    private final String clusterFlowSerializationFormat;

    public StandardFlowConfigurationDAO(final NiFiProperties nifiProperties, final ExtensionManager extensionManager) throws IOException {
        this.nifiProperties = nifiProperties;
        this.clusterFlowSerializationFormat = nifiProperties.getProperty(CLUSTER_FLOW_SERIALIZATION_FORMAT, FLOW_SERIALIZATION_FORMAT_JSON);

        jsonFile = nifiProperties.getFlowConfigurationFile();

        jsonFileExists = jsonFile.length() > 0L;

        if (!jsonFile.exists()) {
            // createDirectories would throw an exception if the directory exists but is a symbolic link
            if (!jsonFile.getParentFile().exists()) {
                Files.createDirectories(jsonFile.getParentFile().toPath());
            }
        } else if (!jsonFile.canRead() || !jsonFile.canWrite()) {
            throw new IOException(jsonFile + " exists but you have insufficient read/write privileges");
        }

        this.extensionManager = extensionManager;

        this.archiveManager = new FlowConfigurationArchiveManager(nifiProperties);
    }

    @Override
    public boolean isFlowPresent() {
        return getReadableFile() != null;
    }

    @Override
    public synchronized void load(final FlowController controller, final DataFlow dataFlow, final FlowService flowService, final BundleUpdateStrategy bundleUpdateStrategy)
            throws IOException, FlowSerializationException, FlowSynchronizationException, UninheritableFlowException, MissingBundleException {

        final FlowSynchronizer standardFlowSynchronizer = new VersionedFlowSynchronizer(extensionManager, nifiProperties.getFlowConfigurationFile(), archiveManager);
        controller.synchronize(standardFlowSynchronizer, dataFlow, flowService, bundleUpdateStrategy);

        if (VersionedFlowSynchronizer.isFlowEmpty(dataFlow)) {
            // If the dataflow is empty, we want to save it. We do this because when we start up a brand new cluster with no
            // dataflow, we need to ensure that the flow is consistent across all nodes in the cluster and that upon restart
            // of NiFi, the root group ID does not change. However, we don't always want to save it, because if the flow is
            // not empty, then we can get into a bad situation, since the Processors, etc. don't have the appropriate "Scheduled
            // State" yet (since they haven't yet been scheduled). So if there are components in the flow and we save it, we
            // may end up saving the flow in such a way that all components are stopped.
            // We save based on the controller, not the provided data flow.
            save(controller, true);
        }
    }

    private File getReadableFile() {
        return  jsonFileExists ? jsonFile : null;
    }

    @Override
    public synchronized void load(final OutputStream os) throws IOException {
        final File file = getReadableFile();
        if (file == null) {
            return;
        }

        try (final InputStream inStream = new FileInputStream(file);
             final InputStream gzipIn = new GZIPInputStream(inStream)) {
            FileUtils.copy(gzipIn, os);
        }
    }

    @Override
    public void load(final OutputStream os, final boolean compressed) throws IOException {
        final File file = getReadableFile();
        if (file == null) {
            return;
        }

        if (compressed) {
            Files.copy(file.toPath(), os);
        } else {
            load(os);
        }
    }

    @Override
    public void save(final FlowController controller) throws IOException {
        LOG.trace("Saving flow to disk");
        save(controller, true);
        jsonFileExists = true;
        LOG.debug("Finished saving flow to disk");
    }

    @Override
    public synchronized void save(final FlowController controller, final OutputStream os) throws IOException {
        try {
            // Serialize based on the serialization format configured for cluster communications. If not configured, use JSON.
            final FlowSerializer<?> serializer;
            if (FLOW_SERIALIZATION_FORMAT_JSON.equalsIgnoreCase(clusterFlowSerializationFormat)) {
                serializer = new VersionedFlowSerializer(extensionManager);
            } else {
                throw new IllegalArgumentException("Unknown serialization format");
            }

            controller.serialize(serializer, os);
        } catch (final FlowSerializationException fse) {
            throw new IOException(fse);
        }
    }

    @Override
    public synchronized void save(final FlowController controller, final boolean archive) throws IOException {
        if (null == controller) {
            throw new NullPointerException();
        }

        saveJson(controller, archive);

    }

    private void saveJson(final FlowController controller, final boolean archive) throws IOException {
        final FlowSerializer<?> serializer = new VersionedFlowSerializer(controller.getExtensionManager());
        saveFlow(controller, serializer, jsonFile, archive);
        jsonFileExists = true;
    }

    private void saveFlow(final FlowController controller, final FlowSerializer<?> serializer, final File file, final boolean archive) throws IOException {
        final File tempFile = new File(file.getParentFile(), file.getName() + ".temp.gz");

        try {
            serializeControllerStateToTempFile(controller, serializer, tempFile);
            Files.deleteIfExists(file.toPath());
            FileUtils.renameFile(tempFile, file, 5, true);
        } catch (final FlowSerializationException fse) {
            throw new IOException(fse);
        } finally {
            Files.deleteIfExists(tempFile.toPath());
        }

        if (archive) {
            try {
                archiveManager.archive(file);
            } catch (final Exception ex) {
                LOG.error("Unable to archive flow configuration as requested due to " + ex);
                if (LOG.isDebugEnabled()) {
                    LOG.error("", ex);
                }
            }
        }
    }

    private void serializeControllerStateToTempFile(FlowController controller, FlowSerializer<?> serializer, File tempFile) throws IOException {
        try (final OutputStream fileOut = new FileOutputStream(tempFile);
             final OutputStream outStream = new GZIPOutputStream(fileOut)) {

            controller.serialize(serializer, outStream);
        }
    }
}
