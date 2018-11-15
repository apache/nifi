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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.nifi.cluster.protocol.DataFlow;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.MissingBundleException;
import org.apache.nifi.controller.StandardFlowSynchronizer;
import org.apache.nifi.controller.UninheritableFlowException;
import org.apache.nifi.controller.serialization.FlowSerializationException;
import org.apache.nifi.controller.serialization.FlowSynchronizationException;
import org.apache.nifi.controller.serialization.FlowSynchronizer;
import org.apache.nifi.controller.serialization.StandardFlowSerializer;
import org.apache.nifi.encrypt.StringEncryptor;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.file.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class StandardXMLFlowConfigurationDAO implements FlowConfigurationDAO {

    private final Path flowXmlPath;
    private final StringEncryptor encryptor;
    private final FlowConfigurationArchiveManager archiveManager;
    private final NiFiProperties nifiProperties;
    private final ExtensionManager extensionManager;

    private static final Logger LOG = LoggerFactory.getLogger(StandardXMLFlowConfigurationDAO.class);

    public StandardXMLFlowConfigurationDAO(final Path flowXml, final StringEncryptor encryptor, final NiFiProperties nifiProperties,
                                           final ExtensionManager extensionManager) throws IOException {
        this.nifiProperties = nifiProperties;
        final File flowXmlFile = flowXml.toFile();
        if (!flowXmlFile.exists()) {
            // createDirectories would throw an exception if the directory exists but is a symbolic link
            if (Files.notExists(flowXml.getParent())) {
                Files.createDirectories(flowXml.getParent());
            }
            Files.createFile(flowXml);
            //TODO: find a better solution. With Windows 7 and Java 7, Files.isWritable(source.getParent()) returns false, even when it should be true.
        } else if (!flowXmlFile.canRead() || !flowXmlFile.canWrite()) {
            throw new IOException(flowXml + " exists but you have insufficient read/write privileges");
        }

        this.flowXmlPath = flowXml;
        this.encryptor = encryptor;
        this.extensionManager = extensionManager;

        this.archiveManager = new FlowConfigurationArchiveManager(flowXmlPath, nifiProperties);
    }

    @Override
    public boolean isFlowPresent() {
        final File flowXmlFile = flowXmlPath.toFile();
        return flowXmlFile.exists() && flowXmlFile.length() > 0;
    }

    @Override
    public synchronized void load(final FlowController controller, final DataFlow dataFlow)
            throws IOException, FlowSerializationException, FlowSynchronizationException, UninheritableFlowException, MissingBundleException {

        final FlowSynchronizer flowSynchronizer = new StandardFlowSynchronizer(encryptor, nifiProperties, extensionManager);
        controller.synchronize(flowSynchronizer, dataFlow);

        if (StandardFlowSynchronizer.isEmpty(dataFlow)) {
            // If the dataflow is empty, we want to save it. We do this because when we start up a brand new cluster with no
            // dataflow, we need to ensure that the flow is consistent across all nodes in the cluster and that upon restart
            // of NiFi, the root group ID does not change. However, we don't always want to save it, because if the flow is
            // not empty, then we can get into a bad situation, since the Processors, etc. don't have the appropriate "Scheduled
            // State" yet (since they haven't yet been scheduled). So if there are components in the flow and we save it, we
            // may end up saving the flow in such a way that all components are stopped.
            // We save based on the controller, not the provided data flow because Process Groups may contain 'local' templates.
            save(controller);
        }
    }

    @Override
    public synchronized void load(final OutputStream os) throws IOException {
        if (!isFlowPresent()) {
            return;
        }

        try (final InputStream inStream = Files.newInputStream(flowXmlPath, StandardOpenOption.READ);
                final InputStream gzipIn = new GZIPInputStream(inStream)) {
            FileUtils.copy(gzipIn, os);
        }
    }

    @Override
    public void load(final OutputStream os, final boolean compressed) throws IOException {
        if (compressed) {
            Files.copy(flowXmlPath, os);
        } else {
            load(os);
        }
    }

    @Override
    public synchronized void save(final InputStream is) throws IOException {
        try (final OutputStream outStream = Files.newOutputStream(flowXmlPath, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
                final OutputStream gzipOut = new GZIPOutputStream(outStream)) {
            FileUtils.copy(is, gzipOut);
        }
    }

    @Override
    public void save(final FlowController flow) throws IOException {
        LOG.trace("Saving flow to disk");
        try (final OutputStream outStream = Files.newOutputStream(flowXmlPath, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
                final OutputStream gzipOut = new GZIPOutputStream(outStream)) {
            save(flow, gzipOut);
        }
        LOG.debug("Finished saving flow to disk");
    }

    @Override
    public synchronized void save(final FlowController flow, final OutputStream os) throws IOException {
        try {
            final StandardFlowSerializer xmlTransformer = new StandardFlowSerializer(encryptor);
            flow.serialize(xmlTransformer, os);
        } catch (final FlowSerializationException fse) {
            throw new IOException(fse);
        }
    }

    @Override
    public synchronized void save(final FlowController controller, final boolean archive) throws IOException {
        if (null == controller) {
            throw new NullPointerException();
        }

        Path tempFile;
        Path configFile;

        configFile = flowXmlPath;
        tempFile = configFile.getParent().resolve(configFile.toFile().getName() + ".new.xml.gz");

        try (final OutputStream fileOut = Files.newOutputStream(tempFile);
                final OutputStream outStream = new GZIPOutputStream(fileOut)) {

            final StandardFlowSerializer xmlTransformer = new StandardFlowSerializer(encryptor);
            controller.serialize(xmlTransformer, outStream);

            Files.deleteIfExists(configFile);
            FileUtils.renameFile(tempFile.toFile(), configFile.toFile(), 5, true);
        } catch (final FlowSerializationException fse) {
            throw new IOException(fse);
        } finally {
            Files.deleteIfExists(tempFile);
        }

        if (archive) {
            try {
                archiveManager.archive();
            } catch (final Exception ex) {
                LOG.error("Unable to archive flow configuration as requested due to " + ex);
                if (LOG.isDebugEnabled()) {
                    LOG.error("", ex);
                }
            }
        }
    }

}
