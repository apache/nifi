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

import org.apache.nifi.cluster.protocol.DataFlow;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.MissingBundleException;
import org.apache.nifi.controller.StandardFlowSynchronizer;
import org.apache.nifi.controller.UninheritableFlowException;
import org.apache.nifi.controller.YAMLFlowSynchronizer;
import org.apache.nifi.controller.serialization.FlowSerializationException;
import org.apache.nifi.controller.serialization.FlowSynchronizationException;
import org.apache.nifi.controller.serialization.FlowSynchronizer;
import org.apache.nifi.encrypt.StringEncryptor;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.file.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.zip.GZIPInputStream;

public final class MiNiFiYAMLFlowConfigurationDAO implements FlowConfigurationDAO {

    private final Path configYamlPath;
    private final StringEncryptor encryptor;
    private final FlowConfigurationArchiveManager archiveManager;
    private final NiFiProperties nifiProperties;
    private final ExtensionManager extensionManager;

    private static final Logger LOG = LoggerFactory.getLogger(MiNiFiYAMLFlowConfigurationDAO.class);

    public MiNiFiYAMLFlowConfigurationDAO(final Path yamlFile, final StringEncryptor encryptor, final NiFiProperties nifiProperties,
                                          final ExtensionManager extensionManager) throws IOException {
        this.nifiProperties = nifiProperties;
        final File flowYamlFile = yamlFile.toFile();

        if (!yamlFile.toFile().exists()) {
            // createDirectories would throw an exception if the directory exists but is a symbolic link
            if (Files.notExists(yamlFile.getParent())) {
                Files.createDirectories(yamlFile.getParent());
            }
            Files.createFile(yamlFile);
            //TODO: find a better solution. With Windows 7 and Java 7, Files.isWritable(source.getParent()) returns false, even when it should be true.
        } else if (!flowYamlFile.canRead() || !flowYamlFile.canWrite()) {
            throw new IOException(yamlFile + " exists but you have insufficient read/write privileges");
        }

        this.configYamlPath = yamlFile;
        this.encryptor = encryptor;
        this.extensionManager = extensionManager;

        this.archiveManager = null;
    }

    @Override
    public boolean isFlowPresent() {
        final File configYaml = configYamlPath.toFile();
        return configYaml.exists() && configYaml.length() > 0;
    }

    @Override
    public synchronized void load(final FlowController controller, final DataFlow dataFlow)
            throws IOException, FlowSerializationException, FlowSynchronizationException, UninheritableFlowException, MissingBundleException {

        final FlowSynchronizer flowSynchronizer = new YAMLFlowSynchronizer(encryptor, nifiProperties, extensionManager);
        controller.synchronize(flowSynchronizer, dataFlow);

        if (YAMLFlowSynchronizer.isEmpty(dataFlow)) {
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

        try (final InputStream inStream = Files.newInputStream(configYamlPath, StandardOpenOption.READ)) {
            FileUtils.copy(inStream, os);
        }
    }

    @Override
    public void load(final OutputStream os, final boolean compressed) throws IOException {
        load(os);
    }

    @Override
    public synchronized void save(final InputStream is) throws IOException {
    }

    @Override
    public void save(final FlowController flow) throws IOException {
    }

    @Override
    public synchronized void save(final FlowController flow, final OutputStream os) throws IOException {
    }

    @Override
    public synchronized void save(final FlowController controller, final boolean archive) throws IOException {
    }

}
