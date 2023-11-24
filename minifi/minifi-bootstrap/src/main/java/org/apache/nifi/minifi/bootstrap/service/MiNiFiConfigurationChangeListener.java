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

package org.apache.nifi.minifi.bootstrap.service;

import static java.nio.ByteBuffer.wrap;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.io.IOUtils.closeQuietly;
import static org.apache.commons.io.IOUtils.toByteArray;
import static org.apache.nifi.minifi.commons.api.MiNiFiConstants.BACKUP_EXTENSION;
import static org.apache.nifi.minifi.commons.api.MiNiFiConstants.RAW_EXTENSION;
import static org.apache.nifi.minifi.commons.util.FlowUpdateUtils.backup;
import static org.apache.nifi.minifi.commons.util.FlowUpdateUtils.persist;
import static org.apache.nifi.minifi.commons.util.FlowUpdateUtils.removeIfExists;
import static org.apache.nifi.minifi.commons.util.FlowUpdateUtils.revert;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.io.FilenameUtils;
import org.apache.nifi.minifi.bootstrap.RunMiNiFi;
import org.apache.nifi.minifi.bootstrap.configuration.ConfigurationChangeException;
import org.apache.nifi.minifi.bootstrap.configuration.ConfigurationChangeListener;
import org.apache.nifi.minifi.commons.api.MiNiFiProperties;
import org.apache.nifi.minifi.commons.service.FlowEnrichService;
import org.slf4j.Logger;

public class MiNiFiConfigurationChangeListener implements ConfigurationChangeListener {

    private static final ReentrantLock handlingLock = new ReentrantLock();

    private final RunMiNiFi runner;
    private final Logger logger;
    private final BootstrapFileProvider bootstrapFileProvider;
    private final FlowEnrichService flowEnrichService;

    public MiNiFiConfigurationChangeListener(RunMiNiFi runner, Logger logger, BootstrapFileProvider bootstrapFileProvider, FlowEnrichService flowEnrichService) {
        this.runner = runner;
        this.logger = logger;
        this.bootstrapFileProvider = bootstrapFileProvider;
        this.flowEnrichService = flowEnrichService;
    }

    @Override
    public void handleChange(InputStream flowConfigInputStream) throws ConfigurationChangeException {
        logger.info("Received notification of a change");

        if (!handlingLock.tryLock()) {
            throw new ConfigurationChangeException("Instance is already handling another change");
        }

        Path currentFlowConfigFile = null;
        Path backupFlowConfigFile = null;
        Path currentRawFlowConfigFile = null;
        Path backupRawFlowConfigFile = null;
        try {
            Properties bootstrapProperties = bootstrapFileProvider.getBootstrapProperties();

            currentFlowConfigFile = Path.of(bootstrapProperties.getProperty(MiNiFiProperties.NIFI_MINIFI_FLOW_CONFIG.getKey())).toAbsolutePath();
            backupFlowConfigFile = Path.of(currentFlowConfigFile + BACKUP_EXTENSION);
            String currentFlowConfigFileBaseName = FilenameUtils.getBaseName(currentFlowConfigFile.toString());
            currentRawFlowConfigFile = currentFlowConfigFile.getParent().resolve(currentFlowConfigFileBaseName + RAW_EXTENSION);
            backupRawFlowConfigFile = currentFlowConfigFile.getParent().resolve(currentFlowConfigFileBaseName + RAW_EXTENSION + BACKUP_EXTENSION);

            backup(currentFlowConfigFile, backupFlowConfigFile);
            backup(currentRawFlowConfigFile, backupRawFlowConfigFile);

            byte[] rawFlow = toByteArray(flowConfigInputStream);
            byte[] enrichedFlow = flowEnrichService.enrichFlow(rawFlow);
            persist(enrichedFlow, currentFlowConfigFile, true);
            restartInstance();
            persist(rawFlow, currentRawFlowConfigFile, false);
            setActiveFlowReference(wrap(rawFlow));
            logger.info("MiNiFi has finished reloading successfully and applied the new flow configuration");
        } catch (Exception e) {
            logger.error("Configuration update failed. Reverting to previous flow", e);
            revert(backupFlowConfigFile, currentFlowConfigFile);
            revert(backupRawFlowConfigFile, currentRawFlowConfigFile);
            throw new ConfigurationChangeException("Unable to perform reload of received configuration change", e);
        } finally {
            removeIfExists(backupFlowConfigFile);
            removeIfExists(backupRawFlowConfigFile);
            closeQuietly(flowConfigInputStream);
            handlingLock.unlock();
        }
    }

    @Override
    public String getDescriptor() {
        return "MiNiFiConfigurationChangeListener";
    }

    private void setActiveFlowReference(ByteBuffer flowConfig) {
        logger.debug("Setting active flow reference {} with content:\n{}", flowConfig, new String(flowConfig.array(), UTF_8));
        runner.getConfigFileReference().set(flowConfig);
    }

    private void restartInstance() throws IOException {
        try {
            runner.reload();
        } catch (IOException e) {
            throw new IOException("Unable to successfully restart MiNiFi instance after configuration change.", e);
        }
    }
}
