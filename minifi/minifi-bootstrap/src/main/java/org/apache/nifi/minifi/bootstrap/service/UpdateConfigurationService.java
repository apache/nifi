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

import static java.util.Optional.ofNullable;
import static org.apache.nifi.minifi.commons.api.MiNiFiConstants.CONFIG_UPDATED_FILE_NAME;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Optional;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.minifi.bootstrap.RunMiNiFi;
import org.apache.nifi.minifi.bootstrap.configuration.ConfigurationChangeListener;
import org.apache.nifi.minifi.bootstrap.configuration.differentiators.Differentiator;
import org.apache.nifi.minifi.bootstrap.configuration.differentiators.WholeConfigDifferentiator;
import org.apache.nifi.minifi.bootstrap.util.ByteBufferInputStream;
import org.apache.nifi.minifi.bootstrap.util.ConfigTransformer;
import org.apache.nifi.minifi.commons.api.MiNiFiCommandState;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpdateConfigurationService {

    private static final Logger logger = LoggerFactory.getLogger(UpdateConfigurationService.class);
    private static final String FALLBACK_CONFIG_FILE_DIR = "./conf/";

    private final Differentiator<ByteBuffer> differentiator;
    private final RunMiNiFi runMiNiFi;
    private final ConfigurationChangeListener miNiFiConfigurationChangeListener;
    private final BootstrapFileProvider bootstrapFileProvider;

    public UpdateConfigurationService(RunMiNiFi runMiNiFi, ConfigurationChangeListener miNiFiConfigurationChangeListener, BootstrapFileProvider bootstrapFileProvider) {
        this.differentiator = WholeConfigDifferentiator.getByteBufferDifferentiator();
        this.differentiator.initialize(runMiNiFi);
        this.runMiNiFi = runMiNiFi;
        this.miNiFiConfigurationChangeListener = miNiFiConfigurationChangeListener;
        this.bootstrapFileProvider = bootstrapFileProvider;
    }

    public Optional<MiNiFiCommandState> handleUpdate() {
        logger.info("Handling configuration update");
        MiNiFiCommandState commandState = null;
        try (FileInputStream configFile = new FileInputStream(getConfigFilePath().toFile())) {
            ByteBuffer readOnlyNewConfig = ConfigTransformer.overrideNonFlowSectionsFromOriginalSchema(
                    IOUtils.toByteArray(configFile), runMiNiFi.getConfigFileReference().get().duplicate(), bootstrapFileProvider.getBootstrapProperties());
            if (differentiator.isNew(readOnlyNewConfig)) {
                miNiFiConfigurationChangeListener.handleChange(new ByteBufferInputStream(readOnlyNewConfig.duplicate()));
            } else {
                logger.info("The given configuration does not contain any update. No operation required");
                commandState = MiNiFiCommandState.NO_OPERATION;
            }
        } catch (Exception e) {
            commandState = MiNiFiCommandState.NOT_APPLIED_WITHOUT_RESTART;
            logger.error("Could not handle configuration update", e);
        }
        return Optional.ofNullable(commandState);
    }

    private Path getConfigFilePath() {
        return ofNullable(safeGetPropertiesFilePath())
            .map(File::new)
            .map(File::getParent)
            .map(parentDir -> new File(parentDir + CONFIG_UPDATED_FILE_NAME))
            .orElse(new File(FALLBACK_CONFIG_FILE_DIR + CONFIG_UPDATED_FILE_NAME)).toPath();
    }

    private String safeGetPropertiesFilePath() {
        String propertyFile = null;
        try {
            propertyFile = bootstrapFileProvider.getBootstrapProperties().getProperty(NiFiProperties.PROPERTIES_FILE_PATH, null);
        } catch (IOException e) {
            logger.error("Failed to get properties file path");
        }
        return propertyFile;
    }
}
