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

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.apache.nifi.minifi.bootstrap.RunMiNiFi.CONF_DIR_KEY;
import static org.apache.nifi.minifi.bootstrap.RunMiNiFi.MINIFI_CONFIG_FILE_KEY;
import static org.apache.nifi.minifi.bootstrap.configuration.ingestors.PullHttpChangeIngestor.OVERRIDE_SECURITY;
import static org.apache.nifi.minifi.bootstrap.configuration.ingestors.PullHttpChangeIngestor.PULL_HTTP_BASE_KEY;
import static org.apache.nifi.minifi.bootstrap.util.ConfigTransformer.generateConfigFiles;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Properties;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.minifi.bootstrap.RunMiNiFi;
import org.apache.nifi.minifi.bootstrap.configuration.ConfigurationChangeException;
import org.apache.nifi.minifi.bootstrap.configuration.ConfigurationChangeListener;
import org.apache.nifi.minifi.bootstrap.util.ByteBufferInputStream;
import org.apache.nifi.minifi.bootstrap.util.ConfigTransformer;
import org.apache.nifi.minifi.commons.schema.ConfigSchema;
import org.apache.nifi.minifi.commons.schema.common.ConvertableSchema;
import org.apache.nifi.minifi.commons.schema.serialization.SchemaLoader;
import org.slf4j.Logger;

public class MiNiFiConfigurationChangeListener implements ConfigurationChangeListener {

    private final RunMiNiFi runner;
    private final Logger logger;
    private final BootstrapFileProvider bootstrapFileProvider;

    private static final ReentrantLock handlingLock = new ReentrantLock();

    public MiNiFiConfigurationChangeListener(RunMiNiFi runner, Logger logger, BootstrapFileProvider bootstrapFileProvider) {
        this.runner = runner;
        this.logger = logger;
        this.bootstrapFileProvider = bootstrapFileProvider;
    }

    @Override
    public void handleChange(InputStream configInputStream) throws ConfigurationChangeException {
        logger.info("Received notification of a change");

        if (!handlingLock.tryLock()) {
            throw new ConfigurationChangeException("Instance is already handling another change");
        }
        // Store the incoming stream as a byte array to be shared among components that need it
        try(ByteArrayOutputStream bufferedConfigOs = new ByteArrayOutputStream()) {

            Properties bootstrapProperties = bootstrapFileProvider.getBootstrapProperties();
            File configFile = new File(bootstrapProperties.getProperty(MINIFI_CONFIG_FILE_KEY));

            IOUtils.copy(configInputStream, bufferedConfigOs);

            File swapConfigFile = bootstrapFileProvider.getSwapFile();
            logger.info("Persisting old configuration to {}", swapConfigFile.getAbsolutePath());

            try (FileInputStream configFileInputStream = new FileInputStream(configFile)) {
                Files.copy(configFileInputStream, swapConfigFile.toPath(), REPLACE_EXISTING);
            }

            persistBackNonFlowSectionsFromOriginalSchema(bufferedConfigOs.toByteArray(), bootstrapProperties, configFile);

            // Create an input stream to feed to the config transformer
            try (FileInputStream newConfigIs = new FileInputStream(configFile)) {

                try {
                    String confDir = bootstrapProperties.getProperty(CONF_DIR_KEY);
                    transformConfigurationFiles(confDir, newConfigIs, configFile, swapConfigFile);
                } catch (Exception e) {
                    logger.debug("Transformation of new config file failed after swap file was created, deleting it.");
                    if (!swapConfigFile.delete()) {
                        logger.warn("The swap file failed to delete after a failed handling of a change. It should be cleaned up manually.");
                    }
                    throw e;
                }
            }
        } catch (Exception e) {
            throw new ConfigurationChangeException("Unable to perform reload of received configuration change", e);
        } finally {
            IOUtils.closeQuietly(configInputStream);
            handlingLock.unlock();
        }
    }

    @Override
    public String getDescriptor() {
        return "MiNiFiConfigurationChangeListener";
    }

    private void transformConfigurationFiles(String confDir, FileInputStream newConfigIs, File configFile, File swapConfigFile) throws Exception {
        try {
            logger.info("Performing transformation for input and saving outputs to {}", confDir);
            ByteBuffer tempConfigFile = generateConfigFiles(newConfigIs, confDir, bootstrapFileProvider.getBootstrapProperties());
            runner.getConfigFileReference().set(tempConfigFile.asReadOnlyBuffer());
            reloadNewConfiguration(swapConfigFile, confDir);
        } catch (Exception e) {
            logger.debug("Transformation of new config file failed after replacing original with the swap file, reverting.");
            try (FileInputStream swapConfigFileStream = new FileInputStream(swapConfigFile)) {
                Files.copy(swapConfigFileStream, configFile.toPath(), REPLACE_EXISTING);
            }
            throw e;
        }
    }

    private void reloadNewConfiguration(File swapConfigFile, String confDir) throws Exception {
        try {
            logger.info("Reloading instance with new configuration");
            restartInstance();
        } catch (Exception e) {
            logger.debug("Transformation of new config file failed after transformation into Flow.xml and nifi.properties, reverting.");
            try (FileInputStream swapConfigFileStream = new FileInputStream(swapConfigFile)) {
                ByteBuffer resetConfigFile = generateConfigFiles(swapConfigFileStream, confDir, bootstrapFileProvider.getBootstrapProperties());
                runner.getConfigFileReference().set(resetConfigFile.asReadOnlyBuffer());
            }
            throw e;
        }
    }

    private void restartInstance() throws IOException {
        try {
            runner.reload();
        } catch (IOException e) {
            throw new IOException("Unable to successfully restart MiNiFi instance after configuration change.", e);
        }
    }

    private void persistBackNonFlowSectionsFromOriginalSchema(byte[] newSchema, Properties bootstrapProperties, File configFile) {
        try {
            ConvertableSchema<ConfigSchema> schemaNew = ConfigTransformer
                .throwIfInvalid(SchemaLoader.loadConvertableSchemaFromYaml(new ByteArrayInputStream(newSchema)));
            ConfigSchema configSchemaNew = ConfigTransformer.throwIfInvalid(schemaNew.convert());
            ConvertableSchema<ConfigSchema> schemaOld = ConfigTransformer
                .throwIfInvalid(SchemaLoader.loadConvertableSchemaFromYaml(new ByteBufferInputStream(runner.getConfigFileReference().get().duplicate())));
            ConfigSchema configSchemaOld = ConfigTransformer.throwIfInvalid(schemaOld.convert());

            configSchemaNew.setNifiPropertiesOverrides(configSchemaOld.getNifiPropertiesOverrides());

            if (!overrideCoreProperties(bootstrapProperties)) {
                logger.debug("Preserving previous core properties...");
                configSchemaNew.setCoreProperties(configSchemaOld.getCoreProperties());
            }

            if (!overrideSecurityProperties(bootstrapProperties)) {
                logger.debug("Preserving previous security properties...");
                configSchemaNew.setSecurityProperties(configSchemaOld.getSecurityProperties());
            }

            logger.debug("Persisting changes to {}", configFile.getAbsolutePath());
            SchemaLoader.toYaml(configSchemaNew, new FileWriter(configFile));
        } catch (Exception e) {
            logger.error("Loading the old and the new schema for merging was not successful", e);
        }
    }

    private static boolean overrideSecurityProperties(Properties properties) {
        String overrideSecurityProperties = (String) properties.getOrDefault(OVERRIDE_SECURITY, "false");
        boolean overrideSecurity;
        if ("true".equalsIgnoreCase(overrideSecurityProperties) || "false".equalsIgnoreCase(overrideSecurityProperties)) {
            overrideSecurity = Boolean.parseBoolean(overrideSecurityProperties);
        } else {
            throw new IllegalArgumentException(
                "Property, " + OVERRIDE_SECURITY + ", to specify whether to override security properties must either be a value boolean value (\"true\" or \"false\")" +
                " or left to the default value of \"false\". It is set to \"" + overrideSecurityProperties + "\".");
        }

        return overrideSecurity;
    }

    private static boolean overrideCoreProperties(Properties properties) {
        String overrideCorePropertiesKey = PULL_HTTP_BASE_KEY + ".override.core";
        String overrideCoreProps = (String) properties.getOrDefault(overrideCorePropertiesKey, "false");
        boolean overrideCoreProperties;
        if ("true".equalsIgnoreCase(overrideCoreProps) || "false".equalsIgnoreCase(overrideCoreProps)) {
            overrideCoreProperties = Boolean.parseBoolean(overrideCoreProps);
        } else {
            throw new IllegalArgumentException(
                "Property, " + overrideCorePropertiesKey + ", to specify whether to override core properties must either be a value boolean value (\"true\" or \"false\")" +
                " or left to the default value of \"false\". It is set to \"" + overrideCoreProps + "\".");
        }

        return overrideCoreProperties;
    }
}
