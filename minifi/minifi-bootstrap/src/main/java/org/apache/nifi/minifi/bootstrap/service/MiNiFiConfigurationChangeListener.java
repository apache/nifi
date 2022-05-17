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
import static org.apache.nifi.minifi.bootstrap.util.ConfigTransformer.generateConfigFiles;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Properties;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.nifi.minifi.bootstrap.RunMiNiFi;
import org.apache.nifi.minifi.bootstrap.configuration.ConfigurationChangeException;
import org.apache.nifi.minifi.bootstrap.configuration.ConfigurationChangeListener;
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

            byte[] copyArray = new byte[1024];
            int available;
            while ((available = configInputStream.read(copyArray)) > 0) {
                bufferedConfigOs.write(copyArray, 0, available);
            }

            // Create an input stream to use for writing a config file as well as feeding to the config transformer
            try (ByteArrayInputStream newConfigBais = new ByteArrayInputStream(bufferedConfigOs.toByteArray())) {
                newConfigBais.mark(-1);

                File swapConfigFile = bootstrapFileProvider.getSwapFile();
                logger.info("Persisting old configuration to {}", swapConfigFile.getAbsolutePath());

                try (FileInputStream configFileInputStream = new FileInputStream(configFile)) {
                    Files.copy(configFileInputStream, swapConfigFile.toPath(), REPLACE_EXISTING);
                }

                try {
                    logger.info("Persisting changes to {}", configFile.getAbsolutePath());
                    saveFile(newConfigBais, configFile);
                    String confDir = bootstrapProperties.getProperty(CONF_DIR_KEY);

                    try {
                        // Reset the input stream to provide to the transformer
                        newConfigBais.reset();

                        logger.info("Performing transformation for input and saving outputs to {}", confDir);
                        ByteBuffer tempConfigFile = generateConfigFiles(newConfigBais, confDir, bootstrapFileProvider.getBootstrapProperties());
                        runner.getConfigFileReference().set(tempConfigFile.asReadOnlyBuffer());

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
                    } catch (Exception e) {
                        logger.debug("Transformation of new config file failed after replacing original with the swap file, reverting.");
                        try (FileInputStream swapConfigFileStream = new FileInputStream(swapConfigFile)) {
                            Files.copy(swapConfigFileStream, configFile.toPath(), REPLACE_EXISTING);
                        }
                        throw e;
                    }
                } catch (Exception e) {
                    logger.debug("Transformation of new config file failed after swap file was created, deleting it.");
                    if (!swapConfigFile.delete()) {
                        logger.warn("The swap file failed to delete after a failed handling of a change. It should be cleaned up manually.");
                    }
                    throw e;
                }
            }
        } catch (ConfigurationChangeException e){
            logger.error("Unable to carry out reloading of configuration on receipt of notification event", e);
            throw e;
        } catch (IOException ioe) {
            logger.error("Unable to carry out reloading of configuration on receipt of notification event", ioe);
            throw new ConfigurationChangeException("Unable to perform reload of received configuration change", ioe);
        } finally {
            try {
                if (configInputStream != null) {
                    configInputStream.close() ;
                }
            } catch (IOException e) {
                // Quietly close
            }
            handlingLock.unlock();
        }
    }

    @Override
    public String getDescriptor() {
        return "MiNiFiConfigurationChangeListener";
    }

    private void saveFile(InputStream configInputStream, File configFile) throws IOException {
        try {
            try (FileOutputStream configFileOutputStream = new FileOutputStream(configFile)) {
                byte[] copyArray = new byte[1024];
                int available;
                while ((available = configInputStream.read(copyArray)) > 0) {
                    configFileOutputStream.write(copyArray, 0, available);
                }
            }
        } catch (IOException ioe) {
            throw new IOException("Unable to save updated configuration to the configured config file location", ioe);
        }
    }

    private void restartInstance() throws IOException {
        try {
            runner.reload();
        } catch (IOException e) {
            throw new IOException("Unable to successfully restart MiNiFi instance after configuration change.", e);
        }
    }
}
