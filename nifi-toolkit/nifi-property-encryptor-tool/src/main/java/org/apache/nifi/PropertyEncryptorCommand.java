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
package org.apache.nifi;

import org.apache.nifi.properties.AbstractBootstrapPropertiesLoader;
import org.apache.nifi.properties.BootstrapProperties;
import org.apache.nifi.properties.MutableBootstrapProperties;
import org.apache.nifi.properties.SensitivePropertyProviderFactory;
import org.apache.nifi.properties.StandardSensitivePropertyProviderFactory;
import org.apache.nifi.properties.scheme.PropertyProtectionScheme;
import org.apache.nifi.properties.scheme.ProtectionScheme;
import org.apache.nifi.registry.properties.util.NiFiRegistryBootstrapPropertiesLoader;
import org.apache.nifi.security.util.KeyDerivationFunction;
import org.apache.nifi.security.util.crypto.SecureHasherFactory;
import org.apache.nifi.serde.StandardPropertiesWriter;
import org.apache.nifi.util.NiFiBootstrapPropertiesLoader;
import org.apache.nifi.util.file.ConfigurationFileResolver;
import org.apache.nifi.util.file.NiFiConfigurationFileResolver;
import org.apache.nifi.util.file.NiFiRegistryConfigurationFileResolver;
import org.apache.nifi.xml.XmlDecryptor;
import org.apache.nifi.xml.XmlEncryptor;
import org.apache.nifi.util.file.ConfigurationFileUtils;
import org.apache.nifi.properties.ApplicationProperties;
import org.apache.nifi.properties.NiFiPropertiesLoader;
import org.apache.nifi.properties.PropertiesLoader;
import org.apache.nifi.registry.properties.NiFiRegistryPropertiesLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.List;

public class PropertyEncryptorCommand {

    private static final Logger logger = LoggerFactory.getLogger(PropertyEncryptorCommand.class);
    private final AbstractBootstrapPropertiesLoader bootstrapLoader;
    private final PropertiesLoader<ApplicationProperties> propertiesLoader;
    private final ConfigurationFileResolver fileResolver;
    private final List<File> configurationFiles;
    private String hexKey;
    private final Path confDirectory;
    private final static String TEMP_FILE_PREFIX = "tmp";

    public PropertyEncryptorCommand(final Path baseDirectory, final String passphrase) throws PropertyEncryptorException {
        confDirectory = ConfigurationFileUtils.resolveAbsoluteConfDirectory(baseDirectory);
        try {
            bootstrapLoader = getBootstrapPropertiesLoader(confDirectory);
            fileResolver = getConfigurationFileResolver(confDirectory);
            final File applicationProperties = ConfigurationFileUtils.resolvePropertiesFile(confDirectory);
            propertiesLoader = getPropertiesLoader(confDirectory);
            configurationFiles = fileResolver.resolveConfigurationFilesFromApplicationProperties(propertiesLoader.load(applicationProperties));
            hexKey = getEncodedRootKey(confDirectory, passphrase);
        } catch (final Exception e) {
            throw new PropertyEncryptorException("Failed to run property encryptor", e);
        }
    }

    /**
     * @param baseDirectory The base directory of a NiFi / NiFi Registry installation that should be encrypted
     */
    public void encryptConfigurationFiles(final Path baseDirectory, final PropertyProtectionScheme scheme) {
        XmlEncryptor encryptor = getXmlEncryptor(scheme);
        try {
            encryptConfigurationFiles(configurationFiles, encryptor);
            outputKeyToBootstrap();
            logger.info("The Property Encryptor successfully encrypted configuration files in the [{}] directory with the scheme [{}]", baseDirectory, scheme.getPath());
        } catch (Exception e) {
            logger.error("The Property Encryptor failed to encrypt configuration files in the [{}] directory with the scheme [{}]", baseDirectory, scheme.getPath(), e);
        }
    }

    private void outputKeyToBootstrap() throws IOException {
        final File bootstrapFile = bootstrapLoader.getBootstrapFileWithinConfDirectory(confDirectory);
        File tempBootstrapFile = ConfigurationFileUtils.getTemporaryOutputFile(TEMP_FILE_PREFIX, bootstrapFile);
        final MutableBootstrapProperties bootstrapProperties = bootstrapLoader.loadMutableBootstrapProperties(bootstrapFile.getPath());
        bootstrapProperties.setProperty(BootstrapProperties.BootstrapPropertyKey.SENSITIVE_KEY.getKey(), hexKey);
        try (InputStream inputStream = new FileInputStream(bootstrapFile);
             FileOutputStream outputStream = new FileOutputStream(tempBootstrapFile)) {
            new StandardPropertiesWriter().writePropertiesFile(inputStream, outputStream, bootstrapProperties);
            logger.info("Output the bootstrap key to {}", tempBootstrapFile);
        }
    }

    private void encryptConfigurationFiles(final List<File> configurationFiles, final XmlEncryptor encryptor) throws IOException {
        for (final File configurationFile : configurationFiles) {
            File temp = ConfigurationFileUtils.getTemporaryOutputFile(TEMP_FILE_PREFIX, configurationFile);
            try (InputStream inputStream = new FileInputStream(configurationFile);
                FileOutputStream outputStream = new FileOutputStream(temp)) {
                encryptor.encrypt(inputStream, outputStream);
                logger.info("Successfully encrypted [{}]", configurationFile.getAbsolutePath());
            } catch (Exception e) {
                throw new PropertyEncryptorException(String.format("Failed to encrypt configuration file: [%s]", configurationFile.getAbsolutePath()), e);
            }

            //Files.copy(temp.toPath(), configurationFile.toPath());
        }
    }

    public void migrateConfigurationFiles(final File baseDirectory) {
        logger.info("Not yet implemented.");
    }

    public void encryptFlowDefinition(final File baseDirectory) {
        logger.info("Not yet implemented.");
    }

    private XmlEncryptor getXmlEncryptor(final PropertyProtectionScheme scheme) {
        final SensitivePropertyProviderFactory providerFactory = StandardSensitivePropertyProviderFactory.withKey(hexKey);
        return new XmlEncryptor(providerFactory, scheme);
    }

    private XmlDecryptor getXmlDecryptor(final SensitivePropertyProviderFactory providerFactory, final ProtectionScheme scheme) {
        return new XmlDecryptor(providerFactory, scheme);
    }

    private String getEncodedRootKey(final Path confDirectory, final String passphrase) {
        String encodedRootKey;

        try {
            final File bootstrapConf = bootstrapLoader.getBootstrapFileWithinConfDirectory(confDirectory);
            encodedRootKey = bootstrapLoader.extractKeyFromBootstrapFile(bootstrapConf.getAbsolutePath());
        } catch (IOException e) {
            throw new PropertyEncryptorException("Failed to get key hex from bootstrap file", e);
        }

        if (encodedRootKey.isEmpty()) {
            try {
                encodedRootKey = SecureHasherFactory.getSecureHasher(KeyDerivationFunction.SCRYPT.getKdfName()).hashHex(passphrase).toUpperCase();
            } catch (Exception e) {
                throw new PropertyEncryptorException("Failed to derive an encryption key from the provided passphrase", e);
            }
        }

        return encodedRootKey;
    }

    private AbstractBootstrapPropertiesLoader getBootstrapPropertiesLoader(final Path baseDirectory) {
        if (ConfigurationFileUtils.isNiFiRegistryConfDirectory(baseDirectory)) {
            return new NiFiRegistryBootstrapPropertiesLoader();
        } else if (ConfigurationFileUtils.isNiFiConfDirectory(baseDirectory)) {
            return new NiFiBootstrapPropertiesLoader();
        } else {
            throw new PropertyEncryptorException(String.format("The base directory [%s] does not contain a recognized bootstrap.conf file", baseDirectory));
        }
    }

    private PropertiesLoader<ApplicationProperties> getPropertiesLoader(final Path baseDirectory) {
        if (ConfigurationFileUtils.isNiFiRegistryConfDirectory(baseDirectory)) {
            return new NiFiRegistryPropertiesLoader();
        } else if (ConfigurationFileUtils.isNiFiConfDirectory(baseDirectory)) {
            return new NiFiPropertiesLoader();
        } else {
            throw new PropertyEncryptorException(String.format("The base directory [%s] does not contain a recognized .properties file", baseDirectory));
        }
    }

    private ConfigurationFileResolver getConfigurationFileResolver(final Path baseDirectory) {
        if (ConfigurationFileUtils.isNiFiRegistryConfDirectory(baseDirectory)) {
            return new NiFiRegistryConfigurationFileResolver(baseDirectory);
        } else if (ConfigurationFileUtils.isNiFiConfDirectory(baseDirectory)) {
            return new NiFiConfigurationFileResolver(baseDirectory);
        } else {
            throw new PropertyEncryptorException(String.format("The base directory [%s] does not contain a recognized .properties file", baseDirectory));
        }
    }
}