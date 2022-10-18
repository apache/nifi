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

import org.apache.nifi.encrypt.PropertyEncryptionMethod;
import org.apache.nifi.encrypt.PropertyEncryptor;
import org.apache.nifi.encrypt.PropertyEncryptorBuilder;
import org.apache.nifi.flow.encryptor.StandardFlowEncryptor;
import org.apache.nifi.properties.AbstractBootstrapPropertiesLoader;
import org.apache.nifi.properties.ApplicationProperties;
import org.apache.nifi.properties.BootstrapProperties;
import org.apache.nifi.properties.MutableApplicationProperties;
import org.apache.nifi.properties.MutableBootstrapProperties;
import org.apache.nifi.properties.NiFiPropertiesLoader;
import org.apache.nifi.properties.PropertiesLoader;
import org.apache.nifi.properties.ProtectedPropertyContext;
import org.apache.nifi.properties.SensitivePropertyProvider;
import org.apache.nifi.properties.SensitivePropertyProviderFactory;
import org.apache.nifi.properties.StandardSensitivePropertyProviderFactory;
import org.apache.nifi.properties.scheme.ProtectionScheme;
import org.apache.nifi.registry.properties.NiFiRegistryPropertiesLoader;
import org.apache.nifi.registry.properties.util.NiFiRegistryBootstrapPropertiesLoader;
import org.apache.nifi.security.util.KeyDerivationFunction;
import org.apache.nifi.security.util.crypto.SecureHasherFactory;
import org.apache.nifi.serde.StandardPropertiesWriter;
import org.apache.nifi.util.NiFiBootstrapPropertiesLoader;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.file.ConfigurationFileResolver;
import org.apache.nifi.util.file.ConfigurationFileUtils;
import org.apache.nifi.util.file.NiFiConfigurationFileResolver;
import org.apache.nifi.util.file.NiFiFlowDefinitionFileResolver;
import org.apache.nifi.util.file.NiFiRegistryConfigurationFileResolver;
import org.apache.nifi.util.properties.NiFiRegistrySensitivePropertyResolver;
import org.apache.nifi.util.properties.NiFiSensitivePropertyResolver;
import org.apache.nifi.util.properties.SensitivePropertyResolver;
import org.apache.nifi.xml.XmlDecryptor;
import org.apache.nifi.xml.XmlEncryptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import java.util.zip.GZIPInputStream;

public class PropertyEncryptorCommand {

    private static final Logger logger = LoggerFactory.getLogger(PropertyEncryptorCommand.class);
    private final AbstractBootstrapPropertiesLoader bootstrapLoader;
    private final PropertiesLoader<ApplicationProperties> propertiesLoader;
    private final ConfigurationFileResolver fileResolver;
    private final SensitivePropertyResolver sensitivePropertyResolver;
    private final List<File> configurationFiles;
    private String inputHexKey;
    private String outputHexKey;
    private String inputSensitivePropertyKey;
    private String outputSensitivePropertyKey;
    private final Path confDirectory;
    private final ApplicationProperties properties;
    private final File applicationPropertiesFile;
    private final Path outputDirectory;

    public PropertyEncryptorCommand(final Path baseDirectory, final String passphrase) throws PropertyEncryptorException {
        confDirectory = ConfigurationFileUtils.resolveAbsoluteConfDirectory(baseDirectory);
        try {
            bootstrapLoader = getBootstrapPropertiesLoader(confDirectory);
            fileResolver = getConfigurationFileResolver(confDirectory);
            applicationPropertiesFile = ConfigurationFileUtils.resolvePropertiesFile(confDirectory);
            propertiesLoader = getPropertiesLoader(confDirectory);
            properties = propertiesLoader.load(applicationPropertiesFile);
            configurationFiles = fileResolver.resolveFilesFromApplicationProperties(properties);
            sensitivePropertyResolver = getSensitivePropertyResolver(confDirectory);
            inputHexKey = getExistingRootKey(confDirectory, passphrase);
            outputHexKey = deriveOutputKey(passphrase);
            outputDirectory = ConfigurationFileUtils.getOutputDirectory(baseDirectory);
            logger.info("Output directory created at [{}]", outputDirectory.toAbsolutePath());
        } catch (final Exception e) {
            throw new PropertyEncryptorException("Failed to run property encryptor", e);
        }
    }

    /**
     * @param baseDirectory The base directory of a NiFi / NiFi Registry installation that should be encrypted
     */
    public void encryptXmlConfigurationFiles(final Path baseDirectory, final ProtectionScheme scheme) {
        XmlEncryptor encryptor = getXmlEncryptor(scheme);
        try {
            encryptXmlConfigurationFiles(configurationFiles, encryptor);
            logger.info("The Property Encryptor successfully encrypted configuration files in the [{}] directory with the scheme [{}]", baseDirectory, scheme.getPath());
        } catch (Exception e) {
            logger.error("The Property Encryptor failed to encrypt configuration files in the [{}] directory with the scheme [{}]", baseDirectory, scheme.getPath(), e);
        }
    }

    public void encryptPropertiesFile(final ProtectionScheme scheme) throws IOException {
        List<String> sensitivePropertyKeys = sensitivePropertyResolver.resolveSensitivePropertyKeys(properties);
        final MutableApplicationProperties encryptedProperties = new MutableApplicationProperties(new Properties());
        final SensitivePropertyProvider provider = StandardSensitivePropertyProviderFactory.withKey(inputHexKey).getProvider(scheme);

        for (String key : sensitivePropertyKeys) {
            if (properties.getProperty(key) != null) {
                String encryptedValue = provider.protect(properties.getProperty(key), ProtectedPropertyContext.defaultContext(key));
                encryptedProperties.setProperty(key, encryptedValue);
            }
        }

        final File outputPropertiesFile = ConfigurationFileUtils.getOutputFile(outputDirectory, applicationPropertiesFile);
        try (FileInputStream inputStream = new FileInputStream(applicationPropertiesFile);
             FileOutputStream outputStream = new FileOutputStream(outputPropertiesFile)) {
            new StandardPropertiesWriter().writePropertiesFile(inputStream, outputStream, encryptedProperties);
        }
    }

    private void encryptXmlConfigurationFiles(final List<File> configurationFiles, final XmlEncryptor encryptor) {
        for (final File configurationFile : configurationFiles) {
            File outputFile = ConfigurationFileUtils.getOutputFile(outputDirectory, configurationFile);
            try (InputStream inputStream = new FileInputStream(configurationFile);
                FileOutputStream outputStream = new FileOutputStream(outputFile)) {
                encryptor.encrypt(inputStream, outputStream);
                logger.info("Successfully encrypted file at [{}], and output to [{}]", configurationFile.getAbsolutePath(), outputFile.getAbsolutePath());
            } catch (Exception e) {
                throw new PropertyEncryptorException(String.format("Failed to encrypt configuration file: [%s]", configurationFile.getAbsolutePath()), e);
            }
        }
    }

    public void migrateConfigurationFiles(final File baseDirectory) {
        logger.info("Not yet implemented.");
    }

    public void encryptFlowDefinition(final PropertyEncryptionMethod algorithm, final String passphrase) {
        final StandardFlowEncryptor flowEncryptor = new StandardFlowEncryptor();
        final String existingSensitivePropertyKey = getSensitivePropertyKey();
        final String existingEncryptionAlgorithm = getSensitivePropertyEncryptionAlgorithm();

        PropertyEncryptor inputEncryptor = new PropertyEncryptorBuilder(existingSensitivePropertyKey).setAlgorithm(existingEncryptionAlgorithm).build();
        PropertyEncryptor outputEncryptor = new PropertyEncryptorBuilder(passphrase).setAlgorithm(algorithm).build();

        if (properties instanceof NiFiProperties) {
            List<File> flowDefinitions = new NiFiFlowDefinitionFileResolver().resolveFilesFromApplicationProperties((NiFiProperties) properties);
            for (File flow : flowDefinitions) {
                final File absoluteFlowDefinition = ConfigurationFileUtils.getAbsoluteFile(confDirectory.toFile(), flow);
                try {
                    File output = ConfigurationFileUtils.getOutputFile(outputDirectory, absoluteFlowDefinition);
                    try (InputStream inputStream = new GZIPInputStream(new FileInputStream(absoluteFlowDefinition));
                         FileOutputStream outputStream = new FileOutputStream(output)) {
                        flowEncryptor.processFlow(inputStream, outputStream, inputEncryptor, outputEncryptor);
                    }
                } catch (IOException e) {
                    logger.error("Failed to encrypt flow definition file: [{}]", flow.getAbsolutePath(), e);
                }
            }
        }
    }

    public void outputSensitiveProperties(final PropertyEncryptionMethod algorithm, final String sensitivePropertyKey) {
        /* TODO
         * Output will look like:
         * nifi.sensitive.props.key=UXcrW8T1UKAPJeun||ezUJSp30AvKGsRxJOOXoPUtZonv56Lx1
         * nifi.sensitive.props.key.protected=aes/gcm/128
         * nifi.sensitive.props.algorithm=PBEWITHMD5AND256BITAES-CBC-OPENSSL
         */

        final File outputPropertiesFile = ConfigurationFileUtils.getOutputFile(outputDirectory, applicationPropertiesFile);
        final MutableApplicationProperties updatedProperties = new MutableApplicationProperties(new Properties());
        updatedProperties.setProperty(NiFiProperties.SENSITIVE_PROPS_KEY, sensitivePropertyKey);
        updatedProperties.setProperty(NiFiProperties.SENSITIVE_PROPS_ALGORITHM, algorithm.name());

        try {
            try (InputStream inputStream = new FileInputStream(applicationPropertiesFile);
                 FileOutputStream outputStream = new FileOutputStream(outputPropertiesFile)) {
                new StandardPropertiesWriter().writePropertiesFile(inputStream, outputStream, updatedProperties);
            }
            logger.info("Output sensitive properties key to {}", applicationPropertiesFile.getAbsolutePath());
        } catch (IOException e) {
            logger.error("Failed to output sensitive property keys after encrypting flow", e);
        }
    }

    public void outputKeyToBootstrap() throws IOException {
        final File bootstrapFile = bootstrapLoader.getBootstrapFileWithinConfDirectory(confDirectory);
        final File outputBootstrapFile = ConfigurationFileUtils.getOutputFile(outputDirectory, bootstrapFile);
        final MutableBootstrapProperties bootstrapProperties = bootstrapLoader.loadMutableBootstrapProperties(bootstrapFile.getPath());
        bootstrapProperties.setProperty(BootstrapProperties.BootstrapPropertyKey.SENSITIVE_KEY.getKey(), outputHexKey);
        try (InputStream inputStream = new FileInputStream(bootstrapFile);
             FileOutputStream outputStream = new FileOutputStream(outputBootstrapFile)) {
            new StandardPropertiesWriter().writePropertiesFile(inputStream, outputStream, bootstrapProperties);
            logger.info("Output the bootstrap key to {}", outputBootstrapFile);
        }
    }

    private XmlEncryptor getXmlEncryptor(final ProtectionScheme scheme) {
        final SensitivePropertyProviderFactory providerFactory = StandardSensitivePropertyProviderFactory.withKey(inputHexKey);
        return new XmlEncryptor(providerFactory, scheme);
    }

    private XmlDecryptor getXmlDecryptor(final SensitivePropertyProviderFactory providerFactory, final ProtectionScheme scheme) {
        return new XmlDecryptor(providerFactory, scheme);
    }

    private String getExistingRootKey(final Path confDirectory, final String passphrase) {
        String encodedRootKey;

        try {
            final File bootstrapConf = bootstrapLoader.getBootstrapFileWithinConfDirectory(confDirectory);
            encodedRootKey = bootstrapLoader.extractKeyFromBootstrapFile(bootstrapConf.getAbsolutePath());
        } catch (IOException e) {
            throw new PropertyEncryptorException("Failed to get key hex from bootstrap file", e);
        }

        return encodedRootKey;
    }

    private String deriveOutputKey(final String passphrase) {
        try {
            return SecureHasherFactory.getSecureHasher(KeyDerivationFunction.SCRYPT.getKdfName()).hashHex(passphrase).toUpperCase();
        } catch (Exception e) {
            throw new PropertyEncryptorException("Failed to derive an encryption key from the provided passphrase", e);
        }
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

    private SensitivePropertyResolver getSensitivePropertyResolver(final Path baseDirectory) {
        if (ConfigurationFileUtils.isNiFiRegistryConfDirectory(baseDirectory)) {
            return new NiFiRegistrySensitivePropertyResolver();
        } else if (ConfigurationFileUtils.isNiFiConfDirectory(baseDirectory)) {
            return new NiFiSensitivePropertyResolver();
        } else {
            throw new PropertyEncryptorException(String.format("The base directory [%s] does not contain a recognized .properties file", baseDirectory));
        }
    }

    private String getSensitivePropertyKey() {
        return properties.getProperty(NiFiProperties.SENSITIVE_PROPS_KEY, "");
    }

    private String getSensitivePropertyEncryptionAlgorithm() {
        return properties.getProperty(NiFiProperties.SENSITIVE_PROPS_ALGORITHM, "");
    }
}