package org.apache.nifi;

import org.apache.nifi.properties.AbstractBootstrapPropertiesLoader;
import org.apache.nifi.properties.BootstrapProperties;
import org.apache.nifi.properties.MutableBootstrapProperties;
import org.apache.nifi.properties.SensitivePropertyProviderFactory;
import org.apache.nifi.properties.StandardSensitivePropertyProviderFactory;
import org.apache.nifi.properties.scheme.PropertyProtectionScheme;
import org.apache.nifi.properties.scheme.ProtectionScheme;
import org.apache.nifi.registry.properties.util.NiFiRegistryBootstrapPropertiesLoader;
import org.apache.nifi.serde.PropertiesWriter;
import org.apache.nifi.util.NiFiBootstrapPropertiesLoader;
import org.apache.nifi.util.crypto.CryptographyUtils;
import org.apache.nifi.xml.XmlDecryptor;
import org.apache.nifi.xml.XmlEncryptor;
import org.apache.nifi.util.file.ConfigurationFileResolver;
import org.apache.nifi.util.file.FileUtilities;
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

public class PropertyEncryptorMain {

    private static final Logger logger = LoggerFactory.getLogger(PropertyEncryptorMain.class);
    private final AbstractBootstrapPropertiesLoader bootstrapLoader;
    private final PropertiesLoader<ApplicationProperties> propertiesLoader;
    private final ConfigurationFileResolver fileResolver;
    private final List<File> configurationFiles;
    private String hexKey;
    private final Path confDirectory;

    public PropertyEncryptorMain(final Path baseDirectory, final String passphrase) throws PropertyEncryptorException {
        confDirectory = FileUtilities.resolveAbsoluteConfDirectory(baseDirectory);
        try {
            bootstrapLoader = getBootstrapPropertiesLoader(confDirectory);
            fileResolver = new ConfigurationFileResolver(confDirectory);
            final File applicationProperties = FileUtilities.resolvePropertiesFile(confDirectory);
            propertiesLoader = getPropertiesLoader(confDirectory);
            configurationFiles = fileResolver.resolveConfigurationFilesFromApplicationProperties(propertiesLoader.load(applicationProperties));
            hexKey = getKeyHex(confDirectory, passphrase);
        } catch (final Exception e) {
            throw new PropertyEncryptorException("Failed to run property encryptor", e);
        }
    }

    private AbstractBootstrapPropertiesLoader getBootstrapPropertiesLoader(final Path baseDirectory) {
        if (FileUtilities.isNiFiRegistryConfDirectory(baseDirectory)) {
            return new NiFiRegistryBootstrapPropertiesLoader();
        } else if (FileUtilities.isNiFiConfDirectory(baseDirectory)) {
            return new NiFiBootstrapPropertiesLoader();
        } else {
            throw new PropertyEncryptorException("The base directory provided does not contain a recognized bootstrap.conf file");
        }
    }

    private PropertiesLoader<ApplicationProperties> getPropertiesLoader(final Path baseDirectory) {
        if (FileUtilities.isNiFiRegistryConfDirectory(baseDirectory)) {
            return new NiFiRegistryPropertiesLoader();
        } else if (FileUtilities.isNiFiConfDirectory(baseDirectory)) {
            return new NiFiPropertiesLoader();
        } else {
            throw new PropertyEncryptorException("The base directory provided does not contain a recognized .properties file");
        }
    }

    /**
     * @param baseDirectory The base directory of a NiFi / NiFi Registry installation that should be encrypted
     * @return
     */
    public int encryptConfigurationFiles(final Path baseDirectory, final String passphrase, final PropertyProtectionScheme scheme) {
        XmlEncryptor encryptor = getXmlEncryptor(scheme);
        try {
            encryptConfigurationFiles(configurationFiles, encryptor);
            outputKeyToBootstrap();
            logger.info("The Property Encryptor successfully encrypted configuration files in the [{}] directory with the scheme [{}]", baseDirectory, scheme.getPath());
            return 0;
        } catch (Exception e) {
            logger.error("The Property Encryptor failed to encrypt configuration files in the [{}] directory with the scheme [{}]", baseDirectory, scheme.getPath(), e);
            return 1;
        }
    }

    private void outputKeyToBootstrap() throws IOException {
        final File bootstrapFile = bootstrapLoader.getBootstrapFileWithinConfDirectory(confDirectory);
        File tempBootstrapFile = FileUtilities.getTemporaryOutputFile("tmp", bootstrapFile);
        final MutableBootstrapProperties bootstrapProperties = bootstrapLoader.loadMutableBootstrapProperties(bootstrapFile.getPath());
        bootstrapProperties.setProperty(BootstrapProperties.BootstrapPropertyKey.SENSITIVE_KEY.getKey(), hexKey);
        PropertiesWriter.writePropertiesFile(new FileInputStream(bootstrapFile), new FileOutputStream(tempBootstrapFile), bootstrapProperties);
        logger.info("Output the bootstrap key to {}", tempBootstrapFile);
    }

    private int encryptConfigurationFiles(final List<File> configurationFiles, final XmlEncryptor encryptor) throws IOException {
        for (final File configurationFile : configurationFiles) {
            File temp = FileUtilities.getTemporaryOutputFile("tmp", configurationFile);
            try (InputStream inputStream = new FileInputStream(configurationFile);
                FileOutputStream outputStream = new FileOutputStream(temp)) {
                encryptor.encrypt(inputStream, outputStream);
                logger.info("Successfully encrypted [{}]", configurationFile.getAbsolutePath());
            } catch (Exception e) {
                throw new PropertyEncryptorException(String.format("Failed to encrypt configuration file: [%s]", configurationFile.getAbsolutePath()), e);
            }

            //Files.copy(temp.toPath(), configurationFile.toPath());
        }
        return 0;
    }

    /**
    * @param baseDirectory The base directory of a NiFi / NiFi Registry installation that should be migrated to a new scheme eg. ./nifi
     * @return
     */
    public int migrateConfigurationFiles(final File baseDirectory) {
        logger.info("Not yet implemented.");
        return 0;
    }


    public int encryptFlowDefinition(final File baseDirectory) {
        logger.info("Not yet implemented.");
        return 0;
    }

    private XmlEncryptor getXmlEncryptor(final PropertyProtectionScheme scheme) {
        final SensitivePropertyProviderFactory providerFactory = StandardSensitivePropertyProviderFactory.withKey(hexKey);
        return new XmlEncryptor(providerFactory, scheme);
    }

    private XmlDecryptor getXmlDecryptor(final SensitivePropertyProviderFactory providerFactory, final ProtectionScheme scheme) {
        return new XmlDecryptor(providerFactory, scheme);
    }

    private String getKeyHex(final Path confDirectory, final String passphrase) {
        String keyHex;

        try {
            final File bootstrapConf = bootstrapLoader.getBootstrapFileWithinConfDirectory(confDirectory);
            keyHex = bootstrapLoader.extractKeyFromBootstrapFile(bootstrapConf.getAbsolutePath());
        } catch (IOException e) {
            throw new PropertyEncryptorException("Failed to get key hex from bootstrap file", e);
        }

        if (keyHex.isEmpty()) {
            try {
                keyHex = CryptographyUtils.deriveKeyFromPassphrase(passphrase);
            } catch (Exception e) {
                throw new PropertyEncryptorException("Failed to derive an encryption key from the provided passphrase", e);
            }
        }

        return keyHex;
    }
}