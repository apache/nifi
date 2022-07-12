package org.apache.nifi;

import com.sun.jna.platform.unix.solaris.LibKstat;
import org.apache.nifi.console.subcommands.Config;
import org.apache.nifi.properties.AbstractBootstrapPropertiesLoader;
import org.apache.nifi.properties.SensitivePropertyProviderFactory;
import org.apache.nifi.properties.StandardSensitivePropertyProviderFactory;
import org.apache.nifi.properties.scheme.PropertyProtectionScheme;
import org.apache.nifi.properties.scheme.ProtectionScheme;
import org.apache.nifi.registry.properties.util.NiFiRegistryBootstrapPropertiesLoader;
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
import java.security.KeyException;
import java.security.NoSuchAlgorithmException;
import java.util.List;

public class PropertyEncryptorMain {

    private static final Logger logger = LoggerFactory.getLogger(PropertyEncryptorMain.class);
    AbstractBootstrapPropertiesLoader bootstrapLoader;
    PropertiesLoader<ApplicationProperties> propertiesLoader;
    ConfigurationFileResolver fileResolver;
    List<File> configurationFiles;
    String hexKey;
    Path confDirectory;

    public PropertyEncryptorMain(final Path baseDirectory, final String password) throws PropertyEncryptorException {
        final Path confDirectory = FileUtilities.resolveAbsoluteConfDirectory(baseDirectory);
        try {
            bootstrapLoader = getBootstrapPropertiesLoader(confDirectory);
            fileResolver = new ConfigurationFileResolver(confDirectory);
            final File applicationProperties = FileUtilities.resolvePropertiesFile(confDirectory);
            propertiesLoader = getPropertiesLoader(confDirectory);
            configurationFiles = fileResolver.resolveConfigurationFilesFromApplicationProperties(propertiesLoader.load(applicationProperties));
            hexKey = getKeyHex(confDirectory, password);
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
     *
     * @param baseDirectory The base directory of a NiFi / NiFi Registry installation that should be encrypted
     * @return
     */
    public int encryptConfigurationFiles(final Path baseDirectory, final String passphrase, final PropertyProtectionScheme scheme) {
        XmlEncryptor encryptor = getXmlEncryptor(baseDirectory, scheme);
        try {
            encryptConfigurationFiles(configurationFiles, encryptor);
            logger.info("The Property Encryptor successfully encrypted configuration files in the [%s] directory with the scheme [%s]", baseDirectory, scheme.getPath());
            return 0;
        } catch (Exception e) {
            logger.error("The Property Encryptor failed to encrypt configuration files in the [%s] directory with the scheme [%s]", baseDirectory, scheme.getPath(), e);
            return 1;
        }
    }

    private int encryptConfigurationFiles(final List<File> configurationFiles, final XmlEncryptor encryptor) throws IOException {
        for (final File configurationFile : configurationFiles) {
            File temp = FileUtilities.getTemporaryOutputFile("tmp", configurationFile);
            try (InputStream inputStream = new FileInputStream(configurationFile);
                FileOutputStream outputStream = new FileOutputStream(temp)) {
                encryptor.encrypt(inputStream, outputStream);
            }

            //Files.copy(temp.toPath(), configurationFile.toPath());
        }
        return 0;
    }

    /**
     *
     * @param baseDirectory The base directory of a NiFi / NiFi Registry installation that should be encrypted
     * @return
     */
    public int migrateConfigurationFiles(final File baseDirectory) {
        return 0;
    }

    public int encryptFlowDefinition(final File baseDirectory) {
        return 0;
    }

    private XmlEncryptor getXmlEncryptor(final Path baseDirectory, final PropertyProtectionScheme scheme) {
        //final ApplicationProperties applicationProperties = propertiesLoader.load(baseDirectory.toFile());
        final SensitivePropertyProviderFactory providerFactory = StandardSensitivePropertyProviderFactory.withKey(hexKey);
        return new XmlEncryptor(providerFactory, scheme);
    }

    private XmlDecryptor getXmlDecryptor(final SensitivePropertyProviderFactory providerFactory, final ProtectionScheme scheme) {
        return new XmlDecryptor(providerFactory, scheme);
    }

    private String getKeyHex(final Path confDirectory, final String password) {
        String keyHex;

        try {
            final File bootstrapConf = bootstrapLoader.getBootstrapFileWithinConfDirectory(confDirectory);
            keyHex = bootstrapLoader.extractKeyFromBootstrapFile(bootstrapConf.getAbsolutePath());
        } catch (IOException e) {
            throw new PropertyEncryptorException("Failed to get key hex from bootstrap file", e);
        }

        if (keyHex.isEmpty()) {
            try {
                keyHex = CryptographyUtils.deriveKeyFromPassword(password);
            } catch (Exception e) {
                throw new PropertyEncryptorException("Failed to derive an encryption key from the provided password", e);
            }
        }

        return keyHex;
    }
}