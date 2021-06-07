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
package org.apache.nifi.properties;

import org.apache.nifi.util.NiFiBootstrapUtils;
import org.apache.nifi.util.NiFiProperties;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.security.Security;
import java.util.Base64;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

public class NiFiPropertiesLoader {

    private static final Logger logger = LoggerFactory.getLogger(NiFiPropertiesLoader.class);
    private static final Base64.Encoder KEY_ENCODER = Base64.getEncoder().withoutPadding();
    private static final int SENSITIVE_PROPERTIES_KEY_LENGTH = 24;
    private static final String EMPTY_SENSITIVE_PROPERTIES_KEY = String.format("%s=", NiFiProperties.SENSITIVE_PROPS_KEY);
    private static final String MIGRATION_INSTRUCTIONS = "See Admin Guide section [Updating the Sensitive Properties Key]";
    private static final String PROPERTIES_KEY_MESSAGE = String.format("Sensitive Properties Key [%s] not found: %s", NiFiProperties.SENSITIVE_PROPS_KEY, MIGRATION_INSTRUCTIONS);

    private final String defaultPropertiesFilePath = NiFiBootstrapUtils.getDefaultApplicationPropertiesFilePath();
    private NiFiProperties instance;
    private String keyHex;

    // Future enhancement: allow for external registration of new providers
    private SensitivePropertyProviderFactory sensitivePropertyProviderFactory;

    public NiFiPropertiesLoader() {
    }

    /**
     * Returns an instance of the loader configured with the key.
     * <p>
     * <p>
     * NOTE: This method is used reflectively by the process which starts NiFi
     * so changes to it must be made in conjunction with that mechanism.</p>
     *
     * @param keyHex the key used to encrypt any sensitive properties
     * @return the configured loader
     */
    public static NiFiPropertiesLoader withKey(final String keyHex) {
        final NiFiPropertiesLoader loader = new NiFiPropertiesLoader();
        loader.setKeyHex(keyHex);
        return loader;
    }

    /**
     * Sets the hexadecimal key used to unprotect properties encrypted with a
     * {@link SensitivePropertyProvider}. If the key has already been set,
     * calling this method will throw a {@link RuntimeException}.
     *
     * @param keyHex the key in hexadecimal format
     */
    public void setKeyHex(final String keyHex) {
        if (this.keyHex == null || this.keyHex.trim().isEmpty()) {
            this.keyHex = keyHex;
        } else {
            throw new RuntimeException("Cannot overwrite an existing key");
        }
    }

    /**
     * Returns a {@link NiFiProperties} instance with any encrypted properties
     * decrypted using the key from the {@code conf/bootstrap.conf} file. This
     * method is exposed to allow Spring factory-method loading at application
     * startup.
     *
     * @return the populated and decrypted NiFiProperties instance
     * @throws IOException if there is a problem reading from the bootstrap.conf
     *                     or nifi.properties files
     */
    public static NiFiProperties loadDefaultWithKeyFromBootstrap() throws IOException {
        try {
            // The default behavior of StandardSensitivePropertiesFactory is to use the key
            // from bootstrap.conf if no key is provided
            return new NiFiPropertiesLoader().loadDefault();
        } catch (Exception e) {
            logger.warn("Encountered an error naively loading the nifi.properties file because one or more properties are protected: {}", e.getLocalizedMessage());
            throw e;
        }
    }

    private NiFiProperties loadDefault() {
        return load(defaultPropertiesFilePath);
    }

    private SensitivePropertyProviderFactory getSensitivePropertyProviderFactory() {
        if (sensitivePropertyProviderFactory == null) {
            sensitivePropertyProviderFactory = StandardSensitivePropertyProviderFactory.withKey(keyHex);
        }
        return sensitivePropertyProviderFactory;
    }

    /**
     * Returns a {@link ProtectedNiFiProperties} instance loaded from the
     * serialized form in the file. Responsible for actually reading from disk
     * and deserializing the properties. Returns a protected instance to allow
     * for decryption operations.
     *
     * @param file the file containing serialized properties
     * @return the ProtectedNiFiProperties instance
     */
    ProtectedNiFiProperties readProtectedPropertiesFromDisk(File file) {
        if (file == null || !file.exists() || !file.canRead()) {
            String path = (file == null ? "missing file" : file.getAbsolutePath());
            logger.error("Cannot read from '{}' -- file is missing or not readable", path);
            throw new IllegalArgumentException("NiFi properties file missing or unreadable");
        }

        final Properties rawProperties = new Properties();
        try (final InputStream inputStream = new BufferedInputStream(new FileInputStream(file))) {
            rawProperties.load(inputStream);
            logger.info("Loaded {} properties from {}", rawProperties.size(), file.getAbsolutePath());

            final Set<String> keys = rawProperties.stringPropertyNames();
            for (final String key : keys) {
                final String property = rawProperties.getProperty(key);
                rawProperties.setProperty(key, property.trim());
            }

            return new ProtectedNiFiProperties(rawProperties);
        } catch (final Exception ex) {
            logger.error("Cannot load properties file due to {}", ex.getLocalizedMessage());
            throw new RuntimeException("Cannot load properties file due to "
                    + ex.getLocalizedMessage(), ex);
        }
    }

    /**
     * Returns an instance of {@link NiFiProperties} loaded from the provided
     * {@link File}. If any properties are protected, will attempt to use the
     * appropriate {@link SensitivePropertyProvider} to unprotect them
     * transparently.
     *
     * @param file the File containing the serialized properties
     * @return the NiFiProperties instance
     */
    public NiFiProperties load(final File file) {
        final ProtectedNiFiProperties protectedNiFiProperties = readProtectedPropertiesFromDisk(file);
        if (protectedNiFiProperties.hasProtectedKeys()) {
            Security.addProvider(new BouncyCastleProvider());
            getSensitivePropertyProviderFactory()
                    .getSupportedSensitivePropertyProviders()
                    .forEach(protectedNiFiProperties::addSensitivePropertyProvider);
        }

        return protectedNiFiProperties.getUnprotectedProperties();
    }

    /**
     * Returns an instance of {@link NiFiProperties}. If the path is empty, this
     * will load the default properties file as specified by
     * {@code NiFiProperties.PROPERTY_FILE_PATH}.
     *
     * @param path the path of the serialized properties file
     * @return the NiFiProperties instance
     * @see NiFiPropertiesLoader#load(File)
     */
    public NiFiProperties load(String path) {
        if (path != null && !path.trim().isEmpty()) {
            return load(new File(path));
        } else {
            return loadDefault();
        }
    }

    /**
     * Returns the loaded {@link NiFiProperties} instance. If none is currently
     * loaded, attempts to load the default instance.
     * <p>
     * <p>
     * NOTE: This method is used reflectively by the process which starts NiFi
     * so changes to it must be made in conjunction with that mechanism.</p>
     *
     * @return the current NiFiProperties instance
     */
    public NiFiProperties get() {
        if (instance == null) {
            instance = getDefaultProperties();
        }

        return instance;
    }

    private NiFiProperties getDefaultProperties() {
        NiFiProperties defaultProperties = loadDefault();
        if (isKeyGenerationRequired(defaultProperties)) {
            if (defaultProperties.isClustered()) {
                logger.error("Clustered Configuration Found: Shared Sensitive Properties Key [{}] required for cluster nodes", NiFiProperties.SENSITIVE_PROPS_KEY);
                throw new SensitivePropertyProtectionException(PROPERTIES_KEY_MESSAGE);
            }

            final File flowConfiguration = defaultProperties.getFlowConfigurationFile();
            if (flowConfiguration.exists()) {
                logger.error("Flow Configuration [{}] Found: Migration Required for blank Sensitive Properties Key [{}]", flowConfiguration, NiFiProperties.SENSITIVE_PROPS_KEY);
                throw new SensitivePropertyProtectionException(PROPERTIES_KEY_MESSAGE);
            }
            setSensitivePropertiesKey();
            defaultProperties = loadDefault();
        }
        return defaultProperties;
    }

    private void setSensitivePropertiesKey() {
        logger.warn("Generating Random Sensitive Properties Key [{}]", NiFiProperties.SENSITIVE_PROPS_KEY);
        final SecureRandom secureRandom = new SecureRandom();
        final byte[] sensitivePropertiesKeyBinary = new byte[SENSITIVE_PROPERTIES_KEY_LENGTH];
        secureRandom.nextBytes(sensitivePropertiesKeyBinary);
        final String sensitivePropertiesKey = KEY_ENCODER.encodeToString(sensitivePropertiesKeyBinary);
        try {
            final File niFiPropertiesFile = new File(defaultPropertiesFilePath);
            final Path niFiPropertiesPath = Paths.get(niFiPropertiesFile.toURI());
            final List<String> lines = Files.readAllLines(niFiPropertiesPath);
            final List<String> updatedLines = lines.stream().map(line -> {
                if (line.equals(EMPTY_SENSITIVE_PROPERTIES_KEY)) {
                    return line + sensitivePropertiesKey;
                } else {
                    return line;
                }
            }).collect(Collectors.toList());
            Files.write(niFiPropertiesPath, updatedLines);

            logger.info("NiFi Properties [{}] updated with Sensitive Properties Key", niFiPropertiesPath);
        } catch (final IOException e) {
            throw new UncheckedIOException("Failed to set Sensitive Properties Key", e);
        }
    }

    private static boolean isKeyGenerationRequired(final NiFiProperties properties) {
        final String configuredSensitivePropertiesKey = properties.getProperty(NiFiProperties.SENSITIVE_PROPS_KEY);
        return (configuredSensitivePropertiesKey == null || configuredSensitivePropertiesKey.length() == 0);
    }
}
