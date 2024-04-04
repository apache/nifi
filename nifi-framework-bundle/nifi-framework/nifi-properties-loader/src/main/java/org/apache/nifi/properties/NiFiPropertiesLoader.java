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

import org.apache.nifi.property.protection.loader.PropertyProtectionURLClassLoader;
import org.apache.nifi.property.protection.loader.PropertyProviderFactoryLoader;
import org.apache.nifi.util.NiFiBootstrapUtils;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.HashSet;
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

    private static final String SET_KEY_METHOD = "setKeyHex";

    private final String defaultPropertiesFilePath = NiFiBootstrapUtils.getDefaultApplicationPropertiesFilePath();
    private NiFiProperties instance;
    private String keyHex;

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
     */
    public static NiFiProperties loadDefaultWithKeyFromBootstrap() {
        try {
            return new NiFiPropertiesLoader().loadDefault();
        } catch (Exception e) {
            logger.warn("Encountered an error naively loading the nifi.properties file because one or more properties are protected: {}", e.getLocalizedMessage());
            throw e;
        }
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
    ProtectedNiFiProperties loadProtectedProperties(final File file) {
        if (file == null || !file.exists() || !file.canRead()) {
            throw new IllegalArgumentException(String.format("Application Properties [%s] not found", file));
        }

        logger.info("Loading Application Properties [{}]", file);
        final DuplicateDetectingProperties rawProperties = new DuplicateDetectingProperties();

        try (final InputStream inputStream = new BufferedInputStream(new FileInputStream(file))) {
            rawProperties.load(inputStream);
        } catch (final Exception e) {
            throw new RuntimeException(String.format("Loading Application Properties [%s] failed", file), e);
        }

        if (!rawProperties.redundantKeySet().isEmpty()) {
            logger.warn("Duplicate property keys with the same value were detected in the properties file: {}", String.join(", ", rawProperties.redundantKeySet()));
        }
        if (!rawProperties.duplicateKeySet().isEmpty()) {
            throw new IllegalArgumentException("Duplicate property keys with different values were detected in the properties file: " + String.join(", ", rawProperties.duplicateKeySet()));
        }

        final Properties properties = new Properties();
        final Set<String> keys = rawProperties.stringPropertyNames();
        for (final String key : keys) {
            final String property = rawProperties.getProperty(key);
            properties.setProperty(key, property.trim());
        }

        return new ProtectedNiFiProperties(properties);
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
        final ProtectedNiFiProperties protectedProperties = loadProtectedProperties(file);
        final NiFiProperties properties;

        if (protectedProperties.hasProtectedKeys()) {
            final ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();

            try {
                final PropertyProtectionURLClassLoader protectionClassLoader = new PropertyProtectionURLClassLoader(contextClassLoader);
                Thread.currentThread().setContextClassLoader(protectionClassLoader);

                final PropertyProviderFactoryLoader factoryLoader = new PropertyProviderFactoryLoader();
                final SensitivePropertyProviderFactory providerFactory = factoryLoader.getPropertyProviderFactory();
                setBootstrapKey(providerFactory);
                providerFactory.getSupportedProviders().forEach(protectedProperties::addSensitivePropertyProvider);

                properties = protectedProperties.getUnprotectedProperties();

                providerFactory.getSupportedProviders().forEach(SensitivePropertyProvider::cleanUp);
            } finally {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        } else {
            properties = protectedProperties.getUnprotectedProperties();
        }

        return properties;
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

    private NiFiProperties loadDefault() {
        return load(defaultPropertiesFilePath);
    }

    private NiFiProperties getDefaultProperties() {
        NiFiProperties defaultProperties = loadDefault();
        if (isKeyGenerationRequired(defaultProperties)) {
            if (defaultProperties.isClustered()) {
                logger.error("Clustered Configuration Found: Shared Sensitive Properties Key [{}] required for cluster nodes", NiFiProperties.SENSITIVE_PROPS_KEY);
                throw new SensitivePropertyProtectionException(PROPERTIES_KEY_MESSAGE);
            }

            final File flowConfigurationFile = defaultProperties.getFlowConfigurationFile();
            if (flowConfigurationFile.exists()) {
                logger.error("Flow Configuration [{}] Found: Migration Required for blank Sensitive Properties Key [{}]", flowConfigurationFile, NiFiProperties.SENSITIVE_PROPS_KEY);
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

    private void setBootstrapKey(final SensitivePropertyProviderFactory providerFactory) {
        if (keyHex == null) {
            logger.debug("Bootstrap Sensitive Key not configured");
        } else {
            final Class<? extends SensitivePropertyProviderFactory> factoryClass = providerFactory.getClass();
            try {
                // Set Bootstrap Key using reflection to preserve ClassLoader isolation
                final Method setMethod = factoryClass.getMethod(SET_KEY_METHOD, String.class);
                setMethod.invoke(providerFactory, keyHex);
            } catch (final NoSuchMethodException e) {
                logger.warn("Method [{}] on Class [{}] not found", SET_KEY_METHOD, factoryClass.getName());
            } catch (final IllegalAccessException e) {
                logger.warn("Method [{}] on Class [{}] access not allowed", SET_KEY_METHOD, factoryClass.getName());
            } catch (final InvocationTargetException e) {
                throw new SensitivePropertyProtectionException("Set Bootstrap Key on Provider Factory failed", e);
            }
        }
    }

    private static class DuplicateDetectingProperties extends Properties {
        // Only need to retain Properties key. This will help prevent possible inadvertent exposure of sensitive Properties value
        private final Set<String> duplicateKeys = new HashSet<>();  // duplicate key with different values
        private final Set<String> redundantKeys = new HashSet<>();  // duplicate key with same value
        public DuplicateDetectingProperties() {
            super();
        }

        public Set<String> duplicateKeySet() {
            return duplicateKeys;
        }

        public Set<String> redundantKeySet() {
            return redundantKeys;
        }

        @Override
        public Object put(Object key, Object value) {
            Object existingValue = super.put(key, value);
            if (existingValue != null) {
                if (existingValue.toString().equals(value.toString())) {
                    redundantKeys.add(key.toString());
                    return existingValue;
                } else {
                    duplicateKeys.add(key.toString());
                }
            }
            return value;
        }
    }
}
