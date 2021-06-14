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
package org.apache.nifi.registry.properties;

import org.apache.nifi.properties.BootstrapProperties;
import org.apache.nifi.properties.SensitivePropertyProvider;
import org.apache.nifi.properties.SensitivePropertyProviderFactory;
import org.apache.nifi.properties.StandardSensitivePropertyProviderFactory;
import org.apache.nifi.registry.properties.util.NiFiRegistryBootstrapUtils;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.security.Security;
import java.util.Properties;

public class NiFiRegistryPropertiesLoader {

    private static final Logger logger = LoggerFactory.getLogger(NiFiRegistryPropertiesLoader.class);

    private static final String APPLICATION_PATH = "nifi.registry";

    private static final String RELATIVE_PATH = "conf/nifi-registry.properties";

    private String keyHex;

    // Future enhancement: allow for external registration of new providers
    private SensitivePropertyProviderFactory sensitivePropertyProviderFactory;

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
    public static NiFiRegistryPropertiesLoader withKey(final String keyHex) {
        NiFiRegistryPropertiesLoader loader = new NiFiRegistryPropertiesLoader();
        loader.setKeyHex(keyHex);
        return loader;
    }

    /**
     * Sets the hexadecimal key used to unprotect properties encrypted with
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

    private SensitivePropertyProviderFactory getSensitivePropertyProviderFactory() {
        if (sensitivePropertyProviderFactory == null) {
            sensitivePropertyProviderFactory = StandardSensitivePropertyProviderFactory
                    .withKeyAndBootstrapSupplier(keyHex, () -> {
                        try {
                            return NiFiRegistryBootstrapUtils.loadBootstrapProperties();
                        } catch (IOException e) {
                            logger.debug("Cannot read bootstrap.conf -- file is missing or not readable.  Defaulting to empty bootstrap.conf");
                            return BootstrapProperties.EMPTY;
                        }
                    });
        }
        return sensitivePropertyProviderFactory;
    }

    /**
     * Returns a {@link ProtectedNiFiRegistryProperties} instance loaded from the
     * serialized form in the file. Responsible for actually reading from disk
     * and deserializing the properties. Returns a protected instance to allow
     * for decryption operations.
     *
     * @param file the file containing serialized properties
     * @return the ProtectedNiFiProperties instance
     */
    ProtectedNiFiRegistryProperties readProtectedPropertiesFromDisk(File file) {
        if (file == null || !file.exists() || !file.canRead()) {
            String path = (file == null ? "missing file" : file.getAbsolutePath());
            logger.error("Cannot read from '{}' -- file is missing or not readable", path);
            throw new IllegalArgumentException("NiFi Registry properties file missing or unreadable");
        }

        final Properties rawProperties = new Properties();
        try (final FileReader reader = new FileReader(file)) {
            rawProperties.load(reader);
            final NiFiRegistryProperties innerProperties = new NiFiRegistryProperties(rawProperties);
            logger.info("Loaded {} properties from {}", rawProperties.size(), file.getAbsolutePath());
            ProtectedNiFiRegistryProperties protectedNiFiRegistryProperties = new ProtectedNiFiRegistryProperties(innerProperties);
            return protectedNiFiRegistryProperties;
        } catch (final IOException ioe) {
            logger.error("Cannot load properties file due to " + ioe.getLocalizedMessage());
            throw new RuntimeException("Cannot load properties file due to " + ioe.getLocalizedMessage(), ioe);
        }
    }

    /**
     * Returns an instance of {@link NiFiRegistryProperties} loaded from the provided
     * {@link File}. If any properties are protected, will attempt to use the appropriate
     * {@link SensitivePropertyProvider} to unprotect them transparently.
     *
     * @param file the File containing the serialized properties
     * @return the NiFiProperties instance
     */
    public NiFiRegistryProperties load(final File file) {
        final ProtectedNiFiRegistryProperties protectedNiFiProperties = readProtectedPropertiesFromDisk(file);
        if (protectedNiFiProperties.hasProtectedKeys()) {
            Security.addProvider(new BouncyCastleProvider());
            getSensitivePropertyProviderFactory()
                    .getSupportedSensitivePropertyProviders()
                    .forEach(protectedNiFiProperties::addSensitivePropertyProvider);
        }

        return protectedNiFiProperties.getUnprotectedProperties();
    }

    /**
     * Returns an instance of {@link NiFiRegistryProperties}. The path must not be empty.
     *
     * @param path the path of the serialized properties file
     * @return the NiFiRegistryProperties instance
     * @see NiFiRegistryPropertiesLoader#load(File)
     */
    public NiFiRegistryProperties load(String path) {
        if (path != null && !path.trim().isEmpty()) {
            return load(new File(path));
        } else {
            logger.error("Cannot read from '{}' -- path is null or empty", path);
            throw new IllegalArgumentException("NiFi Registry properties file path empty or null");
        }
    }

}
