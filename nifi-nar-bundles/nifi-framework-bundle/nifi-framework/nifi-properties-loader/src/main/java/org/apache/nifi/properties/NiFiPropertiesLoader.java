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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.security.Security;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Stream;
import javax.crypto.Cipher;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.util.NiFiProperties;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NiFiPropertiesLoader {

    private static final Logger logger = LoggerFactory.getLogger(NiFiPropertiesLoader.class);

    private static final String RELATIVE_PATH = "conf/nifi.properties";

    private static final String BOOTSTRAP_KEY_PREFIX = "nifi.bootstrap.sensitive.key=";

    private NiFiProperties instance;
    private String keyHex;

    // Future enhancement: allow for external registration of new providers
    private static SensitivePropertyProviderFactory sensitivePropertyProviderFactory;

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
    public static NiFiPropertiesLoader withKey(String keyHex) {
        NiFiPropertiesLoader loader = new NiFiPropertiesLoader();
        loader.setKeyHex(keyHex);
        return loader;
    }

    /**
     * Sets the hexadecimal key used to unprotect properties encrypted with
     * {@link AESSensitivePropertyProvider}. If the key has already been set,
     * calling this method will throw a {@link RuntimeException}.
     *
     * @param keyHex the key in hexadecimal format
     */
    public void setKeyHex(String keyHex) {
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
            String keyHex = extractKeyFromBootstrapFile();
            return NiFiPropertiesLoader.withKey(keyHex).loadDefault();
        } catch (IOException e) {
            logger.error("Encountered an exception loading the default nifi.properties file {} with the key provided in bootstrap.conf", getDefaultFilePath(), e);
            throw e;
        }
    }

    /**
     * Returns the key (if any) used to encrypt sensitive properties, extracted from {@code $NIFI_HOME/conf/bootstrap.conf}.
     *
     * @return the key in hexadecimal format
     * @throws IOException if the file is not readable
     */
    public static String extractKeyFromBootstrapFile() throws IOException {
        return extractKeyFromBootstrapFile("");
    }

    /**
     * Returns the key (if any) used to encrypt sensitive properties, extracted from {@code $NIFI_HOME/conf/bootstrap.conf}.
     *
     * @param bootstrapPath the path to the bootstrap file
     * @return the key in hexadecimal format
     * @throws IOException if the file is not readable
     */
    public static String extractKeyFromBootstrapFile(String bootstrapPath) throws IOException {
        File expectedBootstrapFile;
        if (StringUtils.isBlank(bootstrapPath)) {
            // Guess at location of bootstrap.conf file from nifi.properties file
            String defaultNiFiPropertiesPath = getDefaultFilePath();
            File propertiesFile = new File(defaultNiFiPropertiesPath);
            File confDir = new File(propertiesFile.getParent());
            if (confDir.exists() && confDir.canRead()) {
                expectedBootstrapFile = new File(confDir, "bootstrap.conf");
            } else {
                logger.error("Cannot read from bootstrap.conf file at {} to extract encryption key -- conf/ directory is missing or permissions are incorrect", confDir.getAbsolutePath());
                throw new IOException("Cannot read from bootstrap.conf");
            }
        } else {
            expectedBootstrapFile = new File(bootstrapPath);
        }

        if (expectedBootstrapFile.exists() && expectedBootstrapFile.canRead()) {
            try (Stream<String> stream = Files.lines(Paths.get(expectedBootstrapFile.getAbsolutePath()))) {
                Optional<String> keyLine = stream.filter(l -> l.startsWith(BOOTSTRAP_KEY_PREFIX)).findFirst();
                if (keyLine.isPresent()) {
                    return keyLine.get().split("=", 2)[1];
                } else {
                    logger.warn("No encryption key present in the bootstrap.conf file at {}", expectedBootstrapFile.getAbsolutePath());
                    return "";
                }
            } catch (IOException e) {
                logger.error("Cannot read from bootstrap.conf file at {} to extract encryption key", expectedBootstrapFile.getAbsolutePath());
                throw new IOException("Cannot read from bootstrap.conf", e);
            }
        } else {
            logger.error("Cannot read from bootstrap.conf file at {} to extract encryption key -- file is missing or permissions are incorrect", expectedBootstrapFile.getAbsolutePath());
            throw new IOException("Cannot read from bootstrap.conf");
        }
    }

    private static String getDefaultFilePath() {
        String systemPath = System.getProperty(NiFiProperties.PROPERTIES_FILE_PATH);

        if (systemPath == null || systemPath.trim().isEmpty()) {
            logger.warn("The system variable {} is not set, so it is being set to '{}'", NiFiProperties.PROPERTIES_FILE_PATH, RELATIVE_PATH);
            System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, RELATIVE_PATH);
            systemPath = RELATIVE_PATH;
        }

        logger.info("Determined default nifi.properties path to be '{}'", systemPath);
        return systemPath;
    }

    private NiFiProperties loadDefault() {
        return load(getDefaultFilePath());
    }

    private static String getDefaultProviderKey() {
        try {
            return "aes/gcm/" + (Cipher.getMaxAllowedKeyLength("AES") > 128 ? "256" : "128");
        } catch (NoSuchAlgorithmException e) {
            return "aes/gcm/128";
        }
    }

    private void initializeSensitivePropertyProviderFactory() {
        sensitivePropertyProviderFactory = new AESSensitivePropertyProviderFactory(keyHex);
    }

    private SensitivePropertyProvider getSensitivePropertyProvider() {
        initializeSensitivePropertyProviderFactory();
        return sensitivePropertyProviderFactory.getProvider();
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

        Properties rawProperties = new Properties();

        InputStream inStream = null;
        try {
            inStream = new BufferedInputStream(new FileInputStream(file));
            rawProperties.load(inStream);
            logger.info("Loaded {} properties from {}", rawProperties.size(), file.getAbsolutePath());

            ProtectedNiFiProperties protectedNiFiProperties = new ProtectedNiFiProperties(rawProperties);
            return protectedNiFiProperties;
        } catch (final Exception ex) {
            logger.error("Cannot load properties file due to " + ex.getLocalizedMessage());
            throw new RuntimeException("Cannot load properties file due to "
                    + ex.getLocalizedMessage(), ex);
        } finally {
            if (null != inStream) {
                try {
                    inStream.close();
                } catch (final Exception ex) {
                    /**
                     * do nothing *
                     */
                }
            }
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
    public NiFiProperties load(File file) {
        ProtectedNiFiProperties protectedNiFiProperties = readProtectedPropertiesFromDisk(file);
        if (protectedNiFiProperties.hasProtectedKeys()) {
            Security.addProvider(new BouncyCastleProvider());
            protectedNiFiProperties.addSensitivePropertyProvider(getSensitivePropertyProvider());
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
            instance = loadDefault();
        }

        return instance;
    }
}
