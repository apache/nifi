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
import java.security.NoSuchAlgorithmException;
import java.security.Security;
import java.util.Properties;
import javax.crypto.Cipher;

import org.apache.nifi.properties.sensitive.ProtectedNiFiProperties;
import org.apache.nifi.security.kms.CryptoUtils;
import org.apache.nifi.util.NiFiProperties;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NiFiPropertiesLoader {

    private static final Logger logger = LoggerFactory.getLogger(NiFiPropertiesLoader.class);

    private NiFiProperties instance;
    private String keyHex;

    // Future enhancement: allow for external registration of new providers
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
     * {@link org.apache.nifi.properties.sensitive.SensitivePropertyProvider}. If the key has already been set,
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
            logger.error("Encountered an exception loading the default nifi.properties file {} with the key provided in bootstrap.conf", CryptoUtils.getDefaultFilePath(), e);
            throw e;
        }
    }

    /**
     * Returns the key (if any) used to encrypt sensitive properties, extracted from {@code $NIFI_HOME/conf/bootstrap.conf}.
     *
     * @deprecated
     * Use {@link CryptoUtils#extractKeyFromBootstrapFile()} instead.
     *
     * @return the key in hexadecimal format
     * @throws IOException if the file is not readable
     */
    @Deprecated
    public static String extractKeyFromBootstrapFile() throws IOException {
        // TODO: Replace all existing uses with direct reference to CryptoUtils
        return extractKeyFromBootstrapFile("");
    }

    /**
     * Returns the key (if any) used to encrypt sensitive properties, extracted from {@code $NIFI_HOME/conf/bootstrap.conf}.
     *
     * @deprecated
     * Use {@link CryptoUtils#extractKeyFromBootstrapFile(String)} instead.
     *
     * @param bootstrapPath the path to the bootstrap file
     * @return the key in hexadecimal format
     * @throws IOException if the file is not readable
     */
    @Deprecated
    public static String extractKeyFromBootstrapFile(String bootstrapPath) throws IOException {
        // TODO: Replace all existing uses with direct reference to CryptoUtils
        return CryptoUtils.extractKeyFromBootstrapFile(bootstrapPath);
    }

    private NiFiProperties loadDefault() {
        return load(CryptoUtils.getDefaultFilePath());
    }

    static String getDefaultProviderKey() {
        try {
            return "aes/gcm/" + (Cipher.getMaxAllowedKeyLength("AES") > 128 ? "256" : "128");
        } catch (NoSuchAlgorithmException e) {
            return "aes/gcm/128";
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

            ProtectedNiFiProperties protectedNiFiProperties = new ProtectedNiFiProperties(rawProperties, keyHex);
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
     * appropriate {@link org.apache.nifi.properties.sensitive.SensitivePropertyProvider} to unprotect them
     * transparently.
     *
     * @param file the File containing the serialized properties
     * @return the NiFiProperties instance
     */
    public NiFiProperties load(File file) {
        ProtectedNiFiProperties protectedNiFiProperties = readProtectedPropertiesFromDisk(file);
        if (protectedNiFiProperties.hasProtectedKeys()) {
            Security.addProvider(new BouncyCastleProvider());
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
