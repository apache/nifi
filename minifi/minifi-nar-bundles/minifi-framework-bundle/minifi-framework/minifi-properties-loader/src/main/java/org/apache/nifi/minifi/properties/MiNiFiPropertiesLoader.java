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
package org.apache.nifi.minifi.properties;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import org.apache.nifi.properties.AesGcmSensitivePropertyProvider;
import org.apache.nifi.util.NiFiBootstrapUtils;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MiNiFiPropertiesLoader {

    private static final Logger logger = LoggerFactory.getLogger(MiNiFiPropertiesLoader.class);
    private static final String DEFAULT_APPLICATION_PROPERTIES_FILE_PATH = NiFiBootstrapUtils.getDefaultApplicationPropertiesFilePath();

    private NiFiProperties instance;
    private String keyHex;

    public MiNiFiPropertiesLoader(String keyHex) {
        this.keyHex = keyHex;
    }

    /**
     * Returns a {@link ProtectedMiNiFiProperties} instance loaded from the
     * serialized form in the file. Responsible for actually reading from disk
     * and deserializing the properties. Returns a protected instance to allow
     * for decryption operations.
     *
     * @param file the file containing serialized properties
     * @return the ProtectedMiNiFiProperties instance
     */
    ProtectedMiNiFiProperties loadProtectedProperties(File file) {
        if (file == null || !file.exists() || !file.canRead()) {
            throw new IllegalArgumentException(String.format("Application Properties [%s] not found", file));
        }

        logger.info("Loading Application Properties [{}]", file);
        DuplicateDetectingProperties rawProperties = new DuplicateDetectingProperties();

        try (InputStream inputStream = new BufferedInputStream(new FileInputStream(file))) {
            rawProperties.load(inputStream);
        } catch (Exception e) {
            throw new RuntimeException(String.format("Loading Application Properties [%s] failed", file), e);
        }

        if (!rawProperties.redundantKeySet().isEmpty()) {
            logger.warn("Duplicate property keys with the same value were detected in the properties file: {}", String.join(", ", rawProperties.redundantKeySet()));
        }
        if (!rawProperties.duplicateKeySet().isEmpty()) {
            throw new IllegalArgumentException("Duplicate property keys with different values were detected in the properties file: " + String.join(", ", rawProperties.duplicateKeySet()));
        }

        Properties properties = new Properties();
        rawProperties.stringPropertyNames()
            .forEach(key -> {
                String property = rawProperties.getProperty(key);
                properties.setProperty(key, property.trim());
            });
        return new ProtectedMiNiFiProperties(properties);
    }

    /**
     * Returns an instance of {@link NiFiProperties} loaded from the provided
     * {@link File}. If any properties are protected, will attempt to use the
     * {@link AesGcmSensitivePropertyProvider} to unprotect them
     * transparently.
     *
     * @param file the File containing the serialized properties
     * @return the NiFiProperties instance
     */
    public NiFiProperties load(File file) {
        ProtectedMiNiFiProperties protectedProperties = loadProtectedProperties(file);
        if (protectedProperties.hasProtectedKeys()) {
            protectedProperties.addSensitivePropertyProvider(new AesGcmSensitivePropertyProvider(keyHex));
        }
        return new MultiSourceMinifiProperties(protectedProperties.getUnprotectedPropertiesAsMap());
    }

    /**
     * Returns an instance of {@link NiFiProperties}. If the path is empty, this
     * will load the default properties file as specified by
     * {@code NiFiProperties.PROPERTY_FILE_PATH}.
     *
     * @param path the path of the serialized properties file
     * @return the NiFiProperties instance
     * @see MiNiFiPropertiesLoader#load(File)
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
     * NOTE: This method is used reflectively by the process which starts MiNiFi
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

    private NiFiProperties loadDefault() {
        return load(DEFAULT_APPLICATION_PROPERTIES_FILE_PATH);
    }

}
