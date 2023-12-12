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

import static java.lang.String.format;
import static org.apache.nifi.minifi.commons.utils.SensitivePropertyUtils.MINIFI_BOOTSTRAP_SENSITIVE_KEY;
import static org.apache.nifi.minifi.commons.utils.SensitivePropertyUtils.getFormattedKey;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import org.apache.nifi.properties.AesGcmSensitivePropertyProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BootstrapPropertiesLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(BootstrapPropertiesLoader.class);

    public static BootstrapProperties load(File file) {
        ProtectedBootstrapProperties protectedProperties = loadProtectedProperties(file);
        if (protectedProperties.hasProtectedKeys()) {
            String sensitiveKey = protectedProperties.getApplicationProperties().getProperty(MINIFI_BOOTSTRAP_SENSITIVE_KEY);
            validateSensitiveKeyProperty(sensitiveKey);
            String keyHex = getFormattedKey(sensitiveKey);
            protectedProperties.addSensitivePropertyProvider(new AesGcmSensitivePropertyProvider(keyHex));
        }
        return protectedProperties.getUnprotectedProperties();
    }

    public static ProtectedBootstrapProperties loadProtectedProperties(File file) {
        if (file == null || !file.exists() || !file.canRead()) {
            throw new IllegalArgumentException(format("Application Properties [%s] not found", file));
        }

        LOGGER.info("Loading Bootstrap Properties [{}]", file);
        DuplicateDetectingProperties rawProperties = new DuplicateDetectingProperties();

        try (InputStream inputStream = new BufferedInputStream(new FileInputStream(file))) {
            rawProperties.load(inputStream);
        } catch (Exception e) {
            throw new RuntimeException(format("Loading Application Properties [%s] failed", file), e);
        }

        if (!rawProperties.redundantKeySet().isEmpty()) {
            LOGGER.warn("Duplicate property keys with the same value were detected in the properties file: {}", String.join(", ", rawProperties.redundantKeySet()));
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

        return new ProtectedBootstrapProperties(properties);
    }

    private static void validateSensitiveKeyProperty(String sensitiveKey) {
        if (sensitiveKey == null || sensitiveKey.trim().isEmpty()) {
            throw new IllegalArgumentException(format("bootstrap.conf contains protected properties but %s is not found", MINIFI_BOOTSTRAP_SENSITIVE_KEY));
        }
    }
}
