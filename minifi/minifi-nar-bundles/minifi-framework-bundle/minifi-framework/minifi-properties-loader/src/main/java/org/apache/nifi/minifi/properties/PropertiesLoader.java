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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface PropertiesLoader {

    static final Logger logger = LoggerFactory.getLogger(PropertiesLoader.class);

    static Properties load(File file, String propertiesType) {
        if (file == null || !file.exists() || !file.canRead()) {
            throw new IllegalArgumentException(String.format("{} Properties [%s] not found", propertiesType, file));
        }

        logger.info("Loading {} Properties [{}]", propertiesType, file);
        DuplicateDetectingProperties rawProperties = new DuplicateDetectingProperties();

        try (InputStream inputStream = new BufferedInputStream(new FileInputStream(file))) {
            rawProperties.load(inputStream);
        } catch (Exception e) {
            throw new RuntimeException(String.format("Loading {} Properties [%s] failed", propertiesType, file), e);
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
        return properties;
    }
}
