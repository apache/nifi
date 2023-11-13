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
package org.apache.nifi.minifi.util;

import static org.apache.nifi.minifi.commons.utils.PropertyUtil.resolvePropertyValue;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.nifi.util.NiFiProperties;

/**
 * Extends NiFi properties functionality with System and Environment property override possibility. The property resolution also works with
 * dots and hyphens that are not supported in some shells.
 */
public class MultiSourceMinifiProperties extends NiFiProperties {

    private final Properties properties = new Properties();

    public static MultiSourceMinifiProperties getInstance() {
        return new MultiSourceMinifiProperties();
    }

    private MultiSourceMinifiProperties() {
        readFromPropertiesFile();
    }

    @Override
    public Set<String> getPropertyKeys() {
        return Stream.of(System.getProperties().stringPropertyNames(), System.getenv().keySet(), properties.stringPropertyNames())
            .flatMap(Set::stream)
            .collect(Collectors.toSet());
    }

    @Override
    public int size() {
        return getPropertyKeys().size();
    }

    @Override
    public String getProperty(String key) {
        return resolvePropertyValue(key, System.getProperties())
            .or(() -> resolvePropertyValue(key, System.getenv()))
            .orElseGet(() -> properties.getProperty(key));
    }

    private void readFromPropertiesFile() {
        String propertiesFilePath = System.getProperty(NiFiProperties.PROPERTIES_FILE_PATH);

        if (propertiesFilePath != null) {
            File propertiesFile = new File(propertiesFilePath.trim());

            if (!propertiesFile.exists()) {
                throw new RuntimeException("Properties file doesn't exist '" + propertiesFile.getAbsolutePath() + "'");
            }

            if (!propertiesFile.canRead()) {
                throw new RuntimeException("Properties file exists but cannot be read '" + propertiesFile.getAbsolutePath() + "'");
            }

            try (InputStream inStream = new BufferedInputStream(new FileInputStream(propertiesFile))) {
                properties.load(inStream);
            } catch (Exception ex) {
                throw new RuntimeException("Cannot load properties file due to " + ex.getLocalizedMessage(), ex);
            }
        }
    }
}
