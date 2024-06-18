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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class NiFiRegistryPropertiesLoader {

    private static final Logger logger = LoggerFactory.getLogger(NiFiRegistryPropertiesLoader.class);

    /**
     * Returns an instance of {@link NiFiRegistryProperties} loaded from the provided
     * {@link File}.
     *
     * @param file the File containing the serialized properties
     * @return the NiFiProperties instance
     */
    public NiFiRegistryProperties load(final File file) {
        if (file == null || !file.exists() || !file.canRead()) {
            throw new IllegalArgumentException("NiFi Registry properties file missing or unreadable");
        }

        final Properties rawProperties = new Properties();
        try (final FileReader reader = new FileReader(file)) {
            rawProperties.load(reader);
            final NiFiRegistryProperties innerProperties = new NiFiRegistryProperties(rawProperties);
            logger.info("Loaded {} properties from {}", rawProperties.size(), file.getAbsolutePath());
            return innerProperties;
        } catch (final IOException ioe) {
            throw new RuntimeException("Failed to load Application Properties", ioe);
        }
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
