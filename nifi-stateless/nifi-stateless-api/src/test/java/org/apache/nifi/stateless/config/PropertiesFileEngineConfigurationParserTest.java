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
package org.apache.nifi.stateless.config;

import org.apache.nifi.stateless.engine.StatelessEngineConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class PropertiesFileEngineConfigurationParserTest {
    private PropertiesFileEngineConfigurationParser parser;

    private static Path narDirectory;

    private static Path workingDirectory;

    @BeforeAll
    public static void setDirectories() throws IOException {
        narDirectory = Files.createTempDirectory(PropertiesFileEngineConfigurationParserTest.class.getSimpleName());
        workingDirectory = Files.createTempDirectory(PropertiesFileEngineConfigurationParserTest.class.getSimpleName());
    }

    @AfterAll
    public static void deleteDirectories() throws IOException {
        Files.deleteIfExists(narDirectory);
        Files.deleteIfExists(workingDirectory);
    }

    @BeforeEach
    public void setParser() {
        parser = new PropertiesFileEngineConfigurationParser();
    }

    @Test
    public void testParseEngineConfigurationRequiredProperties() throws IOException, StatelessConfigurationException {
        final Properties properties = getRequiredProperties();
        final File propertiesFile = getPropertiesFile(properties);

        final StatelessEngineConfiguration configuration = parser.parseEngineConfiguration(propertiesFile);
        assertNotNull(configuration);
        assertEquals(narDirectory.toFile(), configuration.getNarDirectory());
        assertEquals(workingDirectory.toFile(), configuration.getWorkingDirectory());
    }

    @Test
    public void testParseEngineConfigurationRandomSensitivePropsKey() throws IOException, StatelessConfigurationException {
        final Properties properties = getRequiredProperties();
        final File propertiesFile = getPropertiesFile(properties);

        final StatelessEngineConfiguration configuration = parser.parseEngineConfiguration(propertiesFile);
        assertNotNull(configuration);

        final String sensitivePropsKey = configuration.getSensitivePropsKey();
        assertNotNull(sensitivePropsKey);
        assertFalse(sensitivePropsKey.isEmpty());

        final StatelessEngineConfiguration reloadedConfiguration = parser.parseEngineConfiguration(propertiesFile);
        assertEquals(sensitivePropsKey, reloadedConfiguration.getSensitivePropsKey());
    }

    private Properties getRequiredProperties() {
        final Properties properties = new Properties();

        properties.setProperty("nifi.stateless.nar.directory", narDirectory.toString());
        properties.setProperty("nifi.stateless.working.directory", workingDirectory.toString());

        return properties;
    }

    private File getPropertiesFile(final Properties properties) throws IOException {
        final File file = File.createTempFile(getClass().getSimpleName(), ".properties");
        file.deleteOnExit();

        try (final OutputStream outputStream = new FileOutputStream(file)) {
            properties.store(outputStream, null);
        }

        return file;
    }
}
