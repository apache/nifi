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
package org.apache.nifi.flow.encryptor.command;

import org.apache.nifi.stream.io.GZIPOutputStream;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SetSensitivePropertiesKeyTest {
    private static final String FLOW_CONTENTS = "<property><value>PROPERTY</value></property>";

    @After
    public void clearProperties() {
        System.clearProperty(SetSensitivePropertiesKey.PROPERTIES_FILE_PATH);
    }

    @Test
    public void testMainNoArguments() {
        SetSensitivePropertiesKey.main(new String[]{});
    }

    @Test
    public void testMainBlankKeyAndAlgorithm() throws IOException, URISyntaxException {
        final Path flowConfiguration = getFlowConfiguration();
        final Path propertiesPath = getNiFiProperties(flowConfiguration, "/blank.nifi.properties");

        System.setProperty(SetSensitivePropertiesKey.PROPERTIES_FILE_PATH, propertiesPath.toString());

        final String sensitivePropertiesKey = UUID.randomUUID().toString();
        SetSensitivePropertiesKey.main(new String[]{sensitivePropertiesKey});

        assertPropertiesKeyUpdated(propertiesPath, sensitivePropertiesKey);
        assertTrue("Flow Configuration not found", flowConfiguration.toFile().exists());
    }

    @Test
    public void testMainPopulatedKeyAndAlgorithm() throws IOException, URISyntaxException {
        final Path flowConfiguration = getFlowConfiguration();
        final Path propertiesPath = getNiFiProperties(flowConfiguration, "/populated.nifi.properties");

        System.setProperty(SetSensitivePropertiesKey.PROPERTIES_FILE_PATH, propertiesPath.toString());

        final String sensitivePropertiesKey = UUID.randomUUID().toString();
        SetSensitivePropertiesKey.main(new String[]{sensitivePropertiesKey});

        assertPropertiesKeyUpdated(propertiesPath, sensitivePropertiesKey);
        assertTrue("Flow Configuration not found", flowConfiguration.toFile().exists());
    }

    private void assertPropertiesKeyUpdated(final Path propertiesPath, final String sensitivePropertiesKey) throws IOException {
        final Optional<String> keyProperty = Files.readAllLines(propertiesPath)
                .stream()
                .filter(line -> line.startsWith(SetSensitivePropertiesKey.PROPS_KEY))
                .findFirst();
        assertTrue("Sensitive Key Property not found", keyProperty.isPresent());

        final String expectedProperty = String.format("%s=%s", SetSensitivePropertiesKey.PROPS_KEY, sensitivePropertiesKey);
        assertEquals("Sensitive Key Property not updated", expectedProperty, keyProperty.get());
    }

    private Path getNiFiProperties(final Path flowConfigurationPath, String propertiesResource) throws IOException, URISyntaxException {
        final Path sourcePropertiesPath = Paths.get(SetSensitivePropertiesKey.class.getResource(propertiesResource).toURI());
        final List<String> sourceProperties = Files.readAllLines(sourcePropertiesPath);
        final List<String> flowProperties = sourceProperties.stream().map(line -> {
            if (line.startsWith(SetSensitivePropertiesKey.CONFIGURATION_FILE)) {
                return line + flowConfigurationPath.toString();
            } else {
                return line;
            }
        }).collect(Collectors.toList());

        final Path propertiesPath = Files.createTempFile(SetSensitivePropertiesKey.class.getSimpleName(), ".properties");
        propertiesPath.toFile().deleteOnExit();
        Files.write(propertiesPath, flowProperties);
        return propertiesPath;
    }

    private Path getFlowConfiguration() throws IOException {
        final Path flowConfigurationPath = Files.createTempFile(SetSensitivePropertiesKey.class.getSimpleName(), ".xml.gz");
        final File flowConfigurationFile = flowConfigurationPath.toFile();
        flowConfigurationFile.deleteOnExit();

        try (final GZIPOutputStream outputStream = new GZIPOutputStream(new FileOutputStream(flowConfigurationFile))) {
            outputStream.write(FLOW_CONTENTS.getBytes(StandardCharsets.UTF_8));
        }
        return flowConfigurationPath;
    }
}
