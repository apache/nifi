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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SetSensitivePropertiesKeyTest {
    private static final String TEMP_FILE_PREFIX = SetSensitivePropertiesKeyTest.class.getSimpleName();

    private static final String FLOW_CONTENTS_JSON = "{\"property\":\"value\"}";

    private static final String FLOW_CONTENTS_XML = "<property><value>PROPERTY</value></property>";

    private static final String JSON_GZ = ".json.gz";

    private static final String XML_GZ = ".xml.gz";

    private static final String PROPERTIES_EXTENSION = ".properties";

    private static final String BLANK_PROPERTIES = "/blank.nifi.properties";

    private static final String POPULATED_PROPERTIES = "/populated.nifi.properties";

    private static final String LEGACY_BLANK_PROPERTIES = "/legacy-blank.nifi.properties";

    @AfterEach
    public void clearProperties() {
        System.clearProperty(SetSensitivePropertiesKey.PROPERTIES_FILE_PATH);
    }

    @Test
    public void testMainNoArguments() {
        SetSensitivePropertiesKey.main(new String[]{});
    }

    @Test
    public void testMainBlankKeyAndAlgorithm() throws IOException, URISyntaxException {
        final Path flowConfiguration = getFlowConfiguration(FLOW_CONTENTS_XML, XML_GZ);
        final Path flowConfigurationJson = getFlowConfiguration(FLOW_CONTENTS_JSON, JSON_GZ);
        final Path propertiesPath = getNiFiProperties(flowConfiguration, flowConfigurationJson, BLANK_PROPERTIES);

        System.setProperty(SetSensitivePropertiesKey.PROPERTIES_FILE_PATH, propertiesPath.toString());

        final String sensitivePropertiesKey = UUID.randomUUID().toString();
        SetSensitivePropertiesKey.main(new String[]{sensitivePropertiesKey});

        assertPropertiesKeyUpdated(propertiesPath, sensitivePropertiesKey);
    }

    @Test
    public void testMainPopulatedKeyAndAlgorithm() throws IOException, URISyntaxException {
        final Path flowConfiguration = getFlowConfiguration(FLOW_CONTENTS_XML, XML_GZ);
        final Path flowConfigurationJson = getFlowConfiguration(FLOW_CONTENTS_JSON, JSON_GZ);
        final Path propertiesPath = getNiFiProperties(flowConfiguration, flowConfigurationJson, POPULATED_PROPERTIES);

        System.setProperty(SetSensitivePropertiesKey.PROPERTIES_FILE_PATH, propertiesPath.toString());

        final String sensitivePropertiesKey = UUID.randomUUID().toString();
        SetSensitivePropertiesKey.main(new String[]{sensitivePropertiesKey});

        assertPropertiesKeyUpdated(propertiesPath, sensitivePropertiesKey);
    }

    @Test
    public void testMainLegacyBlankKeyAndAlgorithm() throws IOException, URISyntaxException {
        final Path flowConfiguration = getFlowConfiguration(FLOW_CONTENTS_XML, XML_GZ);
        final Path propertiesPath = getNiFiProperties(flowConfiguration, null, LEGACY_BLANK_PROPERTIES);

        System.setProperty(SetSensitivePropertiesKey.PROPERTIES_FILE_PATH, propertiesPath.toString());

        final String sensitivePropertiesKey = UUID.randomUUID().toString();
        SetSensitivePropertiesKey.main(new String[]{sensitivePropertiesKey});

        assertPropertiesKeyUpdated(propertiesPath, sensitivePropertiesKey);
    }

    private void assertPropertiesKeyUpdated(final Path propertiesPath, final String sensitivePropertiesKey) throws IOException {
        final Optional<String> keyProperty = Files.readAllLines(propertiesPath)
                .stream()
                .filter(line -> line.startsWith(SetSensitivePropertiesKey.PROPS_KEY))
                .findFirst();
        assertTrue(keyProperty.isPresent(), "Sensitive Key Property not found");

        final String expectedProperty = String.format("%s=%s", SetSensitivePropertiesKey.PROPS_KEY, sensitivePropertiesKey);
        assertEquals(expectedProperty, keyProperty.get(), "Sensitive Key Property not updated");
    }

    private Path getNiFiProperties(
            final Path flowConfigurationPath,
            final Path flowConfigurationJsonPath,
            String propertiesResource
    ) throws IOException, URISyntaxException {
        final Path sourcePropertiesPath = Paths.get(getResourceUrl(propertiesResource).toURI());
        final List<String> sourceProperties = Files.readAllLines(sourcePropertiesPath);
        final List<String> flowProperties = sourceProperties.stream().map(line -> {
            if (line.startsWith(SetSensitivePropertiesKey.CONFIGURATION_FILE)) {
                return line + flowConfigurationPath;
            } else if (line.startsWith(SetSensitivePropertiesKey.CONFIGURATION_JSON_FILE)) {
                return flowConfigurationJsonPath == null ? line : line + flowConfigurationJsonPath;
            } else {
                return line;
            }
        }).collect(Collectors.toList());

        final Path propertiesPath = Files.createTempFile(TEMP_FILE_PREFIX, PROPERTIES_EXTENSION);
        propertiesPath.toFile().deleteOnExit();
        Files.write(propertiesPath, flowProperties);
        return propertiesPath;
    }

    private Path getFlowConfiguration(final String contents, final String extension) throws IOException {
        final Path flowConfigurationPath = Files.createTempFile(TEMP_FILE_PREFIX, extension);
        final File flowConfigurationFile = flowConfigurationPath.toFile();
        flowConfigurationFile.deleteOnExit();

        try (final GZIPOutputStream outputStream = new GZIPOutputStream(new FileOutputStream(flowConfigurationFile))) {
            outputStream.write(contents.getBytes(StandardCharsets.UTF_8));
        }
        return flowConfigurationPath;
    }

    private URL getResourceUrl(String resource) throws FileNotFoundException {
        final URL resourceUrl = SetSensitivePropertiesKey.class.getResource(resource);
        if (resourceUrl == null) {
            throw new FileNotFoundException(String.format("Resource [%s] not found", resource));
        }
        return resourceUrl;
    }
}
