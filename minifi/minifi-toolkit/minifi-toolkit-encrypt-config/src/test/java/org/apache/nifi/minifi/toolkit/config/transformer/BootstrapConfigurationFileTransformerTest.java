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

package org.apache.nifi.minifi.toolkit.config.transformer;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BootstrapConfigurationFileTransformerTest {
    private static final String BOOTSTRAP_ROOT_KEY_PROPERTY = "minifi.bootstrap.sensitive.key";
    private static final String MOCK_KEY = "mockKey";
    private static final String BOOTSTRAP_CONF_FILE_WITHOUT_KEY = "/transformer/bootstrap_without_key.conf";
    private static final String BOOTSTRAP_CONF_TRANSFORMED= "transformed.conf";

    @TempDir
    private Path tempDir;

    @Test
    void shouldWriteRootPropertyKeyIfItIsNotPresent() throws URISyntaxException, IOException {
        Path propertiesPath = getResourcePath(BOOTSTRAP_CONF_FILE_WITHOUT_KEY);

        Path tempPropertiesPath = tempDir.resolve(propertiesPath.getFileName());
        Files.copy(propertiesPath, tempPropertiesPath);

        Path outputPropertiesPath = tempDir.resolve(BOOTSTRAP_CONF_TRANSFORMED);

        BootstrapConfigurationFileTransformer transformer = new BootstrapConfigurationFileTransformer(BOOTSTRAP_ROOT_KEY_PROPERTY, MOCK_KEY);
        transformer.transform(tempPropertiesPath, outputPropertiesPath);

        Properties properties = loadProperties(outputPropertiesPath);
        assertEquals(MOCK_KEY, properties.get(BOOTSTRAP_ROOT_KEY_PROPERTY));
    }

    private Properties loadProperties(Path resourcePath) throws IOException {
        Properties properties = new Properties();
        try (InputStream propertiesStream = Files.newInputStream(resourcePath)) {
            properties.load(propertiesStream);
        }
        return properties;
    }

    private Path getResourcePath(String resource) throws URISyntaxException {
        final URL resourceUrl = Objects.requireNonNull(getClass().getResource(resource), String.format("Resource [%s] not found", resource));
        return Paths.get(resourceUrl.toURI());
    }
}