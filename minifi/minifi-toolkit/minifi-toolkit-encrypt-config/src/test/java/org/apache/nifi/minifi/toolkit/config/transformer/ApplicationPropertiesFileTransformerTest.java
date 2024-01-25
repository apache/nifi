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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import org.apache.nifi.properties.ApplicationProperties;
import org.apache.nifi.properties.SensitivePropertyProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ApplicationPropertiesFileTransformerTest {

    private static final String UNPROTECTED_BOOTSTRAP_CONF = "/transformer/unprotected.conf";
    private static final String PROTECTED_BOOTSTRAP_PROPERTIES = "/transformer/protected.conf";
    private static final String PROPERTIES_TRANSFORMED = "transformed.conf";
    private static final String SENSITIVE_PROPERTY_NAME_1 = "property1";
    private static final String SENSITIVE_PROPERTY_NAME_2 = "property2";
    private static final String SENSITIVE_PROPERTY_NAME_3 = "property3";
    private static final String PROVIDER_IDENTIFIER_KEY = "mocked-provider";
    private static final String UNPROTECTED = "UNPROTECTED";
    private static final String ENCRYPTED = "ENCRYPTED";

    private static final Set<String> SENSITIVE_PROPERTY_NAMES = Set.of(SENSITIVE_PROPERTY_NAME_1, SENSITIVE_PROPERTY_NAME_2, SENSITIVE_PROPERTY_NAME_3);

    @TempDir
    private Path tempDir;

    @Mock
    private SensitivePropertyProvider sensitivePropertyProvider;

    @Test
    void shouldTransformProperties() throws URISyntaxException, IOException {
        Path propertiesPath = getResourcePath(UNPROTECTED_BOOTSTRAP_CONF);

        Path tempPropertiesPath = tempDir.resolve(propertiesPath.getFileName());
        Files.copy(propertiesPath, tempPropertiesPath);

        Path outputPropertiesPath = tempDir.resolve(PROPERTIES_TRANSFORMED);
        ApplicationProperties applicationProperties = new ApplicationProperties(Map.of(SENSITIVE_PROPERTY_NAME_3, UNPROTECTED));
        FileTransformer transformer = new ApplicationPropertiesFileTransformer(applicationProperties, sensitivePropertyProvider, SENSITIVE_PROPERTY_NAMES);

        when(sensitivePropertyProvider.getIdentifierKey()).thenReturn(PROVIDER_IDENTIFIER_KEY);
        when(sensitivePropertyProvider.protect(eq(UNPROTECTED), any())).thenReturn(ENCRYPTED);

        transformer.transform(tempPropertiesPath, outputPropertiesPath);

        Properties expectedProperties = loadProperties(getResourcePath(PROTECTED_BOOTSTRAP_PROPERTIES));
        Properties resultProperties = loadProperties(outputPropertiesPath);

        assertEquals(expectedProperties, resultProperties);
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