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
package org.apache.nifi.toolkit.config.transformer;

import org.apache.nifi.properties.ApplicationProperties;
import org.apache.nifi.properties.SensitivePropertyProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ApplicationPropertiesFileTransformerTest {
    private static final String UNPROTECTED_NIFI_PROPERTIES = "/transformer/application-unprotected-nifi.properties";

    private static final String PROPERTIES_PROTECTED_RESOURCE = "/transformer/application-protected-nifi.properties";

    private static final String PROPERTIES_NON_SENSITIVE_RESOURCE = "/transformer/application-non-sensitive-nifi.properties";

    private static final String PROPERTIES_TRANSFORMED = "transformed-nifi.properties";

    private static final String PROVIDER_IDENTIFIER_KEY = "mocked-provider";

    private static final String SENSITIVE_PROPERTY_NAME = "nifi.security.keystorePasswd";

    private static final String SENSITIVE_PROPERTY_NAME_PROTECTED = String.format("%s.protected", SENSITIVE_PROPERTY_NAME);

    private static final Set<String> SENSITIVE_PROPERTY_NAMES = Collections.singleton(SENSITIVE_PROPERTY_NAME);

    private static final String ENCRYPTED = "ENCRYPTED";

    private static final String UNPROTECTED = "UNPROTECTED";

    @TempDir
    private Path tempDir;

    @Mock
    private SensitivePropertyProvider sensitivePropertyProvider;

    @Test
    void testTransformUnprotectedProperty() throws URISyntaxException, IOException {
        final Path propertiesPath = getResourcePath(UNPROTECTED_NIFI_PROPERTIES);

        final Path tempPropertiesPath = tempDir.resolve(propertiesPath.getFileName());
        Files.copy(propertiesPath, tempPropertiesPath);

        final Path outputPropertiesPath = tempDir.resolve(PROPERTIES_TRANSFORMED);

        final ApplicationProperties applicationProperties = new ApplicationProperties(Collections.emptyMap());
        final FileTransformer transformer = new ApplicationPropertiesFileTransformer(applicationProperties, sensitivePropertyProvider, SENSITIVE_PROPERTY_NAMES);

        when(sensitivePropertyProvider.getIdentifierKey()).thenReturn(PROVIDER_IDENTIFIER_KEY);
        when(sensitivePropertyProvider.protect(eq(UNPROTECTED), any())).thenReturn(ENCRYPTED);

        transformer.transform(tempPropertiesPath, outputPropertiesPath);

        assertProtectedPropertyFound(outputPropertiesPath);
    }

    @Test
    void testTransformProtectedProperty() throws URISyntaxException, IOException {
        final Path propertiesPath = getResourcePath(PROPERTIES_PROTECTED_RESOURCE);

        final Path tempPropertiesPath = tempDir.resolve(propertiesPath.getFileName());
        Files.copy(propertiesPath, tempPropertiesPath);

        final Path outputPropertiesPath = tempDir.resolve(PROPERTIES_TRANSFORMED);

        final Properties sourceProperties = new Properties();
        sourceProperties.setProperty(SENSITIVE_PROPERTY_NAME, UNPROTECTED);
        final ApplicationProperties applicationProperties = new ApplicationProperties(sourceProperties);
        final FileTransformer transformer = new ApplicationPropertiesFileTransformer(applicationProperties, sensitivePropertyProvider, SENSITIVE_PROPERTY_NAMES);

        when(sensitivePropertyProvider.getIdentifierKey()).thenReturn(PROVIDER_IDENTIFIER_KEY);
        when(sensitivePropertyProvider.protect(eq(UNPROTECTED), any())).thenReturn(ENCRYPTED);

        transformer.transform(tempPropertiesPath, outputPropertiesPath);

        assertProtectedPropertyFound(outputPropertiesPath);
    }

    @Test
    void testTransformNonSensitiveProperties() throws URISyntaxException, IOException {
        final Path propertiesPath = getResourcePath(PROPERTIES_NON_SENSITIVE_RESOURCE);

        final Path tempPropertiesPath = tempDir.resolve(propertiesPath.getFileName());
        Files.copy(propertiesPath, tempPropertiesPath);

        final Path outputPropertiesPath = tempDir.resolve(PROPERTIES_TRANSFORMED);

        final ApplicationProperties applicationProperties = new ApplicationProperties(Collections.emptyMap());
        final FileTransformer transformer = new ApplicationPropertiesFileTransformer(applicationProperties, sensitivePropertyProvider, SENSITIVE_PROPERTY_NAMES);

        transformer.transform(tempPropertiesPath, outputPropertiesPath);

        verifyNoInteractions(sensitivePropertyProvider);

        final Properties inputProperties = loadProperties(propertiesPath);
        final Properties outputProperties = loadProperties(outputPropertiesPath);

        assertEquals(inputProperties, outputProperties);
    }

    private void assertProtectedPropertyFound(final Path outputPropertiesPath) throws IOException {
        final Properties outputProperties = loadProperties(outputPropertiesPath);

        final String sensitivePropertyValue = outputProperties.getProperty(SENSITIVE_PROPERTY_NAME);
        assertEquals(ENCRYPTED, sensitivePropertyValue);

        final String protectedSensitivePropertyValue = outputProperties.getProperty(SENSITIVE_PROPERTY_NAME_PROTECTED);
        assertEquals(PROVIDER_IDENTIFIER_KEY, protectedSensitivePropertyValue);
    }

    private Properties loadProperties(final Path resourcePath) throws IOException {
        final Properties properties = new Properties();
        try (InputStream propertiesStream = Files.newInputStream(resourcePath)) {
            properties.load(propertiesStream);
        }
        return properties;
    }

    private Path getResourcePath(final String resource) throws URISyntaxException {
        final URL resourceUrl = Objects.requireNonNull(getClass().getResource(resource), String.format("Resource [%s] not found", resource));
        return Paths.get(resourceUrl.toURI());
    }
}
