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

import org.apache.nifi.properties.SensitivePropertyProvider;
import org.apache.nifi.properties.SensitivePropertyProviderFactory;
import org.apache.nifi.properties.scheme.ProtectionScheme;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class XmlFileTransformerTest {
    private static final String AUTHORIZERS_RESOURCE = "/transformer/authorizers-unprotected.xml";

    private static final String AUTHORIZERS_TRANSFORMED = "authorizers-transformed.xml";

    private static final String PROVIDER_IDENTIFIER_KEY = "protected";

    private static final String ENCRYPTED = "ENCRYPTED";

    private static final String UNPROTECTED = "UNPROTECTED";

    @TempDir
    private Path tempDir;

    @Mock
    private SensitivePropertyProvider inputSensitivePropertyProvider;

    @Mock
    private SensitivePropertyProviderFactory sensitivePropertyProviderFactory;

    @Mock
    private SensitivePropertyProvider sensitivePropertyProvider;

    @Mock
    private ProtectionScheme protectionScheme;

    @Test
    void testTransform() throws URISyntaxException, IOException {
        final Path authorizersPath = getResourcePath();

        final Path tempAuthorizersPath = tempDir.resolve(authorizersPath.getFileName());
        Files.copy(authorizersPath, tempAuthorizersPath);

        final Path outputAuthorizesPath = tempDir.resolve(AUTHORIZERS_TRANSFORMED);

        when(sensitivePropertyProviderFactory.getProvider(eq(protectionScheme))).thenReturn(sensitivePropertyProvider);
        final XmlFileTransformer transformer = new XmlFileTransformer(inputSensitivePropertyProvider, sensitivePropertyProviderFactory, protectionScheme);

        when(sensitivePropertyProvider.getIdentifierKey()).thenReturn(PROVIDER_IDENTIFIER_KEY);
        when(sensitivePropertyProvider.protect(eq(UNPROTECTED), any())).thenReturn(ENCRYPTED);

        transformer.transform(tempAuthorizersPath, outputAuthorizesPath);

        final String outputAuthorizers = Files.readString(outputAuthorizesPath);
        assertTrue(outputAuthorizers.contains(ENCRYPTED), "encrypted property not found");

        verifyNoInteractions(inputSensitivePropertyProvider);
    }

    private Path getResourcePath() throws URISyntaxException {
        final URL resourceUrl = Objects.requireNonNull(getClass().getResource(AUTHORIZERS_RESOURCE), "Resource not found");
        return Paths.get(resourceUrl.toURI());
    }
}
