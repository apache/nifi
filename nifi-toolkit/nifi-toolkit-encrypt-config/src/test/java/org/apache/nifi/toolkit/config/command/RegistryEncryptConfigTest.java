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
package org.apache.nifi.toolkit.config.command;

import org.apache.nifi.properties.ApplicationPropertiesProtector;
import org.apache.nifi.xml.processing.parsers.DocumentProvider;
import org.apache.nifi.xml.processing.parsers.StandardDocumentProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RegistryEncryptConfigTest {

    private static final String BOOTSTRAP_CONF_RESOURCE = "/command/bootstrap-registry.conf";

    private static final String APPLICATION_PROPERTIES_RESOURCE = "/command/nifi-registry.properties";

    private static final String AUTHORIZERS_UNPROTECTED_RESOURCE = "/transformer/authorizers-unprotected.xml";

    private static final String IDENTITY_PROVIDERS_UNPROTECTED_RESOURCE = "/command/identity-providers-unprotected.xml";

    private static final String APPLICATION_PROPERTIES_TRANSFORMED = "transformed.nifi.properties";

    private static final String AUTHORIZERS_TRANSFORMED = "transformed.authorizers.xml";

    private static final String IDENTITY_PROVIDERS_TRANSFORMED = "transformed.identity-providers.xml";

    private static final String BOOTSTRAP_CONF_TRANSFORMED = "transformed.bootstrap-registry.conf";

    private static final String BOOTSTRAP_HEXADECIMAL_KEY = "012345678901234567890123456789012345678901234567890123456789ABCD";

    private static final String PROTECTED_PROPERTY_NAME = "Random Password";

    private static final String SENSITIVE_APPLICATION_PROPERTY = "nifi.registry.security.keystorePasswd";

    private static final String PROTECTED_IDENTIFIER = "aes/gcm/256";

    @TempDir
    private Path tempDir;

    @Test
    void testRun() {
        final String[] arguments = new String[0];

        final int status = run(arguments);
        assertEquals(CommandLine.ExitCode.OK, status);
    }

    @Test
    void testHelpRequested() {
        final String[] arguments = {"-h"};

        final int status = run(arguments);
        assertEquals(CommandLine.ExitCode.OK, status);
    }

    @Test
    void testApplicationPropertiesNotFound() {
        final Path propertiesPath = tempDir.resolve(APPLICATION_PROPERTIES_TRANSFORMED);

        final String[] arguments = {
                "-k", BOOTSTRAP_HEXADECIMAL_KEY,
                "-r", propertiesPath.toString()
        };

        final int status = run(arguments);
        assertEquals(CommandLine.ExitCode.SOFTWARE, status);
    }

    @Test
    void testApplicationProperties() throws URISyntaxException, IOException {
        final Path propertiesPath = getResourcePath(APPLICATION_PROPERTIES_RESOURCE);
        final Path outputPropertiesPath = tempDir.resolve(APPLICATION_PROPERTIES_TRANSFORMED);

        final String[] arguments = {
                "-k", BOOTSTRAP_HEXADECIMAL_KEY,
                "-r", propertiesPath.toString(),
                "-R", outputPropertiesPath.toString()
        };

        final int status = run(arguments);
        assertEquals(CommandLine.ExitCode.OK, status);

        assertProtectedPropertyFound(outputPropertiesPath);
    }

    @Test
    void testApplicationPropertiesWithBootstrapConf() throws URISyntaxException, IOException {
        final Path propertiesPath = getResourcePath(APPLICATION_PROPERTIES_RESOURCE);
        final Path outputPropertiesPath = tempDir.resolve(APPLICATION_PROPERTIES_TRANSFORMED);

        final Path bootstrapConfPath = getResourcePath(BOOTSTRAP_CONF_RESOURCE);
        final Path outputBootstrapConfPath = tempDir.resolve(BOOTSTRAP_CONF_TRANSFORMED);

        final String[] arguments = {
                "-k", BOOTSTRAP_HEXADECIMAL_KEY,
                "-b", bootstrapConfPath.toString(),
                "-B", outputBootstrapConfPath.toString(),
                "-r", propertiesPath.toString(),
                "-R", outputPropertiesPath.toString()
        };

        final int status = run(arguments);
        assertEquals(CommandLine.ExitCode.OK, status);

        assertProtectedPropertyFound(outputPropertiesPath);
        assertRootKeyFound(outputBootstrapConfPath);
    }

    @Test
    void testAuthorizersWithKey() throws URISyntaxException, IOException {
        final Path authorizersPath = getResourcePath(AUTHORIZERS_UNPROTECTED_RESOURCE);
        final Path outputAuthorizersPath = tempDir.resolve(AUTHORIZERS_TRANSFORMED);

        final String[] arguments = {
                "-k", BOOTSTRAP_HEXADECIMAL_KEY,
                "-a", authorizersPath.toString(),
                "-u", outputAuthorizersPath.toString()
        };

        final int status = run(arguments);
        assertEquals(CommandLine.ExitCode.OK, status);

        assertEncryptedPropertyFound(outputAuthorizersPath);
    }

    @Test
    void testIdentityProvidersWithKey() throws URISyntaxException, IOException {
        final Path identityProvidersPath = getResourcePath(IDENTITY_PROVIDERS_UNPROTECTED_RESOURCE);
        final Path outputIdentityProvidersPath = tempDir.resolve(IDENTITY_PROVIDERS_TRANSFORMED);

        final String[] arguments = {
                "-k", BOOTSTRAP_HEXADECIMAL_KEY,
                "-i", identityProvidersPath.toString(),
                "-I", outputIdentityProvidersPath.toString()
        };

        final int status = run(arguments);
        assertEquals(CommandLine.ExitCode.OK, status);

        assertEncryptedPropertyFound(outputIdentityProvidersPath);
    }

    private int run(final String[] arguments) {
        final CommandLine commandLine = new CommandLine(new RegistryEncryptConfig());
        final PrintWriter writer = new PrintWriter(new ByteArrayOutputStream());
        commandLine.setOut(writer);
        commandLine.setErr(writer);
        return commandLine.execute(arguments);
    }

    private void assertEncryptedPropertyFound(final Path resourcePath) throws IOException {
        try (InputStream inputStream = Files.newInputStream(resourcePath)) {
            final DocumentProvider documentProvider = new StandardDocumentProvider();
            final Document document = documentProvider.parse(inputStream);
            final NodeList propertyNodes = document.getElementsByTagName("property");
            assertNotEquals(0, propertyNodes.getLength());

            boolean protectedPropertyFound = false;

            for (int i = 0; i < propertyNodes.getLength(); i++) {
                final Node propertyNode = propertyNodes.item(i);
                final NamedNodeMap attributes = propertyNode.getAttributes();
                final Node propertyNameAttribute = attributes.getNamedItem("name");
                final String propertyNameValue = propertyNameAttribute.getNodeValue();
                if (PROTECTED_PROPERTY_NAME.equals(propertyNameValue)) {
                    protectedPropertyFound = true;
                    final Node encryptionAttribute = attributes.getNamedItem("encryption");
                    assertNotNull(encryptionAttribute);
                }
            }

            assertTrue(protectedPropertyFound, "Protected Property element not found");
        }
    }

    private void assertProtectedPropertyFound(final Path outputPropertiesPath) throws IOException {
        final Properties outputProperties = loadProperties(outputPropertiesPath);

        assertTrue(outputProperties.containsKey(SENSITIVE_APPLICATION_PROPERTY));

        final String applicationPropertyProtected = ApplicationPropertiesProtector.getProtectionKey(SENSITIVE_APPLICATION_PROPERTY);
        final String protectedIdentifier = outputProperties.getProperty(applicationPropertyProtected);
        assertEquals(PROTECTED_IDENTIFIER, protectedIdentifier);
    }

    private void assertRootKeyFound(final Path bootstrapConfPath) throws IOException {
        final Properties bootstrapConf = loadProperties(bootstrapConfPath);

        final String rootKey = bootstrapConf.getProperty(RegistryEncryptConfig.BOOTSTRAP_ROOT_KEY_PROPERTY);
        assertEquals(BOOTSTRAP_HEXADECIMAL_KEY, rootKey);
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
