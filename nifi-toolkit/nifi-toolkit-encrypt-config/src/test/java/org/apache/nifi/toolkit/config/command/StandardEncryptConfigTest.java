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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.TextNode;
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
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Properties;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StandardEncryptConfigTest {

    private static final String FLOW_JSON_RESOURCE = "/command/flow.json";

    private static final String BOOTSTRAP_CONF_RESOURCE = "/command/bootstrap.conf";

    private static final String APPLICATION_PROPERTIES_RESOURCE = "/command/nifi.properties";

    private static final String AUTHORIZERS_UNPROTECTED_RESOURCE = "/transformer/authorizers-unprotected.xml";

    private static final String LOGIN_IDENTITY_PROVIDERS_UNPROTECTED_RESOURCE = "/command/login-identity-providers-unprotected.xml";

    private static final String LOGIN_IDENTITY_PROVIDERS_PROTECTED_RESOURCE = "/command/login-identity-providers-protected.xml";

    private static final String APPLICATION_PROPERTIES_TRANSFORMED = "transformed.nifi.properties";

    private static final String FLOW_JSON_TRANSFORMED = "transformed.flow.json.gz";

    private static final String AUTHORIZERS_TRANSFORMED = "transformed.authorizers.xml";

    private static final String LOGIN_IDENTITY_PROVIDERS_TRANSFORMED = "transformed.login-identity-providers.xml";

    private static final String COMPRESSED_FILE_NAME_FORMAT = "%s.gz";

    private static final String SENSITIVE_PROPERTIES_KEY = "UNPROTECTED_KEY";

    private static final String BOOTSTRAP_HEXADECIMAL_KEY = "012345678901234567890123456789012345678901234567890123456789ABCD";

    private static final String NEW_BOOTSTRAP_HEXADECIMAL_KEY = "ABCD012345678901234567890123456789012345678901234567890123456789";

    private static final String BOOTSTRAP_PASSWORD = "BOOTSTRAP_UNPROTECTED";

    private static final String NEW_SENSITIVE_PROPERTIES_ALGORITHM = "NIFI_ARGON2_AES_GCM_256";

    private static final String SENSITIVE_PROPERTY_NAME = "Sensitive Property";

    private static final String PROTECTED_PROPERTY_NAME = "Random Password";

    private static final String SENSITIVE_APPLICATION_PROPERTY = "nifi.sensitive.props.key";

    private static final String PROTECTED_IDENTIFIER = "aes/gcm/256";

    private static final String STANDARD_PROTECTION_SCHEME = "AES_GCM";

    private static final ObjectMapper objectMapper = new ObjectMapper();

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
                "-n", propertiesPath.toString()
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
                "-n", propertiesPath.toString(),
                "-o", outputPropertiesPath.toString()
        };

        final int status = run(arguments);
        assertEquals(CommandLine.ExitCode.OK, status);

        assertProtectedPropertyFound(outputPropertiesPath);
    }

    @Test
    void testFlowConfiguration() throws URISyntaxException, IOException {
        final Path propertiesPath = getResourcePath(APPLICATION_PROPERTIES_RESOURCE);
        final Path flowConfigurationPath = getResourcePathCompressed();
        final Path outputFlowConfigurationPath = tempDir.resolve(FLOW_JSON_TRANSFORMED);

        final String[] arguments = {
                "-x",
                "-k", BOOTSTRAP_HEXADECIMAL_KEY,
                "-s", SENSITIVE_PROPERTIES_KEY,
                "-n", propertiesPath.toString(),
                "-f", flowConfigurationPath.toString(),
                "-g", outputFlowConfigurationPath.toString()
        };

        final int status = run(arguments);
        assertEquals(CommandLine.ExitCode.OK, status);

        assertFlowConfigurationTransformed(flowConfigurationPath, outputFlowConfigurationPath);
    }

    @Test
    void testFlowConfigurationNewPropertiesAlgorithm() throws URISyntaxException, IOException {
        final Path propertiesPath = getResourcePath(APPLICATION_PROPERTIES_RESOURCE);
        final Path flowConfigurationPath = getResourcePathCompressed();
        final Path outputFlowConfigurationPath = tempDir.resolve(FLOW_JSON_TRANSFORMED);

        final String[] arguments = {
                "-x",
                "-k", BOOTSTRAP_HEXADECIMAL_KEY,
                "-A", NEW_SENSITIVE_PROPERTIES_ALGORITHM,
                "-s", SENSITIVE_PROPERTIES_KEY,
                "-n", propertiesPath.toString(),
                "-f", flowConfigurationPath.toString(),
                "-g", outputFlowConfigurationPath.toString()
        };

        final int status = run(arguments);
        assertEquals(CommandLine.ExitCode.OK, status);

        assertFlowConfigurationTransformed(flowConfigurationPath, outputFlowConfigurationPath);
    }

    @Test
    void testFlowConfigurationWithBootstrapConf() throws URISyntaxException, IOException {
        final Path propertiesPath = getResourcePath(APPLICATION_PROPERTIES_RESOURCE);
        final Path flowConfigurationPath = getResourcePathCompressed();
        final Path outputFlowConfigurationPath = tempDir.resolve(FLOW_JSON_TRANSFORMED);

        final Path inputBootstrapConfPath = getResourcePath(BOOTSTRAP_CONF_RESOURCE);
        final Path bootstrapConfPath = tempDir.resolve(inputBootstrapConfPath.getFileName());
        Files.copy(inputBootstrapConfPath, bootstrapConfPath);

        final String[] arguments = {
                "-b", bootstrapConfPath.toString(),
                "-k", BOOTSTRAP_HEXADECIMAL_KEY,
                "-s", SENSITIVE_PROPERTIES_KEY,
                "-n", propertiesPath.toString(),
                "-f", flowConfigurationPath.toString(),
                "-g", outputFlowConfigurationPath.toString()
        };

        final int status = run(arguments);
        assertEquals(CommandLine.ExitCode.OK, status);

        assertFlowConfigurationTransformed(flowConfigurationPath, outputFlowConfigurationPath);
        assertRootKeyFound(bootstrapConfPath);
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
    void testAuthorizersWithPassword() throws URISyntaxException, IOException {
        final Path authorizersPath = getResourcePath(AUTHORIZERS_UNPROTECTED_RESOURCE);
        final Path outputAuthorizersPath = tempDir.resolve(AUTHORIZERS_TRANSFORMED);

        final String[] arguments = {
                "-p", BOOTSTRAP_PASSWORD,
                "-a", authorizersPath.toString(),
                "-u", outputAuthorizersPath.toString()
        };

        final int status = run(arguments);
        assertEquals(CommandLine.ExitCode.OK, status);

        assertEncryptedPropertyFound(outputAuthorizersPath);
    }

    @Test
    void testLoginIdentityProvidersWithKey() throws URISyntaxException, IOException {
        final Path loginIdentityProvidersPath = getResourcePath(LOGIN_IDENTITY_PROVIDERS_UNPROTECTED_RESOURCE);
        final Path outputLoginIdentityProvidersPath = tempDir.resolve(LOGIN_IDENTITY_PROVIDERS_TRANSFORMED);

        final String[] arguments = {
                "-k", BOOTSTRAP_HEXADECIMAL_KEY,
                "-l", loginIdentityProvidersPath.toString(),
                "-i", outputLoginIdentityProvidersPath.toString()
        };

        final int status = run(arguments);
        assertEquals(CommandLine.ExitCode.OK, status);

        assertEncryptedPropertyFound(outputLoginIdentityProvidersPath);
    }

    @Test
    void testLoginIdentityProvidersMigrationWithKey() throws URISyntaxException, IOException {
        final Path loginIdentityProvidersPath = getResourcePath(LOGIN_IDENTITY_PROVIDERS_PROTECTED_RESOURCE);
        final Path outputLoginIdentityProvidersPath = tempDir.resolve(LOGIN_IDENTITY_PROVIDERS_TRANSFORMED);

        final String[] arguments = {
                "-m",
                "-e", BOOTSTRAP_HEXADECIMAL_KEY,
                "-H", STANDARD_PROTECTION_SCHEME,
                "-k", NEW_BOOTSTRAP_HEXADECIMAL_KEY,
                "-l", loginIdentityProvidersPath.toString(),
                "-i", outputLoginIdentityProvidersPath.toString()
        };

        final int status = run(arguments);
        assertEquals(CommandLine.ExitCode.OK, status);

        assertEncryptedPropertyFound(outputLoginIdentityProvidersPath);
    }

    private int run(final String[] arguments) {
        final CommandLine commandLine = new CommandLine(new StandardEncryptConfig());
        final PrintWriter writer = new PrintWriter(new ByteArrayOutputStream());
        //commandLine.setOut(writer);
        commandLine.setErr(writer);
        return commandLine.execute(arguments);
    }

    private void assertFlowConfigurationTransformed(final Path inputFlowConfigurationPath, final Path outputFlowConfigurationPath) throws IOException {
        assertTrue(Files.isReadable(outputFlowConfigurationPath));

        final String inputSensitiveProperty = assertSensitivePropertyFound(inputFlowConfigurationPath);
        final String outputSensitiveProperty = assertSensitivePropertyFound(outputFlowConfigurationPath);
        assertNotEquals(inputSensitiveProperty, outputSensitiveProperty);
    }

    private String assertSensitivePropertyFound(final Path flowConfigurationPath) throws IOException {
        try (InputStream inputStream = new GZIPInputStream(Files.newInputStream(flowConfigurationPath))) {
            final JsonNode flowConfiguration = objectMapper.readTree(inputStream);
            final JsonNode rootGroup = flowConfiguration.get("rootGroup");
            assertNotNull(rootGroup);
            final JsonNode processors = rootGroup.get("processors");
            assertNotNull(processors);
            final JsonNode processor = processors.get(0);
            assertNotNull(processor);
            final JsonNode properties = processor.get("properties");
            assertNotNull(properties);
            final JsonNode sensitiveProperty = properties.get(SENSITIVE_PROPERTY_NAME);
            assertInstanceOf(TextNode.class, sensitiveProperty);
            final TextNode sensitivePropertyValue = (TextNode) sensitiveProperty;
            return sensitivePropertyValue.asText();
        }
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

        final String rootKey = bootstrapConf.getProperty(StandardEncryptConfig.BOOTSTRAP_ROOT_KEY_PROPERTY);
        assertEquals(BOOTSTRAP_HEXADECIMAL_KEY, rootKey);
    }

    private Properties loadProperties(final Path resourcePath) throws IOException {
        final Properties properties = new Properties();
        try (InputStream propertiesStream = Files.newInputStream(resourcePath)) {
            properties.load(propertiesStream);
        }
        return properties;
    }

    private Path getResourcePathCompressed() throws URISyntaxException, IOException {
        final Path resourcePath = getResourcePath(FLOW_JSON_RESOURCE);
        final byte[] resourceBytes = Files.readAllBytes(resourcePath);

        final String resourceFileName = String.format(COMPRESSED_FILE_NAME_FORMAT, resourcePath.getFileName());
        final Path resourcePathCompressed = tempDir.resolve(resourceFileName);
        try (OutputStream outputStream = new GZIPOutputStream(Files.newOutputStream(resourcePathCompressed))) {
            outputStream.write(resourceBytes);
        }

        return resourcePathCompressed;
    }

    private Path getResourcePath(final String resource) throws URISyntaxException {
        final URL resourceUrl = Objects.requireNonNull(getClass().getResource(resource), String.format("Resource [%s] not found", resource));
        return Paths.get(resourceUrl.toURI());
    }
}
