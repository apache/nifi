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
package org.apache.nifi.util;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NiFiPropertiesTest {

    @Test
    public void testProperties() {

        final NiFiProperties properties = loadNiFiProperties("/NiFiProperties/conf/nifi.properties", null);

        assertEquals("UI Banner Text", properties.getBannerText());

        final Set<File> expectedDirectories = new HashSet<>();
        expectedDirectories.add(new File("./target/resources/NiFiProperties/lib/"));
        expectedDirectories.add(new File("./target/resources/NiFiProperties/lib2/"));

        final Set<String> directories = new HashSet<>();
        for (final Path narLibDir : properties.getNarLibraryDirectories()) {
            directories.add(narLibDir.toString());
        }

        assertEquals(expectedDirectories.size(), directories.size(), "Did not have the anticipated number of directories");
        for (final File expectedDirectory : expectedDirectories) {
            assertTrue(directories.contains(expectedDirectory.getPath()), "Listed directories did not contain expected directory");
        }
    }

    @Test
    public void testMissingProperties() {

        final NiFiProperties properties = loadNiFiProperties("/NiFiProperties/conf/nifi.missing.properties", null);

        final List<Path> directories = properties.getNarLibraryDirectories();

        assertEquals(1, directories.size());

        assertEquals(new File(NiFiProperties.DEFAULT_NAR_LIBRARY_DIR).getPath(), directories.get(0)
                .toString());

    }

    @Test
    public void testBlankProperties() {

        final NiFiProperties properties = loadNiFiProperties("/NiFiProperties/conf/nifi.blank.properties", null);

        final List<Path> directories = properties.getNarLibraryDirectories();

        assertEquals(1, directories.size());

        assertEquals(new File(NiFiProperties.DEFAULT_NAR_LIBRARY_DIR).getPath(), directories.get(0)
                .toString());

    }

    @Test
    public void testValidateProperties() {
        // expect no error to be thrown
        Map<String, String> additionalProperties = new HashMap<>();
        additionalProperties.put(NiFiProperties.REMOTE_INPUT_HOST, "localhost");
        NiFiProperties properties = loadNiFiProperties("/NiFiProperties/conf/nifi.blank.properties", additionalProperties);

        assertGoodProperties(properties);

        // expect no error to be thrown
        additionalProperties.put(NiFiProperties.REMOTE_INPUT_HOST, "");
        properties = loadNiFiProperties("/NiFiProperties/conf/nifi.blank.properties", additionalProperties);

        assertGoodProperties(properties);

        // expect no error to be thrown
        additionalProperties.remove(NiFiProperties.REMOTE_INPUT_HOST);
        properties = loadNiFiProperties("/NiFiProperties/conf/nifi.blank.properties", additionalProperties);

        assertGoodProperties(properties);

        // expected error
        additionalProperties = new HashMap<>();
        additionalProperties.put(NiFiProperties.REMOTE_INPUT_HOST, "http://localhost");
        properties = loadNiFiProperties("/NiFiProperties/conf/nifi.blank.properties", additionalProperties);

        final NiFiProperties test = properties;
        assertThrows(Throwable.class, () -> test.validate());
    }

    private void assertGoodProperties(final NiFiProperties properties) {
        assertDoesNotThrow(() -> properties.validate());
    }

    @Test
    public void testAdditionalOidcScopesAreTrimmed() {
        final String scope = "abc";
        final String scopeLeadingWhitespace = " def";
        final String scopeTrailingWhitespace = "ghi ";
        final String scopeLeadingTrailingWhitespace = " jkl ";

        final String additionalScopes = String.join(",", scope, scopeLeadingWhitespace,
                scopeTrailingWhitespace, scopeLeadingTrailingWhitespace);

        final NiFiProperties properties = mock(NiFiProperties.class);
        when(properties.getProperty(NiFiProperties.SECURITY_USER_OIDC_ADDITIONAL_SCOPES, ""))
                .thenReturn(additionalScopes);
        when(properties.getOidcAdditionalScopes()).thenCallRealMethod();

        final List<String> scopes = properties.getOidcAdditionalScopes();

        assertTrue(scopes.contains(scope));
        assertFalse(scopes.contains(scopeLeadingWhitespace));
        assertTrue(scopes.contains(scopeLeadingWhitespace.trim()));
        assertFalse(scopes.contains(scopeTrailingWhitespace));
        assertTrue(scopes.contains(scopeTrailingWhitespace.trim()));
        assertFalse(scopes.contains(scopeLeadingTrailingWhitespace));
        assertTrue(scopes.contains(scopeLeadingTrailingWhitespace.trim()));
    }

    private NiFiProperties loadNiFiProperties(final String propsPath, final Map<String, String> additionalProperties) {
        String realPath = null;
        try {
            realPath = NiFiPropertiesTest.class.getResource(propsPath).toURI().getPath();
        } catch (final URISyntaxException ex) {
            throw new RuntimeException(ex);
        }
        return NiFiProperties.createBasicNiFiProperties(realPath, additionalProperties);
    }

    @Test
    public void testShouldVerifyValidFormatPortValue() {
        // Testing with CLUSTER_NODE_PROTOCOL_PORT

        // Arrange
        final String portValue = "8000";
        final Map<String, String> additionalProperties = new HashMap<>();
        additionalProperties.put(NiFiProperties.CLUSTER_NODE_PROTOCOL_PORT, portValue);
        final NiFiProperties properties = loadNiFiProperties("/NiFiProperties/conf/nifi.blank.properties", additionalProperties);

        // Act
        final Integer clusterProtocolPort = properties.getClusterNodeProtocolPort();

        // Assert
        assertEquals(Integer.parseInt(portValue), clusterProtocolPort.intValue());
    }

    @Test
    public void testShouldVerifyExceptionThrownWhenInValidFormatPortValue() {
        // Testing with CLUSTER_NODE_PROTOCOL_PORT

        // Arrange
        // Port Value is invalid Format
        final String portValue = "8000a";
        final Map<String, String> additionalProperties = new HashMap<>();
        additionalProperties.put(NiFiProperties.CLUSTER_NODE_PROTOCOL_PORT, portValue);
        final NiFiProperties properties = loadNiFiProperties("/NiFiProperties/conf/nifi.blank.properties", additionalProperties);

        // Act
        properties.getClusterNodeProtocolPort();
        assertThrows(NumberFormatException.class, () -> Integer.parseInt(portValue));
    }

    @Test
    public void testShouldVerifyValidPortValue() {
        // Testing with CLUSTER_NODE_ADDRESS

        // Arrange
        final String portValue = "8000";
        final String addressValue = "127.0.0.1";
        final Map<String, String> additionalProperties = new HashMap<>();
        additionalProperties.put(NiFiProperties.CLUSTER_NODE_PROTOCOL_PORT, portValue);
        additionalProperties.put(NiFiProperties.CLUSTER_NODE_ADDRESS, addressValue);
        final NiFiProperties properties = loadNiFiProperties("/NiFiProperties/conf/nifi.blank.properties", additionalProperties);

        // Act
        final InetSocketAddress clusterProtocolAddress = properties.getClusterNodeProtocolAddress();

        // Assert
        assertEquals(Integer.parseInt(portValue), clusterProtocolAddress.getPort());
    }

    @Test
    public void testShouldVerifyExceptionThrownWhenInvalidPortValue() {
        // Testing with CLUSTER_NODE_ADDRESS

        // Arrange
        // Port value is out of range
        final String portValue = "70000";
        final String addressValue = "127.0.0.1";
        final Map<String, String> additionalProperties = new HashMap<>();
        additionalProperties.put(NiFiProperties.CLUSTER_NODE_PROTOCOL_PORT, portValue);
        additionalProperties.put(NiFiProperties.CLUSTER_NODE_ADDRESS, addressValue);
        final NiFiProperties properties = loadNiFiProperties("/NiFiProperties/conf/nifi.blank.properties", additionalProperties);

        assertThrows(RuntimeException.class, () -> properties.getClusterNodeProtocolAddress());
    }

    @Test
    public void testShouldVerifyExceptionThrownWhenPortValueIsZero() {
        // Arrange
        final String portValue = "0";
        final String addressValue = "127.0.0.1";
        final Map<String, String> additionalProperties = new HashMap<>();
        additionalProperties.put(NiFiProperties.CLUSTER_NODE_PROTOCOL_PORT, portValue);
        additionalProperties.put(NiFiProperties.CLUSTER_NODE_ADDRESS, addressValue);
        final NiFiProperties properties = loadNiFiProperties("/NiFiProperties/conf/nifi.blank.properties", additionalProperties);

        assertThrows(RuntimeException.class, () -> properties.getClusterNodeProtocolAddress());
    }

    @Test
    public void testShouldHaveReasonableMaxContentLengthValues() {
        // Arrange with default values:
        NiFiProperties properties = NiFiProperties.createBasicNiFiProperties(null, new HashMap<>());

        // Assert defaults match expectations:
        assertNull(properties.getWebMaxContentSize());

        // Re-arrange with specific values:
        final String size = "size value";
        properties = NiFiProperties.createBasicNiFiProperties(null, Map.of(NiFiProperties.WEB_MAX_CONTENT_SIZE, size));

        // Assert specific values are used:
        assertEquals(properties.getWebMaxContentSize(),  size);
    }

    @Test
    public void testIsZooKeeperTlsConfigurationPresent() {
        final NiFiProperties properties = NiFiProperties.createBasicNiFiProperties(null, Map.of(
            NiFiProperties.ZOOKEEPER_CLIENT_SECURE, "true",
            NiFiProperties.ZOOKEEPER_SECURITY_KEYSTORE, "/a/keystore/filepath/keystore.jks",
            NiFiProperties.ZOOKEEPER_SECURITY_KEYSTORE_PASSWD, "password",
            NiFiProperties.ZOOKEEPER_SECURITY_KEYSTORE_TYPE, "JKS",
            NiFiProperties.ZOOKEEPER_SECURITY_TRUSTSTORE, "/a/truststore/filepath/truststore.jks",
            NiFiProperties.ZOOKEEPER_SECURITY_TRUSTSTORE_PASSWD, "password",
            NiFiProperties.ZOOKEEPER_SECURITY_TRUSTSTORE_TYPE, "JKS"));

        assertTrue(properties.isZooKeeperClientSecure());
        assertTrue(properties.isZooKeeperTlsConfigurationPresent());
    }

    @Test
    public void testSomeZooKeeperTlsConfigurationIsMissing() {
        final NiFiProperties properties = NiFiProperties.createBasicNiFiProperties(null, Map.of(
            NiFiProperties.ZOOKEEPER_CLIENT_SECURE, "true",
            NiFiProperties.ZOOKEEPER_SECURITY_KEYSTORE_PASSWD, "password",
            NiFiProperties.ZOOKEEPER_SECURITY_KEYSTORE_TYPE, "JKS",
            NiFiProperties.ZOOKEEPER_SECURITY_TRUSTSTORE, "/a/truststore/filepath/truststore.jks",
            NiFiProperties.ZOOKEEPER_SECURITY_TRUSTSTORE_TYPE, "JKS"));

        assertTrue(properties.isZooKeeperClientSecure());
        assertFalse(properties.isZooKeeperTlsConfigurationPresent());
    }

    @Test
    public void testZooKeeperTlsPasswordsBlank() {
        final NiFiProperties properties = NiFiProperties.createBasicNiFiProperties(null, Map.of(
            NiFiProperties.ZOOKEEPER_CLIENT_SECURE, "true",
            NiFiProperties.ZOOKEEPER_SECURITY_KEYSTORE, "/a/keystore/filepath/keystore.jks",
            NiFiProperties.ZOOKEEPER_SECURITY_KEYSTORE_PASSWD, "",
            NiFiProperties.ZOOKEEPER_SECURITY_KEYSTORE_TYPE, "JKS",
            NiFiProperties.ZOOKEEPER_SECURITY_TRUSTSTORE, "/a/truststore/filepath/truststore.jks",
            NiFiProperties.ZOOKEEPER_SECURITY_TRUSTSTORE_PASSWD, "",
            NiFiProperties.ZOOKEEPER_SECURITY_TRUSTSTORE_TYPE, "JKS"));

        assertTrue(properties.isZooKeeperClientSecure());
        assertTrue(properties.isZooKeeperTlsConfigurationPresent());
    }

    @Test
    public void testKeystorePasswordIsMissing() {
        final NiFiProperties properties = NiFiProperties.createBasicNiFiProperties(null, Map.of(
            NiFiProperties.SECURITY_KEYSTORE, "/a/keystore/filepath/keystore.jks",
            NiFiProperties.SECURITY_KEYSTORE_TYPE, "JKS",
            NiFiProperties.SECURITY_TRUSTSTORE, "/a/truststore/filepath/truststore.jks",
            NiFiProperties.SECURITY_TRUSTSTORE_PASSWD, "",
            NiFiProperties.SECURITY_TRUSTSTORE_TYPE, "JKS"));

        assertFalse(properties.isTlsConfigurationPresent());
    }

    @Test
    public void testTlsConfigurationIsPresentWithEmptyPasswords() {
        final NiFiProperties properties = NiFiProperties.createBasicNiFiProperties(null, Map.of(
            NiFiProperties.SECURITY_KEYSTORE, "/a/keystore/filepath/keystore.jks",
            NiFiProperties.SECURITY_KEYSTORE_PASSWD, "",
            NiFiProperties.SECURITY_KEYSTORE_TYPE, "JKS",
            NiFiProperties.SECURITY_TRUSTSTORE, "/a/truststore/filepath/truststore.jks",
            NiFiProperties.SECURITY_TRUSTSTORE_PASSWD, "",
            NiFiProperties.SECURITY_TRUSTSTORE_TYPE, "JKS"));

        assertTrue(properties.isTlsConfigurationPresent());
    }

    @Test
    public void testTlsConfigurationIsNotPresentWithPropertiesMissing() {
        final NiFiProperties properties = NiFiProperties.createBasicNiFiProperties(null, Map.of(
            NiFiProperties.SECURITY_KEYSTORE_PASSWD, "password",
            NiFiProperties.SECURITY_KEYSTORE_TYPE, "JKS",
            NiFiProperties.SECURITY_TRUSTSTORE, "/a/truststore/filepath/truststore.jks"));

        assertFalse(properties.isTlsConfigurationPresent());
    }

    @Test
    public void testTlsConfigurationIsNotPresentWithNoProperties() {
        final NiFiProperties properties = NiFiProperties.createBasicNiFiProperties(null, new HashMap<>());

        assertFalse(properties.isTlsConfigurationPresent());
    }

    @Test
    public void testGetPropertiesWithPrefixWithoutDot() {
        // given
        final NiFiProperties testSubject = loadNiFiProperties("/NiFiProperties/conf/nifi.properties", null);

        // when
        final Map<String, String> result = testSubject.getPropertiesWithPrefix("nifi.web.http");

        // then
        assertEquals(4, result.size());
        assertTrue(result.containsKey("nifi.web.http.host"));
        assertTrue(result.containsKey("nifi.web.https.host"));
    }

    @Test
    public void testGetPropertiesWithPrefixWithDot() {
        // given
        final NiFiProperties testSubject = loadNiFiProperties("/NiFiProperties/conf/nifi.properties", null);

        // when
        final Map<String, String> result = testSubject.getPropertiesWithPrefix("nifi.web.http.");

        // then
        assertEquals(2, result.size());
        assertTrue(result.containsKey("nifi.web.http.host"));
        assertFalse(result.containsKey("nifi.web.https.host"));
    }

    @Test
    public void testGetPropertiesWithPrefixWhenNoResult() {
        // given
        final NiFiProperties testSubject = loadNiFiProperties("/NiFiProperties/conf/nifi.properties", null);

        // when
        final Map<String, String> result = testSubject.getPropertiesWithPrefix("invalid.property");

        // then
        assertTrue(result.isEmpty());
    }

    @Test
    public void testGetDirectSubsequentTokensWithoutDot() {
        // given
        final NiFiProperties testSubject = loadNiFiProperties("/NiFiProperties/conf/nifi.properties", null);

        // when
        final Set<String> result = testSubject.getDirectSubsequentTokens("nifi.web.http");

        // then
        assertEquals(2, result.size());
        assertTrue(result.contains("host"));
        assertTrue(result.contains("port"));
    }

    @Test
    public void testGetDirectSubsequentTokensWithDot() {
        // given
        final NiFiProperties testSubject = loadNiFiProperties("/NiFiProperties/conf/nifi.properties", null);

        // when
        final Set<String> result = testSubject.getDirectSubsequentTokens("nifi.web.http.");

        // then
        assertEquals(2, result.size());
        assertTrue(result.contains("host"));
        assertTrue(result.contains("port"));
    }

    @Test
    public void testGetDirectSubsequentTokensWithNonExistingToken() {
        // given
        final NiFiProperties testSubject = loadNiFiProperties("/NiFiProperties/conf/nifi.properties", null);

        // when
        final Set<String> result = testSubject.getDirectSubsequentTokens("lorem.ipsum");

        // then
        assertTrue(result.isEmpty());
    }

    @Test
    public void testGetDirectSubsequentTokensWhenMoreTokensAfterward() {
        // given
        final NiFiProperties testSubject = loadNiFiProperties("/NiFiProperties/conf/nifi.properties", null);

        // when
        final Set<String> result = testSubject.getDirectSubsequentTokens("nifi.web");

        // then
        assertEquals(4, result.size());
        assertTrue(result.contains("http"));
        assertTrue(result.contains("https"));
        assertTrue(result.contains("war"));
        assertTrue(result.contains("jetty"));
    }
}
