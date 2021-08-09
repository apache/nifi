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

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NiFiPropertiesTest {

    @Test
    public void testProperties() {

        NiFiProperties properties = loadNiFiProperties("/NiFiProperties/conf/nifi.properties", null);

        assertEquals("UI Banner Text", properties.getBannerText());

        Set<File> expectedDirectories = new HashSet<>();
        expectedDirectories.add(new File("./target/resources/NiFiProperties/lib/"));
        expectedDirectories.add(new File("./target/resources/NiFiProperties/lib2/"));

        Set<String> directories = new HashSet<>();
        for (Path narLibDir : properties.getNarLibraryDirectories()) {
            directories.add(narLibDir.toString());
        }

        Assert.assertEquals("Did not have the anticipated number of directories", expectedDirectories.size(), directories.size());
        for (File expectedDirectory : expectedDirectories) {
            Assert.assertTrue("Listed directories did not contain expected directory", directories.contains(expectedDirectory.getPath()));
        }
    }

    @Test
    public void testMissingProperties() {

        NiFiProperties properties = loadNiFiProperties("/NiFiProperties/conf/nifi.missing.properties", null);

        List<Path> directories = properties.getNarLibraryDirectories();

        assertEquals(1, directories.size());

        assertEquals(new File(NiFiProperties.DEFAULT_NAR_LIBRARY_DIR).getPath(), directories.get(0)
                .toString());

    }

    @Test
    public void testBlankProperties() {

        NiFiProperties properties = loadNiFiProperties("/NiFiProperties/conf/nifi.blank.properties", null);

        List<Path> directories = properties.getNarLibraryDirectories();

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

        try {
            properties.validate();
        } catch (Throwable t) {
            Assert.fail("unexpected exception: " + t.getMessage());
        }

        // expect no error to be thrown
        additionalProperties.put(NiFiProperties.REMOTE_INPUT_HOST, "");
        properties = loadNiFiProperties("/NiFiProperties/conf/nifi.blank.properties", additionalProperties);

        try {
            properties.validate();
        } catch (Throwable t) {
            Assert.fail("unexpected exception: " + t.getMessage());
        }

        // expect no error to be thrown
        additionalProperties.remove(NiFiProperties.REMOTE_INPUT_HOST);
        properties = loadNiFiProperties("/NiFiProperties/conf/nifi.blank.properties", additionalProperties);

        try {
            properties.validate();
        } catch (Throwable t) {
            Assert.fail("unexpected exception: " + t.getMessage());
        }

        // expected error
        additionalProperties = new HashMap<>();
        additionalProperties.put(NiFiProperties.REMOTE_INPUT_HOST, "http://localhost");
        properties = loadNiFiProperties("/NiFiProperties/conf/nifi.blank.properties", additionalProperties);

        try {
            properties.validate();
            Assert.fail("Validation should throw an exception");
        } catch (Throwable t) {
            // nothing to do
        }
    }

    @Test
    public void testAdditionalOidcScopesAreTrimmed() {
        final String scope = "abc";
        final String scopeLeadingWhitespace = " def";
        final String scopeTrailingWhitespace = "ghi ";
        final String scopeLeadingTrailingWhitespace = " jkl ";

        String additionalScopes = String.join(",", scope, scopeLeadingWhitespace,
                scopeTrailingWhitespace, scopeLeadingTrailingWhitespace);

        NiFiProperties properties = mock(NiFiProperties.class);
        when(properties.getProperty(NiFiProperties.SECURITY_USER_OIDC_ADDITIONAL_SCOPES, ""))
                .thenReturn(additionalScopes);
        when(properties.getOidcAdditionalScopes()).thenCallRealMethod();

        List<String> scopes = properties.getOidcAdditionalScopes();

        assertTrue(scopes.contains(scope));
        assertFalse(scopes.contains(scopeLeadingWhitespace));
        assertTrue(scopes.contains(scopeLeadingWhitespace.trim()));
        assertFalse(scopes.contains(scopeTrailingWhitespace));
        assertTrue(scopes.contains(scopeTrailingWhitespace.trim()));
        assertFalse(scopes.contains(scopeLeadingTrailingWhitespace));
        assertTrue(scopes.contains(scopeLeadingTrailingWhitespace.trim()));
    }

    private NiFiProperties loadNiFiProperties(final String propsPath, final Map<String, String> additionalProperties){
        String realPath = null;
        try{
            realPath = NiFiPropertiesTest.class.getResource(propsPath).toURI().getPath();
        }catch(final URISyntaxException ex){
            throw new RuntimeException(ex);
        }
        return NiFiProperties.createBasicNiFiProperties(realPath, additionalProperties);
    }

    @Test
    public void testShouldVerifyValidFormatPortValue() {
        // Testing with CLUSTER_NODE_PROTOCOL_PORT

        // Arrange
        String portValue = "8000";
        Map<String, String> additionalProperties = new HashMap<>();
        additionalProperties.put(NiFiProperties.CLUSTER_NODE_PROTOCOL_PORT, portValue);
        NiFiProperties properties = loadNiFiProperties("/NiFiProperties/conf/nifi.blank.properties", additionalProperties);

        // Act
        Integer clusterProtocolPort = properties.getClusterNodeProtocolPort();

        // Assert
        assertEquals(Integer.parseInt(portValue), clusterProtocolPort.intValue());
    }

    @Test(expected = NumberFormatException.class)
    public void testShouldVerifyExceptionThrownWhenInValidFormatPortValue() {
        // Testing with CLUSTER_NODE_PROTOCOL_PORT

        // Arrange
        // Port Value is invalid Format
        String portValue = "8000a";
        Map<String, String> additionalProperties = new HashMap<>();
        additionalProperties.put(NiFiProperties.CLUSTER_NODE_PROTOCOL_PORT, portValue);
        NiFiProperties properties = loadNiFiProperties("/NiFiProperties/conf/nifi.blank.properties", additionalProperties);

        // Act
        Integer clusterProtocolPort = properties.getClusterNodeProtocolPort();

        // Assert
        // Expect NumberFormatException thrown
        assertEquals(Integer.parseInt(portValue), clusterProtocolPort.intValue());
    }

    @Test
    public void testShouldVerifyValidPortValue() {
        // Testing with CLUSTER_NODE_ADDRESS

        // Arrange
        String portValue = "8000";
        String addressValue = "127.0.0.1";
        Map<String, String> additionalProperties = new HashMap<>();
        additionalProperties.put(NiFiProperties.CLUSTER_NODE_PROTOCOL_PORT, portValue);
        additionalProperties.put(NiFiProperties.CLUSTER_NODE_ADDRESS, addressValue);
        NiFiProperties properties = loadNiFiProperties("/NiFiProperties/conf/nifi.blank.properties", additionalProperties);

        // Act
        InetSocketAddress clusterProtocolAddress = properties.getClusterNodeProtocolAddress();

        // Assert
        assertEquals(Integer.parseInt(portValue), clusterProtocolAddress.getPort());
    }

    @Test(expected = RuntimeException.class)
    public void testShouldVerifyExceptionThrownWhenInvalidPortValue() {
        // Testing with CLUSTER_NODE_ADDRESS

        // Arrange
        // Port value is out of range
        String portValue = "70000";
        String addressValue = "127.0.0.1";
        Map<String, String> additionalProperties = new HashMap<>();
        additionalProperties.put(NiFiProperties.CLUSTER_NODE_PROTOCOL_PORT, portValue);
        additionalProperties.put(NiFiProperties.CLUSTER_NODE_ADDRESS, addressValue);
        NiFiProperties properties = loadNiFiProperties("/NiFiProperties/conf/nifi.blank.properties", additionalProperties);

        // Act
        InetSocketAddress clusterProtocolAddress = properties.getClusterNodeProtocolAddress();

        // Assert
        // Expect RuntimeException thrown
        assertEquals(Integer.parseInt(portValue), clusterProtocolAddress.getPort());
    }

    @Test(expected = RuntimeException.class)
    public void testShouldVerifyExceptionThrownWhenPortValueIsZero() {
        // Arrange
        String portValue = "0";
        String addressValue = "127.0.0.1";
        Map<String, String> additionalProperties = new HashMap<>();
        additionalProperties.put(NiFiProperties.CLUSTER_NODE_PROTOCOL_PORT, portValue);
        additionalProperties.put(NiFiProperties.CLUSTER_NODE_ADDRESS, addressValue);
        NiFiProperties properties = loadNiFiProperties("/NiFiProperties/conf/nifi.blank.properties", additionalProperties);

        // Act
        InetSocketAddress clusterProtocolAddress = properties.getClusterNodeProtocolAddress();

        // Assert
        // Expect RuntimeException thrown
        assertEquals(Integer.parseInt(portValue), clusterProtocolAddress.getPort());
    }

    @Test
    public void testShouldHaveReasonableMaxContentLengthValues() {
        // Arrange with default values:
        NiFiProperties properties = NiFiProperties.createBasicNiFiProperties(null, new HashMap<String, String>() {{
        }});

        // Assert defaults match expectations:
        assertNull(properties.getWebMaxContentSize());

        // Re-arrange with specific values:
        final String size = "size value";
        properties = NiFiProperties.createBasicNiFiProperties(null, new HashMap<String, String>() {{
            put(NiFiProperties.WEB_MAX_CONTENT_SIZE, size);
        }});

        // Assert specific values are used:
        assertEquals(properties.getWebMaxContentSize(),  size);
    }

    @Test
    public void testIsZooKeeperTlsConfigurationPresent() {
        NiFiProperties properties = NiFiProperties.createBasicNiFiProperties(null, new HashMap<String, String>() {{
            put(NiFiProperties.ZOOKEEPER_CLIENT_SECURE, "true");
            put(NiFiProperties.ZOOKEEPER_SECURITY_KEYSTORE, "/a/keystore/filepath/keystore.jks");
            put(NiFiProperties.ZOOKEEPER_SECURITY_KEYSTORE_PASSWD, "password");
            put(NiFiProperties.ZOOKEEPER_SECURITY_KEYSTORE_TYPE, "JKS");
            put(NiFiProperties.ZOOKEEPER_SECURITY_TRUSTSTORE, "/a/truststore/filepath/truststore.jks");
            put(NiFiProperties.ZOOKEEPER_SECURITY_TRUSTSTORE_PASSWD, "password");
            put(NiFiProperties.ZOOKEEPER_SECURITY_TRUSTSTORE_TYPE, "JKS");
        }});

        assertTrue(properties.isZooKeeperClientSecure());
        assertTrue(properties.isZooKeeperTlsConfigurationPresent());
    }

    @Test
    public void testSomeZooKeeperTlsConfigurationIsMissing() {
        NiFiProperties properties = NiFiProperties.createBasicNiFiProperties(null, new HashMap<String, String>() {{
            put(NiFiProperties.ZOOKEEPER_CLIENT_SECURE, "true");
            put(NiFiProperties.ZOOKEEPER_SECURITY_KEYSTORE_PASSWD, "password");
            put(NiFiProperties.ZOOKEEPER_SECURITY_KEYSTORE_TYPE, "JKS");
            put(NiFiProperties.ZOOKEEPER_SECURITY_TRUSTSTORE, "/a/truststore/filepath/truststore.jks");
            put(NiFiProperties.ZOOKEEPER_SECURITY_TRUSTSTORE_TYPE, "JKS");
        }});

        assertTrue(properties.isZooKeeperClientSecure());
        assertFalse(properties.isZooKeeperTlsConfigurationPresent());
    }

    @Test
    public void testZooKeeperTlsPasswordsBlank() {
        NiFiProperties properties = NiFiProperties.createBasicNiFiProperties(null, new HashMap<String, String>() {{
            put(NiFiProperties.ZOOKEEPER_CLIENT_SECURE, "true");
            put(NiFiProperties.ZOOKEEPER_SECURITY_KEYSTORE, "/a/keystore/filepath/keystore.jks");
            put(NiFiProperties.ZOOKEEPER_SECURITY_KEYSTORE_PASSWD, "");
            put(NiFiProperties.ZOOKEEPER_SECURITY_KEYSTORE_TYPE, "JKS");
            put(NiFiProperties.ZOOKEEPER_SECURITY_TRUSTSTORE, "/a/truststore/filepath/truststore.jks");
            put(NiFiProperties.ZOOKEEPER_SECURITY_TRUSTSTORE_PASSWD, "");
            put(NiFiProperties.ZOOKEEPER_SECURITY_TRUSTSTORE_TYPE, "JKS");
        }});

        assertTrue(properties.isZooKeeperClientSecure());
        assertTrue(properties.isZooKeeperTlsConfigurationPresent());
    }

    @Test
    public void testKeystorePasswordIsMissing() {
        NiFiProperties properties = NiFiProperties.createBasicNiFiProperties(null, new HashMap<String, String>() {{
            put(NiFiProperties.SECURITY_KEYSTORE, "/a/keystore/filepath/keystore.jks");
            put(NiFiProperties.SECURITY_KEYSTORE_TYPE, "JKS");
            put(NiFiProperties.SECURITY_TRUSTSTORE, "/a/truststore/filepath/truststore.jks");
            put(NiFiProperties.SECURITY_TRUSTSTORE_PASSWD, "");
            put(NiFiProperties.SECURITY_TRUSTSTORE_TYPE, "JKS");
        }});

        assertFalse(properties.isTlsConfigurationPresent());
    }

    @Test
    public void testTlsConfigurationIsPresentWithEmptyPasswords() {
        NiFiProperties properties = NiFiProperties.createBasicNiFiProperties(null, new HashMap<String, String>() {{
            put(NiFiProperties.SECURITY_KEYSTORE, "/a/keystore/filepath/keystore.jks");
            put(NiFiProperties.SECURITY_KEYSTORE_PASSWD, "");
            put(NiFiProperties.SECURITY_KEYSTORE_TYPE, "JKS");
            put(NiFiProperties.SECURITY_TRUSTSTORE, "/a/truststore/filepath/truststore.jks");
            put(NiFiProperties.SECURITY_TRUSTSTORE_PASSWD, "");
            put(NiFiProperties.SECURITY_TRUSTSTORE_TYPE, "JKS");
        }});

        assertTrue(properties.isTlsConfigurationPresent());
    }

    @Test
    public void testTlsConfigurationIsNotPresentWithPropertiesMissing() {
        NiFiProperties properties = NiFiProperties.createBasicNiFiProperties(null, new HashMap<String, String>() {{
            put(NiFiProperties.SECURITY_KEYSTORE_PASSWD, "password");
            put(NiFiProperties.SECURITY_KEYSTORE_TYPE, "JKS");
            put(NiFiProperties.SECURITY_TRUSTSTORE, "/a/truststore/filepath/truststore.jks");
        }});

        assertFalse(properties.isTlsConfigurationPresent());
    }

    @Test
    public void testTlsConfigurationIsNotPresentWithNoProperties() {
        NiFiProperties properties = NiFiProperties.createBasicNiFiProperties(null, new HashMap<String, String>() {{
        }});

        assertFalse(properties.isTlsConfigurationPresent());
    }

    @Test
    public void testGetPropertiesWithPrefixWithoutDot() {
        // given
        final NiFiProperties testSubject = loadNiFiProperties("/NiFiProperties/conf/nifi.properties", null);

        // when
        final Map<String, String> result = testSubject.getPropertiesWithPrefix("nifi.web.http");

        // then
        Assert.assertEquals(4, result.size());
        Assert.assertTrue(result.containsKey("nifi.web.http.host"));
        Assert.assertTrue(result.containsKey("nifi.web.https.host"));
    }

    @Test
    public void testGetPropertiesWithPrefixWithDot() {
        // given
        final NiFiProperties testSubject = loadNiFiProperties("/NiFiProperties/conf/nifi.properties", null);

        // when
        final Map<String, String> result = testSubject.getPropertiesWithPrefix("nifi.web.http.");

        // then
        Assert.assertEquals(2, result.size());
        Assert.assertTrue(result.containsKey("nifi.web.http.host"));
        Assert.assertFalse(result.containsKey("nifi.web.https.host"));
    }

    @Test
    public void testGetPropertiesWithPrefixWhenNoResult() {
        // given
        final NiFiProperties testSubject = loadNiFiProperties("/NiFiProperties/conf/nifi.properties", null);

        // when
        final Map<String, String> result = testSubject.getPropertiesWithPrefix("invalid.property");

        // then
        Assert.assertTrue(result.isEmpty());
    }

    @Test
    public void testGetDirectSubsequentTokensWithoutDot() {
        // given
        final NiFiProperties testSubject = loadNiFiProperties("/NiFiProperties/conf/nifi.properties", null);

        // when
        final Set<String> result = testSubject.getDirectSubsequentTokens("nifi.web.http");

        // then
        Assert.assertEquals(2, result.size());
        Assert.assertTrue(result.contains("host"));
        Assert.assertTrue(result.contains("port"));
    }

    @Test
    public void testGetDirectSubsequentTokensWithDot() {
        // given
        final NiFiProperties testSubject = loadNiFiProperties("/NiFiProperties/conf/nifi.properties", null);

        // when
        final Set<String> result = testSubject.getDirectSubsequentTokens("nifi.web.http.");

        // then
        Assert.assertEquals(2, result.size());
        Assert.assertTrue(result.contains("host"));
        Assert.assertTrue(result.contains("port"));
    }

    @Test
    public void testGetDirectSubsequentTokensWithNonExistingToken() {
        // given
        final NiFiProperties testSubject = loadNiFiProperties("/NiFiProperties/conf/nifi.properties", null);

        // when
        final Set<String> result = testSubject.getDirectSubsequentTokens("lorem.ipsum");

        // then
        Assert.assertTrue(result.isEmpty());
    }

    @Test
    public void testGetDirectSubsequentTokensWhenMoreTokensAfterward() {
        // given
        final NiFiProperties testSubject = loadNiFiProperties("/NiFiProperties/conf/nifi.properties", null);

        // when
        final Set<String> result = testSubject.getDirectSubsequentTokens("nifi.web");

        // then
        Assert.assertEquals(4, result.size());
        Assert.assertTrue(result.contains("http"));
        Assert.assertTrue(result.contains("https"));
        Assert.assertTrue(result.contains("war"));
        Assert.assertTrue(result.contains("jetty"));
    }
}
