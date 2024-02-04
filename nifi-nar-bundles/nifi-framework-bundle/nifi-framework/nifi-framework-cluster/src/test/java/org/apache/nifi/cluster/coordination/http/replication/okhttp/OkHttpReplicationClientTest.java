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

package org.apache.nifi.cluster.coordination.http.replication.okhttp;

import org.apache.nifi.cluster.coordination.http.replication.PreparedRequest;
import org.apache.nifi.security.util.TemporaryKeyStoreBuilder;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.util.NiFiProperties;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class OkHttpReplicationClientTest {
    private static TlsConfiguration tlsConfiguration;

    @BeforeAll
    public static void setUpOnce() {
        tlsConfiguration = new TemporaryKeyStoreBuilder().build();
    }

    @Test
    public void testShouldReplaceNonZeroContentLengthHeader() {
        final Map<String, String> headers = new HashMap<>();
        headers.put("Content-Length", "123");
        headers.put("Other-Header", "arbitrary value");

        // must be case-insensitive
        final String[] methods = new String[] {"DELETE", "delete", "DeLeTe"};

        final NiFiProperties mockProperties = mockNiFiProperties();

        final OkHttpReplicationClient client = new OkHttpReplicationClient(mockProperties);

        for (final String method: methods) {
            client.prepareRequest(method, headers, null);

            assertEquals(2, headers.size());
            assertEquals("0", headers.get("Content-Length"));
        }
    }

    @Test
    void testShouldNotReplaceContentLengthHeaderWhenZeroOrNull() {
        final String method = "DELETE";
        final String[] zeroOrNullContentLengths = new String[] {null, "0"};

        final NiFiProperties mockProperties = mockNiFiProperties();

        final OkHttpReplicationClient client = new OkHttpReplicationClient(mockProperties);

        final Map<String, String> headers = new HashMap<>();
        for (final String contentLength: zeroOrNullContentLengths) {
            headers.put("Content-Length", contentLength);
            headers.put("Other-Header", "arbitrary value");

            client.prepareRequest(method, headers, null);

            assertEquals(2, headers.size());
            assertEquals(contentLength, headers.get("Content-Length"));
        }
    }

    @Test
    void testShouldNotReplaceNonZeroContentLengthHeaderOnOtherMethod() {
        final Map<String, String> headers = new HashMap<>();
        headers.put("Content-Length", "123");
        headers.put("Other-Header", "arbitrary value");

        final NiFiProperties mockProperties = mockNiFiProperties();

        final OkHttpReplicationClient client = new OkHttpReplicationClient(mockProperties);

        final String[] nonDeleteMethods = new String[] {"POST", "PUT", "GET", "HEAD"};

        for (final String method: nonDeleteMethods) {
            client.prepareRequest(method, headers, null);

            assertEquals(2, headers.size());
            assertEquals("123", headers.get("Content-Length"));
        }
    }

    @Test
    void testShouldReadCasInsensitiveAcceptEncoding() {
        final Map<String, String> headers = new HashMap<>();
        headers.put("accept-encoding", "gzip");
        headers.put("Other-Header", "arbitrary value");

        final NiFiProperties mockProperties = mockNiFiProperties();

        final OkHttpReplicationClient client = new OkHttpReplicationClient(mockProperties);


        PreparedRequest request = client.prepareRequest("POST", headers, "TEST");

        assertEquals(3, request.getHeaders().size());
        assertEquals("gzip", request.getHeaders().get("accept-encoding"));
        assertEquals("gzip", request.getHeaders().get("Content-Encoding"));
        assertEquals("arbitrary value", request.getHeaders().get("Other-Header"));
    }

    @Test
    void testShouldUseKeystorePasswordIfKeyPasswordIsBlank() {
        final Map<String, String> propsMap = new HashMap<>();
        propsMap.put(NiFiProperties.SECURITY_TRUSTSTORE, tlsConfiguration.getTruststorePath());
        propsMap.put(NiFiProperties.SECURITY_TRUSTSTORE_TYPE, tlsConfiguration.getTruststoreType().getType());
        propsMap.put(NiFiProperties.SECURITY_TRUSTSTORE_PASSWD, tlsConfiguration.getTruststorePassword());
        propsMap.put(NiFiProperties.SECURITY_KEYSTORE, tlsConfiguration.getKeystorePath());
        propsMap.put(NiFiProperties.SECURITY_KEYSTORE_TYPE, tlsConfiguration.getKeystoreType().getType());
        propsMap.put(NiFiProperties.SECURITY_KEYSTORE_PASSWD, tlsConfiguration.getKeystorePassword());
        propsMap.put(NiFiProperties.SECURITY_KEY_PASSWD, "");
        propsMap.put(NiFiProperties.WEB_HTTPS_HOST, "localhost");
        propsMap.put(NiFiProperties.WEB_HTTPS_PORT, "51552");

        final NiFiProperties mockNiFiProperties = new NiFiProperties(propsMap);

        final OkHttpReplicationClient client = new OkHttpReplicationClient(mockNiFiProperties);

        assertTrue(client.isTLSConfigured());
    }

    @Test
    void testShouldUseKeystorePasswordIfKeyPasswordIsNull() {
        final Map<String, String> flowfileEncryptionProps = new HashMap<>();
        flowfileEncryptionProps.put(NiFiProperties.SECURITY_TRUSTSTORE, tlsConfiguration.getTruststorePath());
        flowfileEncryptionProps.put(NiFiProperties.SECURITY_TRUSTSTORE_TYPE, tlsConfiguration.getTruststoreType().getType());
        flowfileEncryptionProps.put(NiFiProperties.SECURITY_TRUSTSTORE_PASSWD, tlsConfiguration.getTruststorePassword());
        flowfileEncryptionProps.put(NiFiProperties.SECURITY_KEYSTORE, tlsConfiguration.getKeystorePath());
        flowfileEncryptionProps.put(NiFiProperties.SECURITY_KEYSTORE_TYPE, tlsConfiguration.getKeystoreType().getType());
        flowfileEncryptionProps.put(NiFiProperties.SECURITY_KEYSTORE_PASSWD, tlsConfiguration.getKeystorePassword());
        flowfileEncryptionProps.put(NiFiProperties.WEB_HTTPS_HOST, "localhost");
        flowfileEncryptionProps.put(NiFiProperties.WEB_HTTPS_PORT, "51552");

        final NiFiProperties mockNiFiProperties = new NiFiProperties(flowfileEncryptionProps);

        final OkHttpReplicationClient client = new OkHttpReplicationClient(mockNiFiProperties);

        assertTrue(client.isTLSConfigured());
    }

    @Test
    void testShouldFailIfKeyPasswordIsSetButKeystorePasswordIsBlank() {
        final Map<String, String> propsMap = new HashMap<>();
        propsMap.put(NiFiProperties.SECURITY_TRUSTSTORE, tlsConfiguration.getTruststorePath());
        propsMap.put(NiFiProperties.SECURITY_TRUSTSTORE_TYPE, tlsConfiguration.getTruststoreType().getType());
        propsMap.put(NiFiProperties.SECURITY_KEYSTORE, tlsConfiguration.getKeystorePath());
        propsMap.put(NiFiProperties.SECURITY_KEYSTORE_TYPE, tlsConfiguration.getKeystoreType().getType());
        propsMap.put(NiFiProperties.SECURITY_KEYSTORE_PASSWD, "");
        propsMap.put(NiFiProperties.WEB_HTTPS_HOST, "localhost");
        propsMap.put(NiFiProperties.WEB_HTTPS_PORT, "51552");

        final NiFiProperties mockNiFiProperties = new NiFiProperties(propsMap);

        final OkHttpReplicationClient client = new OkHttpReplicationClient(mockNiFiProperties);

        assertFalse(client.isTLSConfigured());
    }

    @Test
    void testShouldFailIfKeyPasswordAndKeystorePasswordAreBlank() {
        final Map<String, String> propsMap = new HashMap<>();
        propsMap.put(NiFiProperties.SECURITY_TRUSTSTORE, tlsConfiguration.getTruststorePath());
        propsMap.put(NiFiProperties.SECURITY_TRUSTSTORE_TYPE, tlsConfiguration.getTruststoreType().getType());
        propsMap.put(NiFiProperties.SECURITY_KEYSTORE, tlsConfiguration.getKeystorePath());
        propsMap.put(NiFiProperties.SECURITY_KEYSTORE_TYPE, tlsConfiguration.getKeystoreType().getType());
        propsMap.put(NiFiProperties.SECURITY_KEYSTORE_PASSWD, "");
        propsMap.put(NiFiProperties.SECURITY_KEY_PASSWD, "");
        propsMap.put(NiFiProperties.WEB_HTTPS_HOST, "localhost");
        propsMap.put(NiFiProperties.WEB_HTTPS_PORT, "51552");

        final NiFiProperties mockNiFiProperties = new NiFiProperties(propsMap);

        final OkHttpReplicationClient client = new OkHttpReplicationClient(mockNiFiProperties);

        assertFalse(client.isTLSConfigured());
    }

    @Test
    void testShouldDetermineIfTLSConfigured() {
        final Map<String, String> propsMap = new HashMap<>();
        propsMap.put(NiFiProperties.WEB_HTTPS_HOST, "localhost");
        propsMap.put(NiFiProperties.WEB_HTTPS_PORT, "51552");

        final Map<String, String> tlsPropsMap = new HashMap<>(propsMap);
        tlsPropsMap.put(NiFiProperties.SECURITY_TRUSTSTORE, tlsConfiguration.getTruststorePath());
        tlsPropsMap.put(NiFiProperties.SECURITY_TRUSTSTORE_TYPE, tlsConfiguration.getTruststoreType().getType());
        tlsPropsMap.put(NiFiProperties.SECURITY_TRUSTSTORE_PASSWD, tlsConfiguration.getTruststorePassword());
        tlsPropsMap.put(NiFiProperties.SECURITY_KEYSTORE, tlsConfiguration.getKeystorePath());
        tlsPropsMap.put(NiFiProperties.SECURITY_KEYSTORE_TYPE, tlsConfiguration.getKeystoreType().getType());
        tlsPropsMap.put(NiFiProperties.SECURITY_KEYSTORE_PASSWD, tlsConfiguration.getKeystorePassword());

        final Map<String, String> invalidTlsPropsMap = new HashMap<>(tlsPropsMap);
        // Remove the keystore password to create an invalid configuration
        invalidTlsPropsMap.remove(NiFiProperties.SECURITY_KEYSTORE_PASSWD);

        final NiFiProperties mockNiFiProperties = new NiFiProperties(propsMap);
        final NiFiProperties mockTLSNiFiProperties = new NiFiProperties(tlsPropsMap);
        final NiFiProperties mockInvalidTLSNiFiProperties = new NiFiProperties(invalidTlsPropsMap);

        final OkHttpReplicationClient client = new OkHttpReplicationClient(mockNiFiProperties);
        final OkHttpReplicationClient invalidTlsClient = new OkHttpReplicationClient(mockInvalidTLSNiFiProperties);
        final OkHttpReplicationClient tlsClient = new OkHttpReplicationClient(mockTLSNiFiProperties);

        assertFalse(client.isTLSConfigured());
        assertFalse(invalidTlsClient.isTLSConfigured());
        assertTrue(tlsClient.isTLSConfigured());
    }

    private static NiFiProperties mockNiFiProperties() {
        return NiFiProperties.createBasicNiFiProperties(null);
    }
}
