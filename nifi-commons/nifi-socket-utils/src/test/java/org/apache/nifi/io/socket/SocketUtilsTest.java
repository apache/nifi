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
package org.apache.nifi.io.socket;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.nifi.security.util.KeystoreType;
import org.apache.nifi.security.util.StandardTlsConfiguration;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.security.util.TlsException;
import org.apache.nifi.util.NiFiProperties;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLServerSocket;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class SocketUtilsTest {
    private static final String KEYSTORE_PATH = "src/test/resources/TlsConfigurationKeystore.jks";
    private static final String KEYSTORE_PASSWORD = "keystorepassword";
    private static final String KEY_PASSWORD = "keypassword";
    private static final KeystoreType KEYSTORE_TYPE = KeystoreType.JKS;
    private static final String TRUSTSTORE_PATH = "src/test/resources/TlsConfigurationTruststore.jks";
    private static final String TRUSTSTORE_PASSWORD = "truststorepassword";
    private static final KeystoreType TRUSTSTORE_TYPE = KeystoreType.JKS;
    private static NiFiProperties mockNiFiProperties;

    @BeforeAll
    public static void setUpOnce() throws Exception {
        final Map<String, String> defaultProps = new HashMap<>();
        defaultProps.put(org.apache.nifi.util.NiFiProperties.SECURITY_KEYSTORE, KEYSTORE_PATH);
        defaultProps.put(org.apache.nifi.util.NiFiProperties.SECURITY_KEYSTORE_PASSWD, KEYSTORE_PASSWORD);
        defaultProps.put(org.apache.nifi.util.NiFiProperties.SECURITY_KEY_PASSWD, KEY_PASSWORD);
        defaultProps.put(org.apache.nifi.util.NiFiProperties.SECURITY_KEYSTORE_TYPE, KEYSTORE_TYPE.getType());
        defaultProps.put(org.apache.nifi.util.NiFiProperties.SECURITY_TRUSTSTORE, TRUSTSTORE_PATH);
        defaultProps.put(org.apache.nifi.util.NiFiProperties.SECURITY_TRUSTSTORE_PASSWD, TRUSTSTORE_PASSWORD);
        defaultProps.put(org.apache.nifi.util.NiFiProperties.SECURITY_TRUSTSTORE_TYPE, TRUSTSTORE_TYPE.getType());
        mockNiFiProperties = NiFiProperties.createBasicNiFiProperties(null, defaultProps);
    }

    @Test
    public void testCreateSSLServerSocketShouldRestrictTlsProtocols() throws TlsException, IOException {
        ServerSocketConfiguration mockServerSocketConfiguration = new ServerSocketConfiguration();
        mockServerSocketConfiguration.setTlsConfiguration(StandardTlsConfiguration.fromNiFiProperties(mockNiFiProperties));

        try (SSLServerSocket sslServerSocket = SocketUtils.createSSLServerSocket(0, mockServerSocketConfiguration)) {
            String[] enabledProtocols = sslServerSocket.getEnabledProtocols();
            assertArrayEquals(TlsConfiguration.getCurrentSupportedTlsProtocolVersions(), enabledProtocols);
            assertFalse(ArrayUtils.contains(enabledProtocols, "TLSv1"));
            assertFalse(ArrayUtils.contains(enabledProtocols, "TLSv1.1"));
        }
    }

    @Test
    public void testCreateServerSocketShouldRestrictTlsProtocols() throws TlsException, IOException {
        ServerSocketConfiguration mockServerSocketConfiguration = new ServerSocketConfiguration();
        mockServerSocketConfiguration.setTlsConfiguration(StandardTlsConfiguration.fromNiFiProperties(mockNiFiProperties));

        try (SSLServerSocket sslServerSocket = (SSLServerSocket)SocketUtils.createServerSocket(0, mockServerSocketConfiguration)) {
            String[] enabledProtocols = sslServerSocket.getEnabledProtocols();
            assertArrayEquals(TlsConfiguration.getCurrentSupportedTlsProtocolVersions(), enabledProtocols);
            assertFalse(ArrayUtils.contains(enabledProtocols, "TLSv1"));
            assertFalse(ArrayUtils.contains(enabledProtocols, "TLSv1.1"));
        }
    }
}
