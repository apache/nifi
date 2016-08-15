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

package org.apache.nifi.toolkit.tls.service.server;

import org.apache.nifi.toolkit.tls.commandLine.CommandLineParseException;
import org.apache.nifi.toolkit.tls.configuration.TlsConfig;
import org.apache.nifi.toolkit.tls.util.InputStreamFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(MockitoJUnitRunner.class)
public class TlsCertificateAuthorityServiceCommandLineTest {
    @Mock
    InputStreamFactory inputStreamFactory;

    TlsCertificateAuthorityServiceCommandLine tlsCertificateAuthorityServiceCommandLine;

    String testToken;

    @Before
    public void setup() {
        tlsCertificateAuthorityServiceCommandLine = new TlsCertificateAuthorityServiceCommandLine(inputStreamFactory);
        testToken = "testToken";
    }

    @Test
    public void testDefaults() throws CommandLineParseException, IOException {
        tlsCertificateAuthorityServiceCommandLine.parse("-t", testToken);
        TlsConfig tlsConfig = tlsCertificateAuthorityServiceCommandLine.createConfig();
        assertEquals(TlsConfig.DEFAULT_HOSTNAME, tlsConfig.getCaHostname());
        assertEquals(testToken, tlsConfig.getToken());
        assertEquals(TlsConfig.DEFAULT_PORT, tlsConfig.getPort());
        assertEquals(TlsConfig.DEFAULT_KEY_STORE_TYPE, tlsConfig.getKeyStoreType());
        assertEquals(TlsCertificateAuthorityServiceCommandLine.NIFI_CA_KEYSTORE + tlsConfig.getKeyStoreType().toLowerCase(), tlsConfig.getKeyStore());
        assertNull(tlsConfig.getKeyStorePassword());
        assertNull(tlsConfig.getKeyPassword());
        assertEquals(TlsConfig.DEFAULT_KEY_SIZE, tlsConfig.getKeySize());
        assertEquals(TlsConfig.DEFAULT_KEY_PAIR_ALGORITHM, tlsConfig.getKeyPairAlgorithm());
        assertEquals(TlsConfig.DEFAULT_SIGNING_ALGORITHM, tlsConfig.getSigningAlgorithm());
        assertEquals(TlsConfig.DEFAULT_DAYS, tlsConfig.getDays());
    }

    @Test
    public void testCaHostname() throws CommandLineParseException, IOException {
        String testCaHostname = "testCaHostname";
        tlsCertificateAuthorityServiceCommandLine.parse("-t", testToken, "-c", testCaHostname);
        assertEquals(testCaHostname, tlsCertificateAuthorityServiceCommandLine.createConfig().getCaHostname());
    }

    @Test
    public void testPort() throws CommandLineParseException, IOException {
        int testPort = 4321;
        tlsCertificateAuthorityServiceCommandLine.parse("-t", testToken, "-p", Integer.toString(testPort));
        assertEquals(testPort, tlsCertificateAuthorityServiceCommandLine.createConfig().getPort());
    }

    @Test
    public void testKeyStoreType() throws CommandLineParseException, IOException {
        String testKeyStoreType = "testKeyStoreType";
        tlsCertificateAuthorityServiceCommandLine.parse("-t", testToken, "-T", testKeyStoreType);
        TlsConfig tlsConfig = tlsCertificateAuthorityServiceCommandLine.createConfig();
        assertEquals(testKeyStoreType, tlsConfig.getKeyStoreType());
        assertEquals(TlsCertificateAuthorityServiceCommandLine.NIFI_CA_KEYSTORE + tlsConfig.getKeyStoreType().toLowerCase(), tlsConfig.getKeyStore());
    }

    @Test
    public void testKeySize() throws CommandLineParseException, IOException {
        int testKeySize = 8192;
        tlsCertificateAuthorityServiceCommandLine.parse("-t", testToken, "-k", Integer.toString(testKeySize));
        assertEquals(testKeySize, tlsCertificateAuthorityServiceCommandLine.createConfig().getKeySize());
    }

    @Test
    public void testKeyPairAlgorithm() throws CommandLineParseException, IOException {
        String testAlgorithm = "testAlgorithm";
        tlsCertificateAuthorityServiceCommandLine.parse("-t", testToken, "-a", testAlgorithm);
        assertEquals(testAlgorithm, tlsCertificateAuthorityServiceCommandLine.createConfig().getKeyPairAlgorithm());
    }

    @Test
    public void testSigningAlgorithm() throws CommandLineParseException, IOException {
        String testSigningAlgorithm = "testSigningAlgorithm";
        tlsCertificateAuthorityServiceCommandLine.parse("-t", testToken, "-s", testSigningAlgorithm);
        assertEquals(testSigningAlgorithm, tlsCertificateAuthorityServiceCommandLine.createConfig().getSigningAlgorithm());
    }

    @Test
    public void testDays() throws CommandLineParseException, IOException {
        int days = 1234;
        tlsCertificateAuthorityServiceCommandLine.parse("-t", testToken, "-d", Integer.toString(days));
        assertEquals(days, tlsCertificateAuthorityServiceCommandLine.createConfig().getDays());
    }
}
