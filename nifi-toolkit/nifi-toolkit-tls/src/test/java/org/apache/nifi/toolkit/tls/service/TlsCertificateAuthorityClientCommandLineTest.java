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

package org.apache.nifi.toolkit.tls.service;

import org.apache.nifi.toolkit.tls.commandLine.BaseCommandLine;
import org.apache.nifi.toolkit.tls.commandLine.CommandLineParseException;
import org.apache.nifi.toolkit.tls.configuration.TlsClientConfig;
import org.apache.nifi.toolkit.tls.configuration.TlsConfig;
import org.apache.nifi.toolkit.tls.configuration.TlsHelperConfig;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class TlsCertificateAuthorityClientCommandLineTest {

    private TlsCertificateAuthorityClientCommandLine tlsCertificateAuthorityClientCommandLine;
    private String testToken;

    @Before
    public void setup() {
        tlsCertificateAuthorityClientCommandLine = new TlsCertificateAuthorityClientCommandLine();
        testToken = "testToken";
    }

    @Test
    public void testNoToken() {
        try {
            tlsCertificateAuthorityClientCommandLine.parse(new String[0]);
            fail("Expected failure with no token argument");
        } catch (CommandLineParseException e) {
            assertEquals(TlsCertificateAuthorityClientCommandLine.ERROR_TOKEN_ARG_EMPTY, e.getExitCode());
        }
    }

    @Test
    public void testDefaults() throws CommandLineParseException {
        tlsCertificateAuthorityClientCommandLine.parse("-t", testToken);
        TlsClientConfig clientConfig = tlsCertificateAuthorityClientCommandLine.createClientConfig();

        assertEquals(TlsConfig.DEFAULT_HOSTNAME, clientConfig.getCaHostname());
        assertEquals(TlsCertificateSigningRequestPerformer.getDn(TlsConfig.DEFAULT_HOSTNAME), clientConfig.getDn());
        assertEquals(TlsCertificateAuthorityClientCommandLine.KEYSTORE + TlsCertificateAuthorityClientCommandLine.PKCS_12.toLowerCase(), clientConfig.getKeyStore());
        assertEquals(TlsCertificateAuthorityClientCommandLine.PKCS_12, clientConfig.getKeyStoreType());
        assertNull(clientConfig.getKeyStorePassword());
        assertNull(clientConfig.getKeyPassword());
        assertEquals(TlsCertificateAuthorityClientCommandLine.TRUSTSTORE + TlsCertificateAuthorityClientCommandLine.PKCS_12.toLowerCase(), clientConfig.getTrustStore());
        assertEquals(TlsCertificateAuthorityClientCommandLine.PKCS_12, clientConfig.getTrustStoreType());
        assertNull(clientConfig.getTrustStorePassword());
        assertEquals(TlsHelperConfig.DEFAULT_KEY_SIZE, clientConfig.getTlsHelperConfig().getKeySize());
        assertEquals(TlsHelperConfig.DEFAULT_KEY_PAIR_ALGORITHM, clientConfig.getTlsHelperConfig().getKeyPairAlgorithm());
        assertEquals(testToken, clientConfig.getToken());
        assertEquals(TlsConfig.DEFAULT_PORT, clientConfig.getPort());
        assertEquals(TlsCertificateAuthorityClientCommandLine.DEFAULT_CONFIG_JSON, tlsCertificateAuthorityClientCommandLine.getConfigFile());
    }

    @Test
    public void testKeySize() throws CommandLineParseException {
        int keySize = 1234;
        tlsCertificateAuthorityClientCommandLine.parse("-t", testToken, "-k", Integer.toString(keySize));
        assertEquals(keySize, tlsCertificateAuthorityClientCommandLine.getKeySize());
    }

    @Test
    public void testKeyPairAlgorithm() throws CommandLineParseException {
        String testAlgorithm = "testAlgorithm";
        tlsCertificateAuthorityClientCommandLine.parse("-t", testToken, "-a", testAlgorithm);
        assertEquals(testAlgorithm, tlsCertificateAuthorityClientCommandLine.getKeyAlgorithm());
    }

    @Test
    public void testHelp() {
        try {
            tlsCertificateAuthorityClientCommandLine.parse("-h");
            fail("Expected exception");
        } catch (CommandLineParseException e) {
            assertEquals(BaseCommandLine.HELP_EXIT_CODE, e.getExitCode());
        }
    }

    @Test
    public void testCaHostname() throws CommandLineParseException {
        String testCaHostname = "testCaHostname";
        tlsCertificateAuthorityClientCommandLine.parse("-t", testToken, "-c", testCaHostname);
        assertEquals(testCaHostname, tlsCertificateAuthorityClientCommandLine.createClientConfig().getCaHostname());
    }

    @Test
    public void testDn() throws CommandLineParseException {
        String testDn = "testDn";
        tlsCertificateAuthorityClientCommandLine.parse("-t", testToken, "-d", testDn);
        assertEquals(testDn, tlsCertificateAuthorityClientCommandLine.createClientConfig().getDn());
    }

    @Test
    public void testPort() throws CommandLineParseException {
        int testPort = 2345;
        tlsCertificateAuthorityClientCommandLine.parse("-t", testToken, "-p", Integer.toString(testPort));
        assertEquals(testPort, tlsCertificateAuthorityClientCommandLine.createClientConfig().getPort());
    }

    @Test
    public void testKeyStoreType() throws CommandLineParseException {
        String testType = "testType";
        tlsCertificateAuthorityClientCommandLine.parse("-t", testToken, "-T", testType);

        TlsClientConfig clientConfig = tlsCertificateAuthorityClientCommandLine.createClientConfig();
        assertEquals(testType, clientConfig.getKeyStoreType());
        assertEquals(testType, clientConfig.getTrustStoreType());
        assertEquals(TlsCertificateAuthorityClientCommandLine.KEYSTORE + testType.toLowerCase(), clientConfig.getKeyStore());
        assertEquals(TlsCertificateAuthorityClientCommandLine.TRUSTSTORE + testType.toLowerCase(), clientConfig.getTrustStore());
    }

    @Test
    public void testConfigFile() throws CommandLineParseException {
        String testPath = "/1/2/3/4";
        tlsCertificateAuthorityClientCommandLine.parse("-t", testToken, "-f", testPath);
        assertEquals(new File(testPath), tlsCertificateAuthorityClientCommandLine.getConfigFile());
    }
}
