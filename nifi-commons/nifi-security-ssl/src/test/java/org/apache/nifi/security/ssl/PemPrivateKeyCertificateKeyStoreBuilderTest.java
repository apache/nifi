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
package org.apache.nifi.security.ssl;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.util.Enumeration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PemPrivateKeyCertificateKeyStoreBuilderTest {
    private static final int RSA_KEY_SIZE = 3072;

    private static final String RSA_ALGORITHM = "RSA";

    private static Certificate certificate;

    @BeforeAll
    static void setTrustStore() throws Exception {
        final KeyStore trustStore = StandardPemCertificateReaderTest.loadDefaultTrustStore();

        final Enumeration<String> aliases = trustStore.aliases();
        final String alias = aliases.nextElement();
        certificate = trustStore.getCertificate(alias);
    }

    @Test
    void testBuild() throws GeneralSecurityException {
        final PemPrivateKeyCertificateKeyStoreBuilder builder = new PemPrivateKeyCertificateKeyStoreBuilder();

        final String privateKeyEncoded = StandardPemPrivateKeyReaderTest.getRsaPrivateKeyEncoded(RSA_KEY_SIZE);
        builder.privateKeyInputStream(new ByteArrayInputStream(privateKeyEncoded.getBytes(StandardCharsets.US_ASCII)));

        final String certificateFormatted = StandardPemCertificateReaderTest.getCertificateFormatted(certificate);
        final byte[] certificatePem = certificateFormatted.getBytes(StandardCharsets.US_ASCII);
        builder.certificateInputStream(new ByteArrayInputStream(certificatePem));

        final KeyStore keyStore = builder.build();

        assertNotNull(keyStore);
        assertKeyEntryFound(keyStore);
    }

    private void assertKeyEntryFound(final KeyStore keyStore) throws GeneralSecurityException  {
        final Enumeration<String> aliases = keyStore.aliases();

        assertTrue(aliases.hasMoreElements());

        final String alias = aliases.nextElement();

        final Certificate[] certificateChain = keyStore.getCertificateChain(alias);
        assertNotNull(certificateChain);

        final Certificate certificateFound = certificateChain[0];
        assertEquals(certificate, certificateFound);

        final Key key = keyStore.getKey(alias, null);
        assertNotNull(key);
        assertEquals(RSA_ALGORITHM, key.getAlgorithm());

        assertFalse(aliases.hasMoreElements());
    }
}
