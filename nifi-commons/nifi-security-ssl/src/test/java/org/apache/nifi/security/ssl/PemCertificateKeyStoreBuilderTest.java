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
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.util.Enumeration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PemCertificateKeyStoreBuilderTest {
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
        final PemCertificateKeyStoreBuilder builder = new PemCertificateKeyStoreBuilder();

        final String certificateFormatted = StandardPemCertificateReaderTest.getCertificateFormatted(certificate);
        final byte[] certificatePem = certificateFormatted.getBytes(StandardCharsets.US_ASCII);
        builder.inputStream(new ByteArrayInputStream(certificatePem));

        final KeyStore keyStore = builder.build();

        assertNotNull(keyStore);
        assertCertificateEntryFound(keyStore);
    }

    private void assertCertificateEntryFound(final KeyStore keyStore) throws GeneralSecurityException  {
        final Enumeration<String> aliases = keyStore.aliases();

        assertTrue(aliases.hasMoreElements());

        final String alias = aliases.nextElement();

        final Certificate certificateFound = keyStore.getCertificate(alias);
        assertEquals(certificate, certificateFound);

        assertFalse(aliases.hasMoreElements());
    }
}
