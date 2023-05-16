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
package org.apache.nifi.web.security.saml2.registration;

import org.springframework.security.saml2.Saml2Exception;
import org.springframework.security.saml2.core.Saml2X509Credential;

import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.List;
import java.util.Objects;

/**
 * Standard implementation of SAML2 Credential Provider capable of reading Key Store and Trust Store entries
 */
public class StandardSaml2CredentialProvider implements Saml2CredentialProvider {
    /**
     * Get Credentials from Key Store
     *
     * @param keyStore Key Store containing credentials
     * @param keyPassword Optional key password for loading Private Keys
     * @return Collection of SAML2 X.509 Credentials
     */
    @Override
    public Collection<Saml2X509Credential> getCredentials(final KeyStore keyStore, final char[] keyPassword) {
        Objects.requireNonNull(keyStore, "Key Store required");

        final List<Saml2X509Credential> credentials = new ArrayList<>();

        try {
            final Enumeration<String> aliases = keyStore.aliases();
            while (aliases.hasMoreElements()) {
                final String alias = aliases.nextElement();
                if (keyStore.isKeyEntry(alias)) {
                    processKeyEntry(keyStore, alias, keyPassword, credentials);
                } else if (keyStore.isCertificateEntry(alias)) {
                    processCertificateEntry(keyStore, alias, credentials);
                }
            }
        } catch (final KeyStoreException e) {
            throw new Saml2Exception("Loading SAML Credentials failed", e);
        }

        return credentials;
    }

    private Key getKey(final KeyStore keyStore, final String alias, final char[] keyPassword) {
        try {
            return keyStore.getKey(alias, keyPassword);
        } catch (final GeneralSecurityException e) {
            throw new Saml2Exception(String.format("Loading Key [%s] failed", alias), e);
        }
    }

    private void processKeyEntry(
            final KeyStore keyStore,
            final String alias,
            final char[] keyPassword,
            final List<Saml2X509Credential> credentials
    ) throws KeyStoreException {
        final Key key = getKey(keyStore, alias, keyPassword);
        if (key instanceof PrivateKey) {
            final PrivateKey privateKey = (PrivateKey) key;
            final Certificate certificateEntry = keyStore.getCertificate(alias);
            if (certificateEntry instanceof X509Certificate) {
                final X509Certificate certificate = (X509Certificate) certificateEntry;
                final Saml2X509Credential credential = new Saml2X509Credential(
                        privateKey,
                        certificate,
                        Saml2X509Credential.Saml2X509CredentialType.SIGNING,
                        Saml2X509Credential.Saml2X509CredentialType.DECRYPTION
                );
                credentials.add(credential);
            }
        }
    }

    private void processCertificateEntry(
            final KeyStore keyStore,
            final String alias,
            final List<Saml2X509Credential> credentials
    ) throws KeyStoreException {
        final Certificate certificateEntry = keyStore.getCertificate(alias);
        if (certificateEntry instanceof X509Certificate) {
            final X509Certificate certificate = (X509Certificate) certificateEntry;
            final Saml2X509Credential credential = new Saml2X509Credential(
                    certificate,
                    Saml2X509Credential.Saml2X509CredentialType.VERIFICATION,
                    Saml2X509Credential.Saml2X509CredentialType.ENCRYPTION
            );
            credentials.add(credential);
        }
    }
}
