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
package org.apache.nifi.web.security.saml.impl.tls;

import org.opensaml.xml.security.CriteriaSet;
import org.opensaml.xml.security.SecurityException;
import org.opensaml.xml.security.credential.Credential;
import org.springframework.security.saml.key.KeyManager;

import java.security.cert.X509Certificate;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * KeyManager implementation that combines two KeyManager instances where one instance represents a keystore containing
 * the service provider's private key (i.e. nifi's keystore.jks) and the other represents a keystore containing the
 * trusted certificates (i.e. nifi's truststore.jks).
 *
 * During any call that requires resolution of a Credential, the server KeyManager is always checked first, if nothing
 * is found then the trust KeyManager is checked.
 *
 * The default Credential is considered that default Credential from the server KeyManager.
 */
public class CompositeKeyManager implements KeyManager {

    private final KeyManager serverKeyManager;
    private final KeyManager trustKeyManager;

    public CompositeKeyManager(final KeyManager serverKeyManager, final KeyManager trustKeyManager) {
        this.serverKeyManager = Objects.requireNonNull(serverKeyManager);
        this.trustKeyManager = Objects.requireNonNull(trustKeyManager);
    }

    @Override
    public Credential getCredential(String keyName) {
        if (keyName == null) {
            return serverKeyManager.getDefaultCredential();
        }

        Credential credential = serverKeyManager.getCredential(keyName);
        if (credential == null) {
            credential = trustKeyManager.getCredential(keyName);
        }

        return credential;
    }

    @Override
    public Credential getDefaultCredential() {
        return serverKeyManager.getDefaultCredential();
    }

    @Override
    public String getDefaultCredentialName() {
        return serverKeyManager.getDefaultCredentialName();
    }

    @Override
    public Set<String> getAvailableCredentials() {
        final Set<String> allCredentials = new HashSet<>();
        allCredentials.addAll(serverKeyManager.getAvailableCredentials());
        allCredentials.addAll(trustKeyManager.getAvailableCredentials());
        return allCredentials;
    }

    @Override
    public X509Certificate getCertificate(String alias) {
        X509Certificate certificate = serverKeyManager.getCertificate(alias);
        if (certificate == null) {
            certificate = trustKeyManager.getCertificate(alias);
        }
        return certificate;
    }

    @Override
    public Iterable<Credential> resolve(CriteriaSet criteria) throws SecurityException {
        Iterable<Credential> credentials = serverKeyManager.resolve(criteria);
        if (credentials == null || !credentials.iterator().hasNext()) {
            credentials = trustKeyManager.resolve(criteria);
        }
        return credentials;
    }

    @Override
    public Credential resolveSingle(CriteriaSet criteria) throws SecurityException {
        Credential credential = serverKeyManager.resolveSingle(criteria);
        if (credential == null) {
            trustKeyManager.resolveSingle(criteria);
        }
        return credential;
    }
}
