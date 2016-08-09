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

package org.apache.nifi.toolkit.tls.manager;

import org.apache.nifi.security.util.CertificateUtils;
import org.apache.nifi.toolkit.tls.configuration.TlsConfig;
import org.apache.nifi.toolkit.tls.standalone.TlsToolkitStandalone;
import org.apache.nifi.toolkit.tls.util.TlsHelper;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.cert.X509Certificate;

public class TlsCertificateAuthorityManager extends BaseTlsManager {
    public TlsCertificateAuthorityManager(TlsConfig tlsConfig) throws GeneralSecurityException, IOException {
        super(tlsConfig);
    }

    public KeyStore.PrivateKeyEntry getOrGenerateCertificateAuthority() throws GeneralSecurityException, IOException {
        KeyStore.Entry entry = getEntry(TlsToolkitStandalone.NIFI_KEY);
        if (entry == null) {
            TlsConfig tlsConfig = getTlsConfig();
            KeyPair keyPair = TlsHelper.generateKeyPair(tlsConfig.getKeyPairAlgorithm(), tlsConfig.getKeySize());
            X509Certificate caCert = CertificateUtils.generateSelfSignedX509Certificate(keyPair, CertificateUtils.reorderDn(tlsConfig.getDn()), tlsConfig.getSigningAlgorithm(), tlsConfig.getDays());
            entry = addPrivateKeyToKeyStore(keyPair, TlsToolkitStandalone.NIFI_KEY, caCert);
        } else if (!KeyStore.PrivateKeyEntry.class.isInstance(entry)) {
            throw new IOException("Expected " + TlsToolkitStandalone.NIFI_KEY + " alias to contain a private key entry");
        }

        return (KeyStore.PrivateKeyEntry) entry;
    }
}
