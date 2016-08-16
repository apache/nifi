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

import org.apache.nifi.toolkit.tls.configuration.TlsClientConfig;
import org.apache.nifi.toolkit.tls.manager.writer.ConfigurationWriter;
import org.apache.nifi.toolkit.tls.util.InputStreamFactory;
import org.apache.nifi.toolkit.tls.util.OutputStreamFactory;
import org.apache.nifi.toolkit.tls.util.PasswordUtil;
import org.apache.nifi.toolkit.tls.util.TlsHelper;
import org.apache.nifi.util.StringUtils;
import org.bouncycastle.openssl.jcajce.JcaMiscPEMGenerator;
import org.bouncycastle.util.io.pem.PemWriter;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.UnrecoverableEntryException;
import java.security.cert.Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TlsClientManager extends BaseTlsManager {
    private final TlsClientConfig tlsClientConfig;
    private final KeyStore trustStore;
    private final List<ConfigurationWriter<TlsClientConfig>> configurationWriters;
    private final Set<String> certificateAliases;
    private File certificateAuthorityDirectory;

    public TlsClientManager(TlsClientConfig tlsClientConfig) throws GeneralSecurityException, IOException {
        this(tlsClientConfig, new PasswordUtil(), FileInputStream::new);
    }

    public TlsClientManager(TlsClientConfig tlsClientConfig, PasswordUtil passwordUtil, InputStreamFactory inputStreamFactory) throws GeneralSecurityException, IOException {
        super(tlsClientConfig, passwordUtil, inputStreamFactory);
        this.trustStore = loadKeystore(tlsClientConfig.getTrustStore(), tlsClientConfig.getTrustStoreType(), tlsClientConfig.getTrustStorePassword());
        this.tlsClientConfig = tlsClientConfig;
        this.configurationWriters = new ArrayList<>();
        this.certificateAliases = new HashSet<>();
    }

    public void setCertificateEntry(String alias, Certificate cert) throws KeyStoreException {
        trustStore.setCertificateEntry(alias, cert);
        certificateAliases.add(alias);
    }

    public void setCertificateAuthorityDirectory(File certificateAuthorityDirectory) {
        this.certificateAuthorityDirectory = certificateAuthorityDirectory;
    }

    @Override
    public void write(OutputStreamFactory outputStreamFactory) throws IOException, GeneralSecurityException {
        super.write(outputStreamFactory);

        String trustStorePassword = tlsClientConfig.getTrustStorePassword();
        boolean trustStorePasswordGenerated = false;
        if (StringUtils.isEmpty(trustStorePassword)) {
            trustStorePassword = getPasswordUtil().generatePassword();
            trustStorePasswordGenerated = true;
        }

        trustStorePassword = TlsHelper.writeKeyStore(trustStore, outputStreamFactory, new File(tlsClientConfig.getTrustStore()), trustStorePassword, trustStorePasswordGenerated);
        tlsClientConfig.setTrustStorePassword(trustStorePassword);

        for (ConfigurationWriter<TlsClientConfig> configurationWriter : configurationWriters) {
            configurationWriter.write(tlsClientConfig, outputStreamFactory);
        }

        if (certificateAuthorityDirectory != null) {
            // Write out all trusted certificates from truststore
            for (String alias : Collections.list(trustStore.aliases())) {
                try {
                    KeyStore.Entry trustStoreEntry = trustStore.getEntry(alias, null);
                    if (trustStoreEntry instanceof KeyStore.TrustedCertificateEntry) {
                        Certificate trustedCertificate = ((KeyStore.TrustedCertificateEntry) trustStoreEntry).getTrustedCertificate();
                        try (OutputStream outputStream = outputStreamFactory.create(new File(certificateAuthorityDirectory, alias + ".pem"));
                             OutputStreamWriter outputStreamWriter = new OutputStreamWriter(outputStream);
                             PemWriter pemWriter = new PemWriter(outputStreamWriter)) {
                            pemWriter.writeObject(new JcaMiscPEMGenerator(trustedCertificate));
                        }
                    }
                } catch (UnrecoverableEntryException e) {
                    // Ignore, not a trusted cert
                }
            }
        }
    }

    public void addClientConfigurationWriter(ConfigurationWriter<TlsClientConfig> configurationWriter) {
        configurationWriters.add(configurationWriter);
    }
}
