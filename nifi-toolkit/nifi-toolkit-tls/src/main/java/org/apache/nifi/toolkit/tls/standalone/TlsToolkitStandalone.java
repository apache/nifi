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

package org.apache.nifi.toolkit.tls.standalone;

import org.apache.nifi.security.util.CertificateUtils;
import org.apache.nifi.toolkit.tls.configuration.TlsClientConfig;
import org.apache.nifi.toolkit.tls.configuration.TlsConfig;
import org.apache.nifi.toolkit.tls.manager.TlsCertificateAuthorityManager;
import org.apache.nifi.toolkit.tls.manager.TlsClientManager;
import org.apache.nifi.toolkit.tls.manager.writer.NifiPropertiesTlsClientConfigWriter;
import org.apache.nifi.toolkit.tls.properties.NiFiPropertiesWriterFactory;
import org.apache.nifi.toolkit.tls.util.OutputStreamFactory;
import org.apache.nifi.toolkit.tls.util.TlsHelper;
import org.bouncycastle.openssl.jcajce.JcaMiscPEMGenerator;
import org.bouncycastle.util.io.pem.PemWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.List;

public class TlsToolkitStandalone {
    public static final String NIFI_KEY = "nifi-key";
    public static final String NIFI_CERT = "nifi-cert";
    public static final String NIFI_PROPERTIES = "nifi.properties";

    private final Logger logger = LoggerFactory.getLogger(TlsToolkitStandalone.class);
    private final OutputStreamFactory outputStreamFactory;

    public TlsToolkitStandalone() {
        this(FileOutputStream::new);
    }

    public TlsToolkitStandalone(OutputStreamFactory outputStreamFactory) {
        this.outputStreamFactory = outputStreamFactory;
    }

    public void createNifiKeystoresAndTrustStores(File baseDir, TlsConfig tlsConfig, NiFiPropertiesWriterFactory niFiPropertiesWriterFactory, List<String> hostnames, List<String> keyStorePasswords,
                                                  List<String> keyPasswords, List<String> trustStorePasswords, int httpsPort) throws GeneralSecurityException, IOException {
        String signingAlgorithm = tlsConfig.getSigningAlgorithm();
        int days = tlsConfig.getDays();
        String keyPairAlgorithm = tlsConfig.getKeyPairAlgorithm();
        int keySize = tlsConfig.getKeySize();
        TlsCertificateAuthorityManager tlsCertificateAuthorityManager = new TlsCertificateAuthorityManager(tlsConfig);
        KeyStore.PrivateKeyEntry privateKeyEntry = tlsCertificateAuthorityManager.getOrGenerateCertificateAuthority();
        X509Certificate certificate = (X509Certificate) privateKeyEntry.getCertificateChain()[0];
        KeyPair caKeyPair = new KeyPair(certificate.getPublicKey(), privateKeyEntry.getPrivateKey());

        if (!baseDir.exists() && !baseDir.mkdirs()) {
            throw new IOException(baseDir + " doesn't exist and unable to create it.");
        }

        if (!baseDir.isDirectory()) {
            throw new IOException("Expected directory to output to");
        }

        if (logger.isInfoEnabled()) {
            logger.info("Running standalone certificate generation with output directory " + baseDir + " and hostnames " + hostnames);
        }

        File nifiCert = new File(baseDir, NIFI_CERT + ".pem");
        if (nifiCert.exists()) {
            throw new IOException(nifiCert.getAbsolutePath() + " exists already.");
        }

        File nifiKey = new File(baseDir, NIFI_KEY + ".key");
        if (nifiKey.exists()) {
            throw new IOException(nifiKey.getAbsolutePath() + " exists already.");
        }

        for (String hostname : hostnames) {
            File hostDirectory = new File(baseDir, hostname);
            if (hostDirectory.exists()) {
                throw new IOException("Output destination for host " + hostname + " (" + hostDirectory.getAbsolutePath() + ") exists already.");
            }
        }

        try (PemWriter pemWriter = new PemWriter(new OutputStreamWriter(outputStreamFactory.create(nifiCert)))) {
            pemWriter.writeObject(new JcaMiscPEMGenerator(certificate));
        }

        try (PemWriter pemWriter = new PemWriter(new OutputStreamWriter(outputStreamFactory.create(nifiKey)))) {
            pemWriter.writeObject(new JcaMiscPEMGenerator(caKeyPair));
        }

        for (int i = 0; i < hostnames.size(); i++) {
            String hostname = hostnames.get(i);
            File hostDir = new File(baseDir, hostname);

            if (!hostDir.mkdirs()) {
                throw new IOException("Unable to make directory: " + hostDir.getAbsolutePath());
            }

            TlsClientConfig tlsClientConfig = new TlsClientConfig(tlsConfig);
            tlsClientConfig.setKeyStore(new File(hostDir, "keystore." + tlsClientConfig.getKeyStoreType().toLowerCase()).getAbsolutePath());
            tlsClientConfig.setKeyStorePassword(keyStorePasswords.get(i));
            tlsClientConfig.setKeyPassword(keyPasswords.get(i));
            tlsClientConfig.setTrustStore(new File(hostDir, "truststore." + tlsClientConfig.getTrustStoreType().toLowerCase()).getAbsolutePath());
            tlsClientConfig.setTrustStorePassword(trustStorePasswords.get(i));
            TlsClientManager tlsClientManager = new TlsClientManager(tlsClientConfig);
            KeyPair keyPair = TlsHelper.generateKeyPair(keyPairAlgorithm, keySize);
            tlsClientManager.addPrivateKeyToKeyStore(keyPair, NIFI_KEY, CertificateUtils.generateIssuedCertificate(TlsConfig.calcDefaultDn(hostname),
                    keyPair.getPublic(), certificate, caKeyPair, signingAlgorithm, days), certificate);
            tlsClientManager.setCertificateEntry(NIFI_CERT, certificate);
            tlsClientManager.addClientConfigurationWriter(new NifiPropertiesTlsClientConfigWriter(niFiPropertiesWriterFactory, outputStreamFactory, new File(hostDir, "nifi.properties"),
                    hostname, httpsPort));
            tlsClientManager.write(outputStreamFactory);
            if (logger.isInfoEnabled()) {
                logger.info("Successfully generated TLS configuration for " + hostname + ":" + httpsPort + " in " + hostDir);
            }
        }

        if (logger.isInfoEnabled()) {
            logger.info("Successfully generated TLS configuration for all hosts");
        }
    }
}
