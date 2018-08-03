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

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.SignatureException;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import org.apache.nifi.security.util.CertificateUtils;
import org.apache.nifi.security.util.KeyStoreUtils;
import org.apache.nifi.security.util.KeystoreType;
import org.apache.nifi.toolkit.tls.configuration.InstanceDefinition;
import org.apache.nifi.toolkit.tls.configuration.StandaloneConfig;
import org.apache.nifi.toolkit.tls.configuration.TlsClientConfig;
import org.apache.nifi.toolkit.tls.manager.TlsCertificateAuthorityManager;
import org.apache.nifi.toolkit.tls.manager.TlsClientManager;
import org.apache.nifi.toolkit.tls.manager.writer.NifiPropertiesTlsClientConfigWriter;
import org.apache.nifi.toolkit.tls.properties.NiFiPropertiesWriterFactory;
import org.apache.nifi.toolkit.tls.util.OutputStreamFactory;
import org.apache.nifi.toolkit.tls.util.TlsHelper;
import org.apache.nifi.util.StringUtils;
import org.bouncycastle.asn1.x509.Extensions;
import org.bouncycastle.openssl.jcajce.JcaMiscPEMGenerator;
import org.bouncycastle.util.io.pem.PemWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public void createNifiKeystoresAndTrustStores(StandaloneConfig standaloneConfig) throws GeneralSecurityException, IOException {
        // TODO: This 200 line method should be refactored, as it is difficult to test the various validations separately from the filesystem interaction and generation logic
        File baseDir = standaloneConfig.getBaseDir();
        if (!baseDir.exists() && !baseDir.mkdirs()) {
            throw new IOException(baseDir + " doesn't exist and unable to create it.");
        }

        if (!baseDir.isDirectory()) {
            throw new IOException("Expected directory to output to");
        }

        String signingAlgorithm = standaloneConfig.getSigningAlgorithm();
        int days = standaloneConfig.getDays();
        String keyPairAlgorithm = standaloneConfig.getKeyPairAlgorithm();
        int keySize = standaloneConfig.getKeySize();

        File nifiCert = new File(baseDir, NIFI_CERT + ".pem");
        File nifiKey = new File(baseDir, NIFI_KEY + ".key");

        X509Certificate certificate;
        KeyPair caKeyPair;

        if (logger.isInfoEnabled()) {
            logger.info("Running standalone certificate generation with output directory " + baseDir);
        }
        if (nifiCert.exists()) {
            if (!nifiKey.exists()) {
                throw new IOException(nifiCert + " exists already, but " + nifiKey + " does not, we need both certificate and key to continue with an existing CA.");
            }
            try (FileReader pemEncodedCertificate = new FileReader(nifiCert)) {
                certificate = TlsHelper.parseCertificate(pemEncodedCertificate);
            }
            try (FileReader pemEncodedKeyPair = new FileReader(nifiKey)) {
                caKeyPair = TlsHelper.parseKeyPairFromReader(pemEncodedKeyPair);
            }

            // TODO: Do same in client/server
            // Load additional signing certificates from config
            List<X509Certificate> signingCertificates = new ArrayList<>();

            // Read the provided additional CA certificate if it exists and extract the certificate
            if (!StringUtils.isBlank(standaloneConfig.getAdditionalCACertificate())) {
                X509Certificate signingCertificate;
                final File additionalCACertFile = new File(standaloneConfig.getAdditionalCACertificate());
                if (!additionalCACertFile.exists()) {
                    throw new IOException("The additional CA certificate does not exist at " + additionalCACertFile.getAbsolutePath());
                }
                try (FileReader pemEncodedCACertificate = new FileReader(additionalCACertFile)) {
                    signingCertificate = TlsHelper.parseCertificate(pemEncodedCACertificate);
                }
                signingCertificates.add(signingCertificate);
            }

            // Support self-signed CA certificates
            signingCertificates.add(certificate);

            boolean signatureValid = TlsHelper.verifyCertificateSignature(certificate, signingCertificates);

            if (!signatureValid) {
                throw new SignatureException("The signing certificate was not signed by any known certificates");
            }

            if (!caKeyPair.getPublic().equals(certificate.getPublicKey())) {
                throw new IOException("Expected " + nifiKey + " to correspond to CA certificate at " + nifiCert);
            }

            if (logger.isInfoEnabled()) {
                logger.info("Using existing CA certificate " + nifiCert + " and key " + nifiKey);
            }
        } else if (nifiKey.exists()) {
            throw new IOException(nifiKey + " exists already, but " + nifiCert + " does not, we need both certificate and key to continue with an existing CA.");
        } else {
            TlsCertificateAuthorityManager tlsCertificateAuthorityManager = new TlsCertificateAuthorityManager(standaloneConfig);
            KeyStore.PrivateKeyEntry privateKeyEntry = tlsCertificateAuthorityManager.getOrGenerateCertificateAuthority();
            certificate = (X509Certificate) privateKeyEntry.getCertificateChain()[0];
            caKeyPair = new KeyPair(certificate.getPublicKey(), privateKeyEntry.getPrivateKey());

            try (PemWriter pemWriter = new PemWriter(new OutputStreamWriter(outputStreamFactory.create(nifiCert)))) {
                pemWriter.writeObject(new JcaMiscPEMGenerator(certificate));
            }

            try (PemWriter pemWriter = new PemWriter(new OutputStreamWriter(outputStreamFactory.create(nifiKey)))) {
                pemWriter.writeObject(new JcaMiscPEMGenerator(caKeyPair));
            }

            if (logger.isInfoEnabled()) {
                logger.info("Generated new CA certificate " + nifiCert + " and key " + nifiKey);
            }
        }

        NiFiPropertiesWriterFactory niFiPropertiesWriterFactory = standaloneConfig.getNiFiPropertiesWriterFactory();
        boolean overwrite = standaloneConfig.isOverwrite();

        List<InstanceDefinition> instanceDefinitions = standaloneConfig.getInstanceDefinitions();
        if (instanceDefinitions.isEmpty() && logger.isInfoEnabled()) {
            logger.info("No " + TlsToolkitStandaloneCommandLine.HOSTNAMES_ARG + " specified, not generating any host certificates or configuration.");
        }
        for (InstanceDefinition instanceDefinition : instanceDefinitions) {
            String hostname = instanceDefinition.getHostname();
            File hostDir;
            int hostIdentifierNumber = instanceDefinition.getInstanceIdentifier().getNumber();
            if (hostIdentifierNumber == 1) {
                hostDir = new File(baseDir, hostname);
            } else {
                hostDir = new File(baseDir, hostname + "_" + hostIdentifierNumber);
            }

            TlsClientConfig tlsClientConfig = new TlsClientConfig(standaloneConfig);
            File keystore = new File(hostDir, "keystore." + tlsClientConfig.getKeyStoreType().toLowerCase());
            File truststore = new File(hostDir, "truststore." + tlsClientConfig.getTrustStoreType().toLowerCase());

            if (hostDir.exists()) {
                if (!hostDir.isDirectory()) {
                    throw new IOException(hostDir + " exists but is not a directory.");
                } else if (overwrite) {
                    if (logger.isInfoEnabled()) {
                        logger.info("Overwriting any existing ssl configuration in " + hostDir);
                    }
                    keystore.delete();
                    if (keystore.exists()) {
                        throw new IOException("Keystore " + keystore + " already exists and couldn't be deleted.");
                    }
                    truststore.delete();
                    if (truststore.exists()) {
                        throw new IOException("Truststore " + truststore + " already exists and couldn't be deleted.");
                    }
                } else {
                    throw new IOException(hostDir + " exists and overwrite is not set.");
                }
            } else if (!hostDir.mkdirs()) {
                throw new IOException("Unable to make directory: " + hostDir.getAbsolutePath());
            } else if (logger.isInfoEnabled()) {
                logger.info("Writing new ssl configuration to " + hostDir);
            }

            tlsClientConfig.setKeyStore(keystore.getAbsolutePath());
            tlsClientConfig.setKeyStorePassword(instanceDefinition.getKeyStorePassword());
            tlsClientConfig.setKeyPassword(instanceDefinition.getKeyPassword());
            tlsClientConfig.setTrustStore(truststore.getAbsolutePath());
            tlsClientConfig.setTrustStorePassword(instanceDefinition.getTrustStorePassword());
            TlsClientManager tlsClientManager = new TlsClientManager(tlsClientConfig);
            KeyPair keyPair = TlsHelper.generateKeyPair(keyPairAlgorithm, keySize);
            Extensions sanDnsExtensions = TlsHelper.createDomainAlternativeNamesExtensions(tlsClientConfig.getDomainAlternativeNames(), tlsClientConfig.calcDefaultDn(hostname));
            tlsClientManager.addPrivateKeyToKeyStore(keyPair, NIFI_KEY, CertificateUtils.generateIssuedCertificate(tlsClientConfig.calcDefaultDn(hostname),
                    keyPair.getPublic(), sanDnsExtensions, certificate, caKeyPair, signingAlgorithm, days), certificate);
            tlsClientManager.setCertificateEntry(NIFI_CERT, certificate);
            tlsClientManager.addClientConfigurationWriter(new NifiPropertiesTlsClientConfigWriter(niFiPropertiesWriterFactory, new File(hostDir, "nifi.properties"),
                    hostname, instanceDefinition.getNumber()));
            tlsClientManager.write(outputStreamFactory);
            if (logger.isInfoEnabled()) {
                logger.info("Successfully generated TLS configuration for " + hostname + " " + hostIdentifierNumber + " in " + hostDir);
            }
        }

        List<String> clientDns = standaloneConfig.getClientDns();
        if (standaloneConfig.getClientDns().isEmpty() && logger.isInfoEnabled()) {
            logger.info("No " + TlsToolkitStandaloneCommandLine.CLIENT_CERT_DN_ARG + " specified, not generating any client certificates.");
        }

        List<String> clientPasswords = standaloneConfig.getClientPasswords();
        for (int i = 0; i < clientDns.size(); i++) {
            String reorderedDn = CertificateUtils.reorderDn(clientDns.get(i));
            String clientDnFile = TlsHelper.escapeFilename(reorderedDn);
            File clientCertFile = new File(baseDir, clientDnFile + ".p12");

            if (clientCertFile.exists()) {
                if (overwrite) {
                    if (logger.isInfoEnabled()) {
                        logger.info("Overwriting existing client cert " + clientCertFile);
                    }
                } else {
                    throw new IOException(clientCertFile + " exists and overwrite is not set.");
                }
            } else if (logger.isInfoEnabled()) {
                logger.info("Generating new client certificate " + clientCertFile);
            }
            KeyPair keyPair = TlsHelper.generateKeyPair(keyPairAlgorithm, keySize);
            X509Certificate clientCert = CertificateUtils.generateIssuedCertificate(reorderedDn, keyPair.getPublic(), null, certificate, caKeyPair, signingAlgorithm, days);
            KeyStore keyStore = KeyStoreUtils.getKeyStore(KeystoreType.PKCS12.toString());
            keyStore.load(null, null);
            keyStore.setKeyEntry(NIFI_KEY, keyPair.getPrivate(), null, new Certificate[]{clientCert, certificate});
            String password = TlsHelper.writeKeyStore(keyStore, outputStreamFactory, clientCertFile, clientPasswords.get(i), standaloneConfig.isClientPasswordsGenerated());

            try (FileWriter fileWriter = new FileWriter(new File(baseDir, clientDnFile + ".password"))) {
                fileWriter.write(password);
            }

            if (logger.isInfoEnabled()) {
                logger.info("Successfully generated client certificate " + clientCertFile);
            }
        }

        if (logger.isInfoEnabled()) {
            logger.info("tls-toolkit standalone completed successfully");
        }
    }

}
