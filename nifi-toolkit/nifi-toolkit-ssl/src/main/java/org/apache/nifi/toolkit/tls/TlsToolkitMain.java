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

package org.apache.nifi.toolkit.tls;

import org.apache.nifi.toolkit.tls.commandLine.TlsToolkitCommandLine;
import org.apache.nifi.toolkit.tls.commandLine.CommandLineParseException;
import org.apache.nifi.toolkit.tls.configuration.TlsHostConfigurationBuilder;
import org.apache.nifi.toolkit.tls.properties.NiFiPropertiesWriterFactory;
import org.apache.nifi.toolkit.tls.util.TlsHelper;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.jcajce.JcaMiscPEMGenerator;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.util.io.pem.PemWriter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.Security;
import java.security.cert.X509Certificate;
import java.util.List;

import static org.apache.nifi.toolkit.tls.commandLine.TlsToolkitCommandLine.ERROR_GENERATING_CONFIG;

/**
 * Command line utility for generating a certificate authority and using it to issue certificates for all nifi nodes
 */
public class TlsToolkitMain {
    public static final String NIFI_KEY = "nifi-key";
    public static final String NIFI_CERT = "nifi-cert";
    public static final String ROOT_CERT_PRIVATE_KEY = "rootCert.key";
    public static final String ROOT_CERT_CRT = "rootCert.crt";
    public static final String NIFI_PROPERTIES = "nifi.properties";

    private final TlsHelper tlsHelper;
    private final File baseDir;
    private final NiFiPropertiesWriterFactory niFiPropertiesWriterFactory;

    public TlsToolkitMain(TlsHelper tlsHelper, File baseDir, NiFiPropertiesWriterFactory niFiPropertiesWriterFactory) {
        this.tlsHelper = tlsHelper;
        this.baseDir = baseDir;
        this.niFiPropertiesWriterFactory = niFiPropertiesWriterFactory;
    }

    public static void main(String[] args) {
        Security.addProvider(new BouncyCastleProvider());
        TlsToolkitCommandLine tlsToolkitCommandLine = new TlsToolkitCommandLine(new SecureRandom());
        try {
            tlsToolkitCommandLine.parse(args);
        } catch (CommandLineParseException e) {
            System.exit(e.getExitCode());
        }
        try {
            new TlsToolkitMain(new TlsHelper(tlsToolkitCommandLine), tlsToolkitCommandLine.getBaseDir(), tlsToolkitCommandLine.getNiFiPropertiesWriterFactory())
                    .createNifiKeystoresAndTrustStores("CN=nifi.root.ca,OU=apache.nifi", tlsToolkitCommandLine.getHostnames(), tlsToolkitCommandLine.getKeyStorePasswords(),
                            tlsToolkitCommandLine.getKeyPasswords(), tlsToolkitCommandLine.getTrustStorePasswords(), tlsToolkitCommandLine.getHttpsPort());
        } catch (Exception e) {
            tlsToolkitCommandLine.printUsage("Error creating generating tls configuration. (" + e.getMessage() + ")");
            System.exit(ERROR_GENERATING_CONFIG);
        }
        System.exit(0);
    }

    public void createNifiKeystoresAndTrustStores(String dn, List<String> hostnames, List<String> keyStorePasswords, List<String> keyPasswords,
                                                  List<String> trustStorePasswords, String httpsPort) throws GeneralSecurityException, IOException, OperatorCreationException {
        KeyPair certificateKeypair = tlsHelper.generateKeyPair();
        X509Certificate x509Certificate = tlsHelper.generateSelfSignedX509Certificate(certificateKeypair, dn);

        try (PemWriter pemWriter = new PemWriter(new FileWriter(new File(baseDir, ROOT_CERT_CRT)))) {
            pemWriter.writeObject(new JcaMiscPEMGenerator(x509Certificate));
        }

        try (PemWriter pemWriter = new PemWriter(new FileWriter(new File(baseDir, ROOT_CERT_PRIVATE_KEY)))) {
            pemWriter.writeObject(new JcaMiscPEMGenerator(certificateKeypair));
        }

        KeyStore trustStore = tlsHelper.createKeyStore();
        trustStore.setCertificateEntry(NIFI_CERT, x509Certificate);

        TlsHostConfigurationBuilder tlsHostConfigurationBuilder = new TlsHostConfigurationBuilder(tlsHelper, niFiPropertiesWriterFactory)
                .setHttpsPort(httpsPort)
                .setCertificateKeypair(certificateKeypair)
                .setX509Certificate(x509Certificate)
                .setTrustStore(trustStore);

        for (int i = 0; i < hostnames.size(); i++) {
            String hostname = hostnames.get(i);
            File hostDir = new File(baseDir, hostname);

            if (!hostDir.mkdirs()) {
                throw new IOException("Unable to make directory: " + hostDir.getAbsolutePath());
            }

            tlsHostConfigurationBuilder
                    .setHostDir(hostDir)
                    .setKeyStorePassword(keyStorePasswords.get(i))
                    .setKeyPassword(keyPasswords.get(i))
                    .setTrustStorePassword(trustStorePasswords.get(i))
                    .setHostname(hostname)
                    .createSSLHostConfiguration()
                    .processHost();
        }
    }
}
