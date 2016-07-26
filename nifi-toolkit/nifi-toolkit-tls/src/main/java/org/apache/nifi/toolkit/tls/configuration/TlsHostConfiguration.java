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

package org.apache.nifi.toolkit.tls.configuration;

import org.apache.nifi.toolkit.tls.properties.NiFiPropertiesWriter;
import org.apache.nifi.toolkit.tls.properties.NiFiPropertiesWriterFactory;
import org.apache.nifi.toolkit.tls.util.OutputStreamFactory;
import org.apache.nifi.toolkit.tls.util.TlsHelper;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.StringUtils;
import org.bouncycastle.operator.OperatorCreationException;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.cert.X509Certificate;

public class TlsHostConfiguration {
    public static final String NIFI_KEY = "nifi-key";
    public static final String NIFI_PROPERTIES = "nifi.properties";
    public static final String TRUSTSTORE = "truststore";

    private final OutputStreamFactory outputStreamFactory;
    private final TlsHelper tlsHelper;
    private final NiFiPropertiesWriterFactory niFiPropertiesWriterFactory;
    private final File hostDir;
    private final String httpsPort;
    private final String extension;
    private final KeyPair certificateKeypair;
    private final X509Certificate x509Certificate;
    private final String keyStorePassword;
    private final String keyStoreType;
    private final String keyPassword;
    private final String trustStorePassword;
    private final KeyStore trustStore;
    private final String hostname;

    public TlsHostConfiguration(OutputStreamFactory outputStreamFactory, TlsHelper tlsHelper, NiFiPropertiesWriterFactory niFiPropertiesWriterFactory, File hostDir,
                                String httpsPort, String extension, KeyPair certificateKeypair, X509Certificate x509Certificate,
                                String keyStorePassword, String keyPassword, String keyStoreType, String trustStorePassword, KeyStore trustStore, String hostname) {
        this.outputStreamFactory = outputStreamFactory;
        this.tlsHelper = tlsHelper;
        this.niFiPropertiesWriterFactory = niFiPropertiesWriterFactory;
        this.hostDir = hostDir;
        this.httpsPort = httpsPort;
        this.extension = extension;
        this.certificateKeypair = certificateKeypair;
        this.x509Certificate = x509Certificate;
        this.keyStorePassword = keyStorePassword;
        this.keyStoreType = keyStoreType;
        this.keyPassword = keyPassword;
        this.trustStorePassword = trustStorePassword;
        this.trustStore = trustStore;
        this.hostname = hostname;
    }

    public void processHost() throws IOException, GeneralSecurityException, OperatorCreationException {
        KeyPair keyPair = tlsHelper.generateKeyPair();

        KeyStore keyStore = tlsHelper.createKeyStore(keyStoreType);
        tlsHelper.addToKeyStore(keyStore, keyPair, NIFI_KEY, keyPassword.toCharArray(),
                tlsHelper.generateIssuedCertificate("CN=" + hostname + ",OU=apache.nifi", keyPair.getPublic(), x509Certificate, certificateKeypair), x509Certificate);

        String keyStoreName = hostname + extension;
        String trustStoreName = TRUSTSTORE + extension;

        NiFiPropertiesWriter niFiPropertiesWriter = niFiPropertiesWriterFactory.create();

        niFiPropertiesWriter.setPropertyValue(NiFiProperties.SECURITY_KEYSTORE, "./conf/" + keyStoreName);
        niFiPropertiesWriter.setPropertyValue(NiFiProperties.SECURITY_KEYSTORE_TYPE, keyStoreType);
        niFiPropertiesWriter.setPropertyValue(NiFiProperties.SECURITY_KEYSTORE_PASSWD, keyStorePassword);
        niFiPropertiesWriter.setPropertyValue(NiFiProperties.SECURITY_KEY_PASSWD, keyPassword);
        niFiPropertiesWriter.setPropertyValue(NiFiProperties.SECURITY_TRUSTSTORE, "./conf/truststore" + extension);
        niFiPropertiesWriter.setPropertyValue(NiFiProperties.SECURITY_TRUSTSTORE_TYPE, trustStore.getType());
        niFiPropertiesWriter.setPropertyValue(NiFiProperties.SECURITY_TRUSTSTORE_PASSWD, trustStorePassword);
        if (!StringUtils.isEmpty(httpsPort)) {
            niFiPropertiesWriter.setPropertyValue(NiFiProperties.WEB_HTTPS_PORT, httpsPort);
            niFiPropertiesWriter.setPropertyValue(NiFiProperties.WEB_HTTP_PORT, "");
            niFiPropertiesWriter.setPropertyValue(NiFiProperties.SITE_TO_SITE_SECURE, "true");
        }

        File propertiesFile = new File(hostDir, NIFI_PROPERTIES);
        try (OutputStream outputStream = outputStreamFactory.create(propertiesFile)) {
            niFiPropertiesWriter.writeNiFiProperties(outputStream);
        }

        File keyStoreFile = new File(hostDir, keyStoreName);
        try (OutputStream fileOutputStream = outputStreamFactory.create(keyStoreFile)) {
            keyStore.store(fileOutputStream, keyStorePassword.toCharArray());
        }

        File trustStoreFile = new File(hostDir, trustStoreName);
        try (OutputStream fileOutputStream = outputStreamFactory.create(trustStoreFile)) {
            trustStore.store(fileOutputStream, trustStorePassword.toCharArray());
        }
    }
}
