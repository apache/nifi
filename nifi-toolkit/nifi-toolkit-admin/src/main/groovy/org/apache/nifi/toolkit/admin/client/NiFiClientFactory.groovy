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
package org.apache.nifi.toolkit.admin.client

import org.apache.commons.lang3.StringUtils
import org.apache.http.conn.ssl.DefaultHostnameVerifier
import org.apache.nifi.util.NiFiProperties
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.net.ssl.KeyManagerFactory
import javax.net.ssl.SSLContext
import javax.net.ssl.TrustManagerFactory
import javax.ws.rs.client.Client
import javax.ws.rs.client.ClientBuilder
import java.security.KeyManagementException
import java.security.KeyStore
import java.security.KeyStoreException
import java.security.NoSuchAlgorithmException
import java.security.SecureRandom
import java.security.UnrecoverableKeyException
import java.security.cert.CertificateException

class NiFiClientFactory implements ClientFactory{

    private static final Logger logger = LoggerFactory.getLogger(NiFiClientFactory.class)
    static enum NiFiAuthType{ NONE, SSL }

    public Client getClient(NiFiProperties niFiProperties, String nifiInstallDir) throws Exception {

        final String authTypeStr = StringUtils.isEmpty(niFiProperties.getProperty(NiFiProperties.WEB_HTTPS_HOST)) &&  StringUtils.isEmpty(niFiProperties.getProperty(NiFiProperties.WEB_HTTPS_PORT))  ? NiFiAuthType.NONE : NiFiAuthType.SSL;
        final NiFiAuthType authType = NiFiAuthType.valueOf(authTypeStr);

        SSLContext sslContext = null;

        if (NiFiAuthType.SSL.equals(authType)) {
            String keystore = niFiProperties.getProperty(NiFiProperties.SECURITY_KEYSTORE);
            final String keystoreType = niFiProperties.getProperty(NiFiProperties.SECURITY_KEYSTORE_TYPE);
            final String keystorePassword = niFiProperties.getProperty(NiFiProperties.SECURITY_KEYSTORE_PASSWD);
            String truststore = niFiProperties.getProperty(NiFiProperties.SECURITY_TRUSTSTORE);
            final String truststoreType = niFiProperties.getProperty(NiFiProperties.SECURITY_TRUSTSTORE_TYPE);
            final String truststorePassword = niFiProperties.getProperty(NiFiProperties.SECURITY_TRUSTSTORE_PASSWD);

            if(keystore.startsWith("./")){
                keystore = keystore.replace("./",nifiInstallDir+"/")
            }
            if(truststore.startsWith("./")){
                truststore = truststore.replace("./",nifiInstallDir+"/")
            }

            sslContext = createSslContext(
                    keystore.trim(),
                    keystorePassword.trim().toCharArray(),
                    keystoreType.trim(),
                    truststore.trim(),
                    truststorePassword.trim().toCharArray(),
                    truststoreType.trim(),
                    "TLS");
        }

        final ClientBuilder clientBuilder = ClientBuilder.newBuilder();

        if (sslContext != null) {
            clientBuilder.sslContext(sslContext).hostnameVerifier(new DefaultHostnameVerifier());
        }

        return clientBuilder.build();

    }

    static SSLContext createSslContext(
            final String keystore, final char[] keystorePasswd, final String keystoreType,
            final String truststore, final char[] truststorePasswd, final String truststoreType,
            final String protocol)
            throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException,
                    UnrecoverableKeyException, KeyManagementException {

        // prepare the keystore
        final KeyStore keyStore = KeyStore.getInstance(keystoreType);
        final InputStream keyStoreStream = new FileInputStream(keystore)
            keyStore.load(keyStoreStream, keystorePasswd);


        final KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(keyStore, keystorePasswd);

        // prepare the truststore
        final KeyStore trustStore = KeyStore.getInstance(truststoreType);
        final InputStream trustStoreStream = new FileInputStream(truststore)
        trustStore.load(trustStoreStream, truststorePasswd);

        final TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(trustStore);

        // initialize the ssl context
        final SSLContext sslContext = SSLContext.getInstance(protocol);
        sslContext.init(keyManagerFactory.getKeyManagers(), trustManagerFactory.getTrustManagers(), new SecureRandom());
        return sslContext;
    }



}
