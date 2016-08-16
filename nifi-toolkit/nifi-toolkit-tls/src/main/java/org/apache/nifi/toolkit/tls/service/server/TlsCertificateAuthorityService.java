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

package org.apache.nifi.toolkit.tls.service.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.toolkit.tls.configuration.TlsConfig;
import org.apache.nifi.toolkit.tls.manager.TlsCertificateAuthorityManager;
import org.apache.nifi.toolkit.tls.manager.writer.JsonConfigurationWriter;
import org.apache.nifi.toolkit.tls.service.BaseCertificateAuthorityCommandLine;
import org.apache.nifi.toolkit.tls.util.OutputStreamFactory;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;

/**
 * Starts a Jetty server that will either load an existing CA or create one and use it to sign CSRs
 */
public class TlsCertificateAuthorityService {
    private final Logger logger = LoggerFactory.getLogger(TlsCertificateAuthorityService.class);
    private final OutputStreamFactory outputStreamFactory;
    private Server server;

    public TlsCertificateAuthorityService() {
        this(FileOutputStream::new);
    }

    public TlsCertificateAuthorityService(OutputStreamFactory outputStreamFactory) {
        this.outputStreamFactory = outputStreamFactory;
    }

    private static Server createServer(Handler handler, int port, KeyStore keyStore, String keyPassword) throws Exception {
        Server server = new Server();

        SslContextFactory sslContextFactory = new SslContextFactory();
        sslContextFactory.setIncludeProtocols("TLSv1.2");
        sslContextFactory.setKeyStore(keyStore);
        sslContextFactory.setKeyManagerPassword(keyPassword);

        HttpConfiguration httpsConfig = new HttpConfiguration();
        httpsConfig.addCustomizer(new SecureRequestCustomizer());

        ServerConnector sslConnector = new ServerConnector(server, new SslConnectionFactory(sslContextFactory, HttpVersion.HTTP_1_1.asString()), new HttpConnectionFactory(httpsConfig));
        sslConnector.setPort(port);

        server.addConnector(sslConnector);
        server.setHandler(handler);

        return server;
    }

    public synchronized void start(TlsConfig tlsConfig, String configJson, boolean differentPasswordsForKeyAndKeystore) throws Exception {
        if (server != null) {
            throw new IllegalStateException("Server already started");
        }
        ObjectMapper objectMapper = new ObjectMapper();
        TlsCertificateAuthorityManager tlsManager;
        try {
            tlsManager = new TlsCertificateAuthorityManager(tlsConfig);
            tlsManager.setDifferentKeyAndKeyStorePassword(differentPasswordsForKeyAndKeystore);
        } catch (IOException e) {
            logger.error("Unable to open existing keystore, it can be reused by specifiying both " + BaseCertificateAuthorityCommandLine.CONFIG_JSON_ARG + " and " +
                    BaseCertificateAuthorityCommandLine.USE_CONFIG_JSON_ARG);
            throw e;
        }
        tlsManager.addConfigurationWriter(new JsonConfigurationWriter<>(objectMapper, new File(configJson)));

        KeyStore.PrivateKeyEntry privateKeyEntry = tlsManager.getOrGenerateCertificateAuthority();
        KeyPair keyPair = new KeyPair(privateKeyEntry.getCertificate().getPublicKey(), privateKeyEntry.getPrivateKey());
        Certificate[] certificateChain = privateKeyEntry.getCertificateChain();
        if (certificateChain.length != 1) {
            throw new IOException("Expected root ca cert to be only certificate in chain");
        }
        Certificate certificate = certificateChain[0];
        X509Certificate caCert;
        if (certificate instanceof X509Certificate) {
            caCert = (X509Certificate) certificate;
        } else {
            throw new IOException("Expected " + X509Certificate.class + " as root ca cert");
        }
        tlsManager.write(outputStreamFactory);
        String signingAlgorithm = tlsConfig.getSigningAlgorithm();
        int days = tlsConfig.getDays();
        server = createServer(new TlsCertificateAuthorityServiceHandler(signingAlgorithm, days, tlsConfig.getToken(), caCert, keyPair, objectMapper), tlsConfig.getPort(), tlsManager.getKeyStore(),
                tlsConfig.getKeyPassword());
        server.start();
    }

    public synchronized void shutdown() throws Exception {
        if (server == null) {
            throw new IllegalStateException("Server already shutdown");
        }
        server.stop();
        server.join();
    }
}
