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

package org.apache.nifi.toolkit.tls.service.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.toolkit.tls.configuration.TlsClientConfig;
import org.apache.nifi.toolkit.tls.manager.TlsClientManager;
import org.apache.nifi.toolkit.tls.manager.writer.JsonConfigurationWriter;
import org.apache.nifi.toolkit.tls.service.BaseCertificateAuthorityCommandLine;
import org.apache.nifi.toolkit.tls.standalone.TlsToolkitStandalone;
import org.apache.nifi.toolkit.tls.util.OutputStreamFactory;
import org.apache.nifi.toolkit.tls.util.TlsHelper;
import org.apache.nifi.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.KeyPair;
import java.security.cert.X509Certificate;

/**
 * Client that will generate a CSR and submit to a CA, writing out the results to a keystore and truststore along with a config file if successful
 */
public class TlsCertificateAuthorityClient {
    private final Logger logger = LoggerFactory.getLogger(TlsCertificateAuthorityClient.class);
    private final OutputStreamFactory outputStreamFactory;

    public TlsCertificateAuthorityClient() {
        this(FileOutputStream::new);
    }

    public TlsCertificateAuthorityClient(OutputStreamFactory outputStreamFactory) {
        this.outputStreamFactory = outputStreamFactory;
    }

    public void generateCertificateAndGetItSigned(TlsClientConfig tlsClientConfig, String certificateDirectory, String configJson, boolean differentKeyAndKeyStorePassword) throws Exception {
        TlsClientManager tlsClientManager;
        try {
            tlsClientManager = new TlsClientManager(tlsClientConfig);
        } catch (IOException e) {
            logger.error("Unable to open existing keystore, it can be reused by specifiying both " + BaseCertificateAuthorityCommandLine.CONFIG_JSON_ARG + " and " +
                    BaseCertificateAuthorityCommandLine.USE_CONFIG_JSON_ARG);
            throw e;
        }
        tlsClientManager.setDifferentKeyAndKeyStorePassword(differentKeyAndKeyStorePassword);

        if (!StringUtils.isEmpty(certificateDirectory)) {
            tlsClientManager.setCertificateAuthorityDirectory(new File(certificateDirectory));
        }

        if (!StringUtils.isEmpty(configJson)) {
            tlsClientManager.addClientConfigurationWriter(new JsonConfigurationWriter<>(new ObjectMapper(), new File(configJson)));
        }

        if (tlsClientManager.getEntry(TlsToolkitStandalone.NIFI_KEY) == null) {
            if (logger.isInfoEnabled()) {
                logger.info("Requesting new certificate from " + tlsClientConfig.getCaHostname() + ":" + tlsClientConfig.getPort());
            }
            KeyPair keyPair = TlsHelper.generateKeyPair(tlsClientConfig.getKeyPairAlgorithm(), tlsClientConfig.getKeySize());

            X509Certificate[] certificates = tlsClientConfig.createCertificateSigningRequestPerformer().perform(keyPair);

            tlsClientManager.addPrivateKeyToKeyStore(keyPair, TlsToolkitStandalone.NIFI_KEY, certificates);
            tlsClientManager.setCertificateEntry(TlsToolkitStandalone.NIFI_CERT, certificates[certificates.length - 1]);
        } else {
            if (logger.isInfoEnabled()) {
                logger.info("Already had entry for " + TlsToolkitStandalone.NIFI_KEY + " not requesting new certificate.");
            }
        }

        tlsClientManager.write(outputStreamFactory);
    }
}
