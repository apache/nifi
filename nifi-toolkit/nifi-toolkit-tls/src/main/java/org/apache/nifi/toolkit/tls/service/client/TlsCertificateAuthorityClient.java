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
import org.apache.nifi.toolkit.tls.standalone.TlsToolkitStandalone;
import org.apache.nifi.toolkit.tls.util.OutputStreamFactory;
import org.apache.nifi.toolkit.tls.util.TlsHelper;
import org.apache.nifi.util.StringUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.security.KeyPair;
import java.security.cert.X509Certificate;

/**
 * Client that will generate a CSR and submit to a CA, writing out the results to a keystore and truststore along with a config file if successful
 */
public class TlsCertificateAuthorityClient {
    private final OutputStreamFactory outputStreamFactory;


    public TlsCertificateAuthorityClient() {
        this(FileOutputStream::new);
    }

    public TlsCertificateAuthorityClient(OutputStreamFactory outputStreamFactory) {
        this.outputStreamFactory = outputStreamFactory;
    }

    public void generateCertificateAndGetItSigned(TlsClientConfig tlsClientConfig, String certificateDirectory, String configJson) throws Exception {
        TlsClientManager tlsClientManager = new TlsClientManager(tlsClientConfig);

        if (!StringUtils.isEmpty(certificateDirectory)) {
            tlsClientManager.setCertificateAuthorityDirectory(new File(certificateDirectory));
        }

        if (!StringUtils.isEmpty(configJson)) {
            tlsClientManager.addClientConfigurationWriter(new JsonConfigurationWriter<>(new ObjectMapper(), outputStreamFactory, new File(configJson)));
        }

        if (tlsClientManager.getEntry(TlsToolkitStandalone.NIFI_KEY) == null) {
            TlsHelper tlsHelper = tlsClientConfig.createTlsHelper();
            KeyPair keyPair = tlsHelper.generateKeyPair();

            X509Certificate[] certificates = tlsClientConfig.createCertificateSigningRequestPerformer().perform(keyPair);

            tlsClientManager.addPrivateKeyToKeyStore(keyPair, TlsToolkitStandalone.NIFI_KEY, certificates);
            tlsClientManager.setCertificateEntry(TlsToolkitStandalone.NIFI_CERT, certificates[certificates.length - 1]);
        }

        tlsClientManager.write(outputStreamFactory);
    }
}
