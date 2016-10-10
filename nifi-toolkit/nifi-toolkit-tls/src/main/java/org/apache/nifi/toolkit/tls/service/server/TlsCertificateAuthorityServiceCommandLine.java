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
import org.apache.nifi.toolkit.tls.commandLine.CommandLineParseException;
import org.apache.nifi.toolkit.tls.configuration.TlsConfig;
import org.apache.nifi.toolkit.tls.service.BaseCertificateAuthorityCommandLine;
import org.apache.nifi.toolkit.tls.util.InputStreamFactory;
import org.apache.nifi.toolkit.tls.util.TlsHelper;
import org.apache.nifi.util.StringUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Command line parser for a TlsConfig object and a main entry point to invoke the parser and run the CA server
 */
public class TlsCertificateAuthorityServiceCommandLine extends BaseCertificateAuthorityCommandLine {
    public static final String DESCRIPTION = "Acts as a Certificate Authority that can be used by clients to get Certificates";
    public static final String NIFI_CA_KEYSTORE = "nifi-ca-" + KEYSTORE;

    private final InputStreamFactory inputStreamFactory;

    public TlsCertificateAuthorityServiceCommandLine() {
        this(FileInputStream::new);
    }

    public TlsCertificateAuthorityServiceCommandLine(InputStreamFactory inputStreamFactory) {
        super(DESCRIPTION);
        this.inputStreamFactory = inputStreamFactory;
    }

    public static void main(String[] args) throws Exception {
        TlsHelper.addBouncyCastleProvider();
        TlsCertificateAuthorityServiceCommandLine tlsCertificateAuthorityServiceCommandLine = new TlsCertificateAuthorityServiceCommandLine();
        try {
            tlsCertificateAuthorityServiceCommandLine.parse(args);
        } catch (CommandLineParseException e) {
            System.exit(e.getExitCode().ordinal());
        }
        TlsCertificateAuthorityService tlsCertificateAuthorityService = new TlsCertificateAuthorityService();
        tlsCertificateAuthorityService.start(tlsCertificateAuthorityServiceCommandLine.createConfig(), tlsCertificateAuthorityServiceCommandLine.getConfigJson(),
                tlsCertificateAuthorityServiceCommandLine.differentPasswordForKeyAndKeystore());
        System.out.println("Server Started");
        System.out.flush();
    }

    public TlsConfig createConfig() throws IOException {
        if (onlyUseConfigJson()) {
            try (InputStream inputStream = inputStreamFactory.create(new File(getConfigJson()))) {
                TlsConfig tlsConfig = new ObjectMapper().readValue(inputStream, TlsConfig.class);
                tlsConfig.initDefaults();
                return tlsConfig;
            }
        } else {
            TlsConfig tlsConfig = new TlsConfig();
            tlsConfig.setCaHostname(getCertificateAuthorityHostname());
            tlsConfig.setDn(getDn());
            tlsConfig.setToken(getToken());
            tlsConfig.setPort(getPort());
            tlsConfig.setKeyStore(NIFI_CA_KEYSTORE + getKeyStoreType().toLowerCase());
            tlsConfig.setKeyStoreType(getKeyStoreType());
            tlsConfig.setKeySize(getKeySize());
            tlsConfig.setKeyPairAlgorithm(getKeyAlgorithm());
            tlsConfig.setSigningAlgorithm(getSigningAlgorithm());
            tlsConfig.setDays(getDays());
            return tlsConfig;
        }
    }

    @Override
    protected String getTokenDescription() {
        return "The token to use to prevent MITM (required and must be same as one used by clients)";
    }

    @Override
    protected String getDnDescription()  {
        return "The dn to use for the CA certificate";
    }

    @Override
    protected String getPortDescription() {
        return "The port for the Certificate Authority to listen on";
    }

    @Override
    protected String getDnHostname() {
        String dnHostname = getCertificateAuthorityHostname();
        if (StringUtils.isEmpty(dnHostname)) {
            return "YOUR_CA_HOSTNAME";
        }
        return dnHostname;
    }
}
