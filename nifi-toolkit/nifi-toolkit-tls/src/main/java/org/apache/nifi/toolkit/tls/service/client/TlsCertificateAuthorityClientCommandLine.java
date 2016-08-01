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
import org.apache.commons.cli.CommandLine;
import org.apache.nifi.toolkit.tls.commandLine.CommandLineParseException;
import org.apache.nifi.toolkit.tls.commandLine.ExitCode;
import org.apache.nifi.toolkit.tls.configuration.TlsClientConfig;
import org.apache.nifi.toolkit.tls.service.BaseCertificateAuthorityCommandLine;
import org.apache.nifi.toolkit.tls.util.InputStreamFactory;
import org.apache.nifi.toolkit.tls.util.TlsHelper;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class TlsCertificateAuthorityClientCommandLine extends BaseCertificateAuthorityCommandLine {
    public static final String DESCRIPTION = "Generates a private key and gets it signed by the certificate authority.";
    public static final String PKCS_12 = "PKCS12";
    public static final String CERTIFICATE_DIRECTORY = "certificateDirectory";
    public static final String DEFAULT_CERTIFICATE_DIRECTORY = ".";
    public static final String SAME_KEY_AND_KEY_STORE_PASSWORD_ARG = "sameKeyAndKeyStorePassword";

    private final InputStreamFactory inputStreamFactory;

    private String certificateDirectory;
    private boolean sameKeyAndKeyStorePassword;

    public TlsCertificateAuthorityClientCommandLine() {
        this(FileInputStream::new);
    }

    public TlsCertificateAuthorityClientCommandLine(InputStreamFactory inputStreamFactory) {
        super(DESCRIPTION);
        this.inputStreamFactory = inputStreamFactory;
        addOptionWithArg("C", CERTIFICATE_DIRECTORY, "The file to write the CA certificate to", DEFAULT_CERTIFICATE_DIRECTORY);
        addOptionNoArg("S", SAME_KEY_AND_KEY_STORE_PASSWORD_ARG, "When generating passwords, use the same one for KeyStore and Key");
    }

    public static void main(String[] args) throws Exception {
        TlsHelper.addBouncyCastleProvider();
        TlsCertificateAuthorityClientCommandLine tlsCertificateAuthorityClientCommandLine = new TlsCertificateAuthorityClientCommandLine();
        try {
            tlsCertificateAuthorityClientCommandLine.parse(args);
        } catch (CommandLineParseException e) {
            System.exit(e.getExitCode());
        }
        new TlsCertificateAuthorityClient().generateCertificateAndGetItSigned(tlsCertificateAuthorityClientCommandLine.createClientConfig(),
                tlsCertificateAuthorityClientCommandLine.getCertificateDirectory(), tlsCertificateAuthorityClientCommandLine.getConfigJson(),
                tlsCertificateAuthorityClientCommandLine.sameKeyAndKeyStorePassword());
        System.exit(ExitCode.SUCCESS.ordinal());
    }

    @Override
    protected boolean shouldAddDaysArg() {
        return false;
    }

    @Override
    protected boolean shouldAddSigningAlgorithmArg() {
        return false;
    }

    @Override
    protected String getKeyStoreTypeDefault() {
        return PKCS_12;
    }

    @Override
    protected CommandLine doParse(String[] args) throws CommandLineParseException {
        CommandLine commandLine = super.doParse(args);
        certificateDirectory = commandLine.getOptionValue(CERTIFICATE_DIRECTORY, DEFAULT_CERTIFICATE_DIRECTORY);
        sameKeyAndKeyStorePassword = commandLine.hasOption(SAME_KEY_AND_KEY_STORE_PASSWORD_ARG);
        return commandLine;
    }

    public boolean sameKeyAndKeyStorePassword() {
        return sameKeyAndKeyStorePassword;
    }

    public String getCertificateDirectory() {
        return certificateDirectory;
    }

    public TlsClientConfig createClientConfig() throws IOException {
        if (onlyUseConfigJson()) {
            try (InputStream inputStream = inputStreamFactory.create(new File(getConfigJson()))) {
                TlsClientConfig tlsClientConfig = new ObjectMapper().readValue(inputStream, TlsClientConfig.class);
                tlsClientConfig.initDefaults();
                return tlsClientConfig;
            }
        } else {
            TlsClientConfig tlsClientConfig = new TlsClientConfig();
            tlsClientConfig.setCaHostname(getCertificateAuthorityHostname());
            tlsClientConfig.setDn(getDn());
            tlsClientConfig.setToken(getToken());
            tlsClientConfig.setPort(getPort());
            tlsClientConfig.setKeyStore(KEYSTORE + getKeyStoreType().toLowerCase());
            tlsClientConfig.setKeyStoreType(getKeyStoreType());
            tlsClientConfig.setTrustStore(TRUSTSTORE + getKeyStoreType().toLowerCase());
            tlsClientConfig.setTrustStoreType(getKeyStoreType());
            tlsClientConfig.setKeySize(getKeySize());
            tlsClientConfig.setKeyPairAlgorithm(getKeyAlgorithm());
            tlsClientConfig.setSigningAlgorithm(getSigningAlgorithm());
            return tlsClientConfig;
        }
    }
}
