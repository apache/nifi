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

package org.apache.nifi.toolkit.tls.service;

import org.apache.commons.cli.CommandLine;
import org.apache.nifi.toolkit.tls.commandLine.BaseCommandLine;
import org.apache.nifi.toolkit.tls.commandLine.CommandLineParseException;
import org.apache.nifi.toolkit.tls.configuration.TlsClientConfig;
import org.apache.nifi.toolkit.tls.configuration.TlsConfig;
import org.apache.nifi.toolkit.tls.configuration.TlsHelperConfig;
import org.apache.nifi.toolkit.tls.util.TlsHelper;
import org.apache.nifi.util.StringUtils;

import java.io.File;
import java.io.FileOutputStream;

public class TlsCertificateAuthorityClientCommandLine extends BaseCommandLine {
    public static final String HEADER = new StringBuilder(System.lineSeparator()).append("Generates a private key and gets it signed by the certificate authority.")
            .append(System.lineSeparator()).append(System.lineSeparator()).toString();
    public static final String CERTIFICATE_AUTHORITY_HOSTNAME = "certificateAuthorityHostname";
    public static final String DN = "dn";
    public static final String TOKEN = "token";
    public static final String PORT = "PORT";
    public static final String KEYSTORE_TYPE = "keystoreType";
    public static final String PKCS_12 = "PKCS12";
    public static final File DEFAULT_CONFIG_JSON = new File("config.json");
    public static final String CONFIG_JSON = "configJson";
    public static final int ERROR_TOKEN_ARG_EMPTY = 4;
    public static final String KEYSTORE = "keystore.";
    public static final String TRUSTSTORE = "truststore.";

    private String caHostname;
    private String dn;
    private String token;
    private int port;
    private String keystoreType;
    private File configFile;

    public TlsCertificateAuthorityClientCommandLine() {
        super(HEADER);
        addOptionWithArg("c", CERTIFICATE_AUTHORITY_HOSTNAME, "Hostname of NiFi Certificate Authority", TlsConfig.DEFAULT_HOSTNAME);
        addOptionWithArg("d", DN, "The dn to generate the CSR for", TlsCertificateSigningRequestPerformer.getDn(TlsConfig.DEFAULT_HOSTNAME));
        addOptionWithArg("t", TOKEN, "The token to use to prevent MITM (required and must be same as one used by CA)");
        addOptionWithArg("p", PORT, "The port to use to communicate with the Certificate Authority", TlsConfig.DEFAULT_PORT);
        addOptionWithArg("T", KEYSTORE_TYPE, "The format to write the keystore in", PKCS_12);
        addOptionWithArg("f", CONFIG_JSON, "The place to write configuration info", DEFAULT_CONFIG_JSON);
    }

    public static void main(String[] args) throws Exception {
        TlsHelper.addBouncyCastleProvider();
        TlsCertificateAuthorityClientCommandLine tlsCertificateAuthorityClientCommandLine = new TlsCertificateAuthorityClientCommandLine();
        try {
            tlsCertificateAuthorityClientCommandLine.parse(args);
        } catch (CommandLineParseException e) {
            System.exit(e.getExitCode());
        }
        new TlsCertificateAuthorityClient(tlsCertificateAuthorityClientCommandLine.configFile, FileOutputStream::new,
                tlsCertificateAuthorityClientCommandLine.createClientConfig()).generateCertificateAndGetItSigned();
    }

    @Override
    protected CommandLine doParse(String[] args) throws CommandLineParseException {
        CommandLine commandLine = super.doParse(args);
        caHostname = commandLine.getOptionValue(CERTIFICATE_AUTHORITY_HOSTNAME, TlsConfig.DEFAULT_HOSTNAME);
        dn = commandLine.getOptionValue(DN, TlsCertificateSigningRequestPerformer.getDn(TlsConfig.DEFAULT_HOSTNAME));
        token = commandLine.getOptionValue(TOKEN);
        if (StringUtils.isEmpty(token)) {
            printUsageAndThrow(TOKEN + " argument must not be empty", ERROR_TOKEN_ARG_EMPTY);
        }
        port = getIntValue(commandLine, PORT, TlsConfig.DEFAULT_PORT);
        keystoreType = commandLine.getOptionValue(KEYSTORE_TYPE, PKCS_12);
        if (commandLine.hasOption(CONFIG_JSON)) {
            configFile = new File(commandLine.getOptionValue(CONFIG_JSON));
        } else {
            configFile = DEFAULT_CONFIG_JSON;
        }
        return commandLine;
    }

    public File getConfigFile() {
        return configFile;
    }

    public TlsClientConfig createClientConfig() {
        TlsClientConfig tlsClientConfig = new TlsClientConfig();
        tlsClientConfig.setCaHostname(caHostname);
        tlsClientConfig.setDn(dn);
        tlsClientConfig.setToken(token);
        tlsClientConfig.setPort(port);
        tlsClientConfig.setKeyStore(KEYSTORE + keystoreType.toLowerCase());
        tlsClientConfig.setKeyStoreType(keystoreType);
        tlsClientConfig.setTrustStore(TRUSTSTORE + keystoreType.toLowerCase());
        tlsClientConfig.setTrustStoreType(keystoreType);
        TlsHelperConfig tlsHelperConfig = new TlsHelperConfig();
        tlsHelperConfig.setKeySize(getKeySize());
        tlsHelperConfig.setKeyPairAlgorithm(getKeyAlgorithm());
        tlsClientConfig.setTlsHelperConfig(tlsHelperConfig);
        return tlsClientConfig;
    }
}
