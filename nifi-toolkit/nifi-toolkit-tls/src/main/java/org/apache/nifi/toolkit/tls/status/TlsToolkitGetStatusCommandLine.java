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
package org.apache.nifi.toolkit.tls.status;

import java.net.URI;
import java.net.URISyntaxException;
import javax.net.ssl.SSLContext;
import org.apache.commons.cli.CommandLine;
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.security.util.StandardTlsConfiguration;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.toolkit.tls.commandLine.BaseCommandLine;
import org.apache.nifi.toolkit.tls.commandLine.CommandLineParseException;
import org.apache.nifi.toolkit.tls.commandLine.ExitCode;
import org.apache.nifi.toolkit.tls.configuration.GetStatusConfig;
import org.apache.nifi.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TlsToolkitGetStatusCommandLine extends BaseCommandLine {

    private final Logger logger = LoggerFactory.getLogger(TlsToolkitGetStatusCommandLine.class);

    public static final String URL_ARG = "url";
    public static final String KEYSTORE_ARG = "keyStore";
    public static final String KEYSTORE_TYPE_ARG = "keyStoreType";
    public static final String KEYSTORE_PASSWORD_ARG = "keyStorePassword";
    public static final String KEY_PASSWORD_ARG = "keyPassword";
    public static final String TRUSTSTORE_ARG = "trustStore";
    public static final String TRUSTSTORE_TYPE_ARG = "trustStoreType";
    public static final String TRUSTSTORE_PASSWORD_ARG = "trustStorePassword";
    public static final String PROTOCOL_ARG = "protocol";

    public static final String DEFAULT_PROTOCOL = TlsConfiguration.getHighestCurrentSupportedTlsProtocolVersion();
    public static final String DEFAULT_KEYSTORE_TYPE = "JKS";

    public static final String DESCRIPTION = "Checks the status of an HTTPS endpoint by making a GET request using a supplied keystore and truststore.";


    private URI url;
    private SSLContext sslContext;

    public TlsToolkitGetStatusCommandLine() {
        super(DESCRIPTION);
        addOptionWithArg("u", URL_ARG, "The full url to connect to, for example: https://localhost:9443/v1/api");
        addOptionWithArg("ks", KEYSTORE_ARG, "The key store to use");
        addOptionWithArg("kst", KEYSTORE_TYPE_ARG, "The type of key store being used (PKCS12 or JKS)", DEFAULT_KEYSTORE_TYPE);
        addOptionWithArg("ksp", KEYSTORE_PASSWORD_ARG, "The password of the key store being used");
        addOptionWithArg("kp", KEY_PASSWORD_ARG, "The key password of the key store being used");
        addOptionWithArg("ts", TRUSTSTORE_ARG, "The trust store being used");
        addOptionWithArg("tst", TRUSTSTORE_TYPE_ARG, "The type of trust store being used (PKCS12 or JKS)", DEFAULT_KEYSTORE_TYPE);
        addOptionWithArg("tsp", TRUSTSTORE_PASSWORD_ARG, "The password of the trust store being used");
        addOptionWithArg("p", PROTOCOL_ARG, "The protocol to use", DEFAULT_PROTOCOL);
    }

    public static void main(String[] args) {
        TlsToolkitGetStatusCommandLine commandLine = new TlsToolkitGetStatusCommandLine();
        try {
            commandLine.parse(args);
        } catch (CommandLineParseException e) {
            System.exit(e.getExitCode().ordinal());
        }

        final GetStatusConfig config = commandLine.createConfig();
        try {
            final TlsToolkitGetStatus tlsToolkitGetStatus = new TlsToolkitGetStatus();
            tlsToolkitGetStatus.get(config);
        } catch (Exception e) {
            commandLine.printUsage("Error communicating with " + config.getUrl().toString()  + " (" + e.getMessage() + ")");
            System.exit(ExitCode.SERVICE_ERROR.ordinal());
        }
        System.exit(ExitCode.SUCCESS.ordinal());
    }


    @Override
    protected void postParse(CommandLine commandLine) throws CommandLineParseException {
        super.postParse(commandLine);

        final String urlValue = commandLine.getOptionValue(URL_ARG);
        if (StringUtils.isBlank(urlValue)) {
            printUsageAndThrow("Url was missing or blank", ExitCode.INVALID_ARGS);
        }

        try {
            this.url = new URI(urlValue);
        } catch (URISyntaxException e) {
            printUsageAndThrow("Invalid Url", ExitCode.INVALID_ARGS);
        }

        // TODO: Refactor this whole thing
        final String keystoreFilename = commandLine.getOptionValue(KEYSTORE_ARG);
        String keystoreTypeStr = commandLine.getOptionValue(KEYSTORE_TYPE_ARG, DEFAULT_KEYSTORE_TYPE);
        final String keystorePassword = commandLine.getOptionValue(KEYSTORE_PASSWORD_ARG);
        final String keyPassword = commandLine.getOptionValue(KEY_PASSWORD_ARG);

        final String truststoreFilename = commandLine.getOptionValue(TRUSTSTORE_ARG);
        final String truststoreTypeStr = commandLine.getOptionValue(TRUSTSTORE_TYPE_ARG, DEFAULT_KEYSTORE_TYPE);
        final String truststorePassword = commandLine.getOptionValue(TRUSTSTORE_PASSWORD_ARG);

        final String protocol = commandLine.getOptionValue(PROTOCOL_ARG, DEFAULT_PROTOCOL);

        // This use case specifically allows truststore configuration without keystore configuration, but attempts to default the keystore type value
        if (StringUtils.isBlank(keystoreFilename)) {
            keystoreTypeStr = null;
        }

        try {
            TlsConfiguration tlsConfiguration = new StandardTlsConfiguration(keystoreFilename, keystorePassword, keyPassword, keystoreTypeStr,
                    truststoreFilename, truststorePassword, truststoreTypeStr, protocol);

            if (tlsConfiguration.isAnyTruststorePopulated()) {
                this.sslContext = SslContextFactory.createSslContext(tlsConfiguration);
            } else {
                printUsageAndThrow("No truststore was provided", ExitCode.INVALID_ARGS);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            printUsageAndThrow("Failed to create SSL Context: " + e.getMessage(), ExitCode.INVALID_ARGS);
        }
    }

    public GetStatusConfig createConfig() {
        return new GetStatusConfig(url, sslContext);
    }

}
