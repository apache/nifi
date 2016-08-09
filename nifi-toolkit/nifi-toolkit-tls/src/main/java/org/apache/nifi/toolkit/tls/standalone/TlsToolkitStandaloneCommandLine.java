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

package org.apache.nifi.toolkit.tls.standalone;

import org.apache.commons.cli.CommandLine;
import org.apache.nifi.toolkit.tls.commandLine.BaseCommandLine;
import org.apache.nifi.toolkit.tls.commandLine.CommandLineParseException;
import org.apache.nifi.toolkit.tls.commandLine.ExitCode;
import org.apache.nifi.toolkit.tls.configuration.StandaloneConfig;
import org.apache.nifi.toolkit.tls.properties.NiFiPropertiesWriterFactory;
import org.apache.nifi.toolkit.tls.util.PasswordUtil;
import org.apache.nifi.toolkit.tls.util.TlsHelper;
import org.apache.nifi.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TlsToolkitStandaloneCommandLine extends BaseCommandLine {
    public static final String OUTPUT_DIRECTORY_ARG = "outputDirectory";
    public static final String NIFI_PROPERTIES_FILE_ARG = "nifiPropertiesFile";
    public static final String KEY_STORE_PASSWORD_ARG = "keyStorePassword";
    public static final String TRUST_STORE_PASSWORD_ARG = "trustStorePassword";
    public static final String KEY_PASSWORD_ARG = "keyPassword";
    public static final String HOSTNAMES_ARG = "hostnames";
    public static final String HTTPS_PORT_ARG = "httpsPort";
    public static final String OVERWRITE_ARG = "isOverwrite";
    public static final String CLIENT_CERT_DN_ARG = "clientCertDn";
    public static final String CLIENT_CERT_PASSWORD_ARG = "clientCertPassword";

    public static final String DEFAULT_OUTPUT_DIRECTORY = "../" + Paths.get(".").toAbsolutePath().normalize().getFileName().toString();
    public static final int DEFAULT_HTTPS_PORT = 9091;

    public static final String DESCRIPTION = "Creates certificates and config files for nifi cluster.";

    private final Logger logger = LoggerFactory.getLogger(TlsToolkitStandaloneCommandLine.class);

    private final PasswordUtil passwordUtil;
    private File baseDir;
    private List<String> hostnames;
    private int httpsPort;
    private NiFiPropertiesWriterFactory niFiPropertiesWriterFactory;
    private List<String> keyStorePasswords;
    private List<String> keyPasswords;
    private List<String> trustStorePasswords;
    private List<String> clientDns;
    private List<String> clientPasswords;
    private boolean clientPasswordsGenerated;
    private boolean overwrite;

    public TlsToolkitStandaloneCommandLine() {
        this(new PasswordUtil());
    }

    protected TlsToolkitStandaloneCommandLine(PasswordUtil passwordUtil) {
        super(DESCRIPTION);
        this.passwordUtil = passwordUtil;
        addOptionWithArg("o", OUTPUT_DIRECTORY_ARG, "The directory to output keystores, truststore, config files.", DEFAULT_OUTPUT_DIRECTORY);
        addOptionWithArg("n", HOSTNAMES_ARG, "Comma separated list of hostnames.");
        addOptionWithArg("p", HTTPS_PORT_ARG, "Https port to use.", DEFAULT_HTTPS_PORT);
        addOptionWithArg("f", NIFI_PROPERTIES_FILE_ARG, "Base nifi.properties file to update. (Embedded file identical to the one in a default NiFi install will be used if not specified.)");
        addOptionWithArg("S", KEY_STORE_PASSWORD_ARG, "Keystore password to use.  Must either be one value or one for each host. (autogenerate if not specified)");
        addOptionWithArg("K", KEY_PASSWORD_ARG, "Key password to use.  Must either be one value or one for each host. (autogenerate if not specified)");
        addOptionWithArg("P", TRUST_STORE_PASSWORD_ARG, "Keystore password to use.  Must either be one value or one for each host. (autogenerate if not specified)");
        addOptionWithArg("C", CLIENT_CERT_DN_ARG, "Generate client certificate suitable for use in browser with specified DN. (Can be specified multiple times.)");
        addOptionWithArg("B", CLIENT_CERT_PASSWORD_ARG, "Password for client certificate.  Must either be one value or one for each client DN. (autogenerate if not specified)");
        addOptionNoArg("O", OVERWRITE_ARG, "Overwrite existing host output.");
    }

    public static void main(String[] args) {
        TlsHelper.addBouncyCastleProvider();
        TlsToolkitStandaloneCommandLine tlsToolkitStandaloneCommandLine = new TlsToolkitStandaloneCommandLine();
        try {
            tlsToolkitStandaloneCommandLine.parse(args);
        } catch (CommandLineParseException e) {
            System.exit(e.getExitCode());
        }
        try {
            new TlsToolkitStandalone().createNifiKeystoresAndTrustStores(tlsToolkitStandaloneCommandLine.createConfig());
        } catch (Exception e) {
            tlsToolkitStandaloneCommandLine.printUsage("Error creating generating tls configuration. (" + e.getMessage() + ")");
            System.exit(ExitCode.ERROR_GENERATING_CONFIG.ordinal());
        }
        System.exit(ExitCode.SUCCESS.ordinal());
    }

    @Override
    protected CommandLine doParse(String... args) throws CommandLineParseException {
        CommandLine commandLine = super.doParse(args);
        String outputDirectory = commandLine.getOptionValue(OUTPUT_DIRECTORY_ARG, DEFAULT_OUTPUT_DIRECTORY);
        baseDir = new File(outputDirectory);

        if (commandLine.hasOption(HOSTNAMES_ARG)) {
            hostnames = Collections.unmodifiableList(Arrays.stream(commandLine.getOptionValue(HOSTNAMES_ARG).split(",")).map(String::trim).collect(Collectors.toList()));
        } else {
            hostnames = Collections.emptyList();
        }

        String[] clientDnValues = commandLine.getOptionValues(CLIENT_CERT_DN_ARG);
        if (clientDnValues != null) {
            clientDns = Collections.unmodifiableList(Arrays.stream(clientDnValues).collect(Collectors.toList()));
        } else {
            clientDns = Collections.emptyList();
        }

        httpsPort = getIntValue(commandLine, HTTPS_PORT_ARG, DEFAULT_HTTPS_PORT);

        int numHosts = hostnames.size();
        keyStorePasswords = Collections.unmodifiableList(getPasswords(KEY_STORE_PASSWORD_ARG, commandLine, numHosts, HOSTNAMES_ARG));
        keyPasswords = Collections.unmodifiableList(getKeyPasswords(commandLine, keyStorePasswords));
        trustStorePasswords = Collections.unmodifiableList(getPasswords(TRUST_STORE_PASSWORD_ARG, commandLine, numHosts, HOSTNAMES_ARG));
        clientPasswords = Collections.unmodifiableList(getPasswords(CLIENT_CERT_PASSWORD_ARG, commandLine, clientDns.size(), CLIENT_CERT_DN_ARG));
        clientPasswordsGenerated = commandLine.getOptionValues(CLIENT_CERT_PASSWORD_ARG) == null;
        overwrite = commandLine.hasOption(OVERWRITE_ARG);

        String nifiPropertiesFile = commandLine.getOptionValue(NIFI_PROPERTIES_FILE_ARG, "");
        try {
            if (StringUtils.isEmpty(nifiPropertiesFile)) {
                logger.info("No " + NIFI_PROPERTIES_FILE_ARG + " specified, using embedded one.");
                niFiPropertiesWriterFactory = new NiFiPropertiesWriterFactory();
            } else {
                logger.info("Using " + nifiPropertiesFile + " as template.");
                niFiPropertiesWriterFactory = new NiFiPropertiesWriterFactory(new FileInputStream(nifiPropertiesFile));
            }
        } catch (IOException e) {
            printUsageAndThrow("Unable to read nifi.properties from " + (StringUtils.isEmpty(nifiPropertiesFile) ? "classpath" : nifiPropertiesFile), ExitCode.ERROR_READING_NIFI_PROPERTIES);
        }
        return commandLine;
    }

    private List<String> getPasswords(String arg, CommandLine commandLine, int num, String numArg) throws CommandLineParseException {
        String[] optionValues = commandLine.getOptionValues(arg);
        if (optionValues == null) {
            return IntStream.range(0, num).mapToObj(operand -> passwordUtil.generatePassword()).collect(Collectors.toList());
        }
        if (optionValues.length == 1) {
            return IntStream.range(0, num).mapToObj(value -> optionValues[0]).collect(Collectors.toList());
        } else if (optionValues.length == num) {
            return Arrays.stream(optionValues).collect(Collectors.toList());
        }
        return printUsageAndThrow("Expected either 1 value or " + num + " (the number of " + numArg + ") values for " + arg, ExitCode.ERROR_INCORRECT_NUMBER_OF_PASSWORDS);
    }

    private List<String> getKeyPasswords(CommandLine commandLine, List<String> keyStorePasswords) throws CommandLineParseException {
        if (differentPasswordForKeyAndKeystore() || commandLine.hasOption(KEY_PASSWORD_ARG)) {
            return getPasswords(KEY_PASSWORD_ARG, commandLine, keyStorePasswords.size(), HOSTNAMES_ARG);
        }
        return new ArrayList<>(keyStorePasswords);
    }

    public StandaloneConfig createConfig() {
        StandaloneConfig standaloneConfig = new StandaloneConfig();

        standaloneConfig.setBaseDir(baseDir);
        standaloneConfig.setNiFiPropertiesWriterFactory(niFiPropertiesWriterFactory);
        standaloneConfig.setHostnames(hostnames);
        standaloneConfig.setKeyStorePasswords(keyStorePasswords);
        standaloneConfig.setKeyPasswords(keyPasswords);
        standaloneConfig.setTrustStorePasswords(trustStorePasswords);
        standaloneConfig.setHttpsPort(httpsPort);
        standaloneConfig.setOverwrite(overwrite);
        standaloneConfig.setClientDns(clientDns);
        standaloneConfig.setClientPasswords(clientPasswords);
        standaloneConfig.setClientPasswordsGenerated(clientPasswordsGenerated);

        standaloneConfig.setCaHostname(getCertificateAuthorityHostname());
        standaloneConfig.setKeyStore("nifi-ca-" + KEYSTORE + getKeyStoreType().toLowerCase());
        standaloneConfig.setKeyStoreType(getKeyStoreType());
        standaloneConfig.setKeySize(getKeySize());
        standaloneConfig.setKeyPairAlgorithm(getKeyAlgorithm());
        standaloneConfig.setSigningAlgorithm(getSigningAlgorithm());
        standaloneConfig.setDays(getDays());
        standaloneConfig.initDefaults();

        return standaloneConfig;
    }
}
