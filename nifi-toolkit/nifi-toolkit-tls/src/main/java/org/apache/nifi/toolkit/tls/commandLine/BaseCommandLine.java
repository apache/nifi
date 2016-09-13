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

package org.apache.nifi.toolkit.tls.commandLine;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.nifi.toolkit.tls.TlsToolkitMain;
import org.apache.nifi.toolkit.tls.configuration.TlsConfig;

/**
 * Base class with common CLI parsing functionality as well as arguments shared by multiple entry points
 */
public abstract class BaseCommandLine {
    public static final String HELP_ARG = "help";
    public static final String JAVA_HOME = "JAVA_HOME";
    public static final String NIFI_TOOLKIT_HOME = "NIFI_TOOLKIT_HOME";
    public static final String FOOTER = new StringBuilder(System.lineSeparator()).append("Java home: ")
            .append(System.getenv(JAVA_HOME)).append(System.lineSeparator()).append("NiFi Toolkit home: ").append(System.getenv(NIFI_TOOLKIT_HOME)).toString();

    public static final String KEY_SIZE_ARG = "keySize";
    public static final String KEY_ALGORITHM_ARG = "keyAlgorithm";
    public static final String CERTIFICATE_AUTHORITY_HOSTNAME_ARG = "certificateAuthorityHostname";
    public static final String DAYS_ARG = "days";
    public static final String KEY_STORE_TYPE_ARG = "keyStoreType";
    public static final String SIGNING_ALGORITHM_ARG = "signingAlgorithm";
    public static final String DN_ARG = "dn";
    public static final String DIFFERENT_KEY_AND_KEYSTORE_PASSWORDS_ARG = "differentKeyAndKeystorePasswords";

    public static final String KEYSTORE = "keystore.";
    public static final String TRUSTSTORE = "truststore.";

    private final Options options;
    private final String header;
    private int keySize;
    private String keyAlgorithm;
    private String certificateAuthorityHostname;
    private String keyStoreType;
    private int days;
    private String signingAlgorithm;
    private boolean differentPasswordForKeyAndKeystore;

    public BaseCommandLine(String header) {
        this.header = System.lineSeparator() + header + System.lineSeparator() + System.lineSeparator();
        this.options = new Options();
        if (shouldAddDaysArg()) {
            addOptionWithArg("d", DAYS_ARG, "Number of days issued certificate should be valid for.", TlsConfig.DEFAULT_DAYS);
        }
        addOptionWithArg("T", KEY_STORE_TYPE_ARG, "The type of keyStores to generate.", getKeyStoreTypeDefault());
        options.addOption("h", HELP_ARG, false, "Print help and exit.");
        addOptionWithArg("c", CERTIFICATE_AUTHORITY_HOSTNAME_ARG, "Hostname of NiFi Certificate Authority", TlsConfig.DEFAULT_HOSTNAME);
        addOptionWithArg("a", KEY_ALGORITHM_ARG, "Algorithm to use for generated keys.", TlsConfig.DEFAULT_KEY_PAIR_ALGORITHM);
        addOptionWithArg("k", KEY_SIZE_ARG, "Number of bits for generated keys.", TlsConfig.DEFAULT_KEY_SIZE);
        if (shouldAddSigningAlgorithmArg()) {
            addOptionWithArg("s", SIGNING_ALGORITHM_ARG, "Algorithm to use for signing certificates.", TlsConfig.DEFAULT_SIGNING_ALGORITHM);
        }
        addOptionNoArg("g", DIFFERENT_KEY_AND_KEYSTORE_PASSWORDS_ARG, "Use different generated password for the key and the keyStore.");
    }

    protected String getKeyStoreTypeDefault() {
        return TlsConfig.DEFAULT_KEY_STORE_TYPE;
    }

    protected boolean shouldAddSigningAlgorithmArg() {
        return true;
    }

    protected boolean shouldAddDaysArg() {
        return true;
    }

    protected void addOptionWithArg(String arg, String longArg, String description) {
        addOptionWithArg(arg, longArg, description, null);
    }

    protected void addOptionNoArg(String arg, String longArg, String description) {
        options.addOption(arg, longArg, false, description);
    }

    protected void addOptionWithArg(String arg, String longArg, String description, Object defaultVal) {
        String fullDescription = description;
        if (defaultVal != null) {
            fullDescription += " (default: " + defaultVal + ")";
        }
        options.addOption(arg, longArg, true, fullDescription);
    }

    public void printUsage(String errorMessage) {
        if (errorMessage != null) {
            System.out.println(errorMessage);
            System.out.println();
        }
        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.setWidth(160);
        helpFormatter.printHelp(TlsToolkitMain.class.getCanonicalName(), header, options, FOOTER, true);
    }

    protected <T> T printUsageAndThrow(String errorMessage, ExitCode exitCode) throws CommandLineParseException {
        printUsage(errorMessage);
        throw new CommandLineParseException(errorMessage, exitCode);
    }

    protected int getIntValue(CommandLine commandLine, String arg, int defaultVal) throws CommandLineParseException {
        try {
            return Integer.parseInt(commandLine.getOptionValue(arg, Integer.toString(defaultVal)));
        } catch (NumberFormatException e) {
            return printUsageAndThrow("Expected integer for " + arg + " argument. (" + e.getMessage() + ")", ExitCode.ERROR_PARSING_INT_ARG);
        }
    }

    /**
     * Returns the number of bits used when generating KeyPairs
     *
     * @return the number of bits used when generating KeyPairs
     */
    public int getKeySize() {
        return keySize;
    }

    /**
     * Returns the algorithm used when generating KeyPairs
     *
     * @return the algorithm used when generating KeyPairs
     */
    public String getKeyAlgorithm() {
        return keyAlgorithm;
    }

    /**
     * Returns the CA Hostname
     *
     * @return the CA Hostname
     */
    public String getCertificateAuthorityHostname() {
        return certificateAuthorityHostname;
    }

    /**
     * Returns the type to use for KeyStores
     *
     * @return the type to use for KeyStores
     */
    public String getKeyStoreType() {
        return keyStoreType;
    }

    /**
     * Returns the number of Certificates should be valid for
     *
     * @return the number of Certificates should be valid for
     */
    public int getDays() {
        return days;
    }

    /**
     * Returns the signing algorithm to use for cryptographic operations
     *
     * @return the signing algorithm to use for cryptographic operations
     */
    public String getSigningAlgorithm() {
        return signingAlgorithm;
    }

    /**
     * Returns true if different passwords should be used for KeyStore and individual Key entries
     *
     * @return true if different passwords should be used for KeyStore and individual Key entries
     */
    public boolean differentPasswordForKeyAndKeystore() {
        return differentPasswordForKeyAndKeystore;
    }

    protected CommandLine doParse(String[] args) throws CommandLineParseException {
        CommandLineParser parser = new DefaultParser();
        CommandLine commandLine;
        try {
            commandLine = parser.parse(options, args);
            if (commandLine.hasOption(HELP_ARG)) {
                return printUsageAndThrow(null, ExitCode.HELP);
            }
            certificateAuthorityHostname = commandLine.getOptionValue(CERTIFICATE_AUTHORITY_HOSTNAME_ARG, TlsConfig.DEFAULT_HOSTNAME);
            days = getIntValue(commandLine, DAYS_ARG, TlsConfig.DEFAULT_DAYS);
            keySize = getIntValue(commandLine, KEY_SIZE_ARG, TlsConfig.DEFAULT_KEY_SIZE);
            keyAlgorithm = commandLine.getOptionValue(KEY_ALGORITHM_ARG, TlsConfig.DEFAULT_KEY_PAIR_ALGORITHM);
            keyStoreType = commandLine.getOptionValue(KEY_STORE_TYPE_ARG, getKeyStoreTypeDefault());
            signingAlgorithm = commandLine.getOptionValue(SIGNING_ALGORITHM_ARG, TlsConfig.DEFAULT_SIGNING_ALGORITHM);
            differentPasswordForKeyAndKeystore = commandLine.hasOption(DIFFERENT_KEY_AND_KEYSTORE_PASSWORDS_ARG);
        } catch (ParseException e) {
            return printUsageAndThrow("Error parsing command line. (" + e.getMessage() + ")", ExitCode.ERROR_PARSING_COMMAND_LINE);
        }
        return commandLine;
    }

    /**
     * Parses the command line arguments
     *
     * @param args the command line arguments
     * @throws CommandLineParseException if the arguments cannot be parsed
     */
    public void parse(String... args) throws CommandLineParseException {
        doParse(args);
    }
}
