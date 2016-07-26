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
import org.apache.nifi.toolkit.tls.configuration.TlsHelperConfig;

public abstract class BaseCommandLine {
    public static final String HELP_ARG = "help";
    public static final String JAVA_HOME = "JAVA_HOME";
    public static final String NIFI_TOOLKIT_HOME = "NIFI_TOOLKIT_HOME";
    public static final String FOOTER = new StringBuilder(System.lineSeparator()).append("Java home: ")
            .append(System.getenv(JAVA_HOME)).append(System.lineSeparator()).append("NiFi Toolkit home: ").append(System.getenv(NIFI_TOOLKIT_HOME)).toString();

    public static final String KEY_SIZE_ARG = "keySize";
    public static final String KEY_ALGORITHM_ARG = "keyAlgorithm";

    public static final int HELP_EXIT_CODE = 1;
    public static final int ERROR_PARSING_COMMAND_LINE = 2;
    public static final int ERROR_PARSING_INT_ARG = 3;

    private final Options options;
    private final String header;
    private int keySize;
    private String keyAlgorithm;

    public BaseCommandLine(String header) {
        this.header = header;
        this.options = new Options();
        options.addOption("h", HELP_ARG, false, "Print help and exit.");
        addOptionWithArg("a", KEY_ALGORITHM_ARG, "Algorithm to use for generated keys.", TlsHelperConfig.DEFAULT_KEY_PAIR_ALGORITHM);
        addOptionWithArg("k", KEY_SIZE_ARG, "Number of bits for generated keys.", TlsHelperConfig.DEFAULT_KEY_SIZE);
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

    protected <T> T printUsageAndThrow(String errorMessage, int exitCode) throws CommandLineParseException {
        printUsage(errorMessage);
        throw new CommandLineParseException(errorMessage, exitCode);
    }

    protected int getIntValue(CommandLine commandLine, String arg, int defaultVal) throws CommandLineParseException {
        try {
            return Integer.parseInt(commandLine.getOptionValue(arg, Integer.toString(defaultVal)));
        } catch (NumberFormatException e) {
            return printUsageAndThrow("Expected integer for " + arg + " argument. (" + e.getMessage() + ")", ERROR_PARSING_INT_ARG);
        }
    }

    public int getKeySize() {
        return keySize;
    }

    public String getKeyAlgorithm() {
        return keyAlgorithm;
    }

    protected CommandLine doParse(String[] args) throws CommandLineParseException {
        CommandLineParser parser = new DefaultParser();
        CommandLine commandLine;
        try {
            commandLine = parser.parse(options, args);
            if (commandLine.hasOption(HELP_ARG)) {
                return printUsageAndThrow(null, HELP_EXIT_CODE);
            }
            keySize = getIntValue(commandLine, KEY_SIZE_ARG, TlsHelperConfig.DEFAULT_KEY_SIZE);
            keyAlgorithm = commandLine.getOptionValue(KEY_ALGORITHM_ARG, TlsHelperConfig.DEFAULT_KEY_PAIR_ALGORITHM);
        } catch (ParseException e) {
            return printUsageAndThrow("Error parsing command line. (" + e.getMessage() + ")", ERROR_PARSING_COMMAND_LINE);
        }
        return commandLine;
    }

    public void parse(String... args) throws CommandLineParseException {
        doParse(args);
    }
}
