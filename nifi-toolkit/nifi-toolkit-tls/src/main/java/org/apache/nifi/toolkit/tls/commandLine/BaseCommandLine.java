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

public abstract class BaseCommandLine {

    public static final String HELP_ARG = "help";
    public static final String JAVA_HOME = "JAVA_HOME";
    public static final String NIFI_TOOLKIT_HOME = "NIFI_TOOLKIT_HOME";
    public static final String FOOTER = new StringBuilder(System.lineSeparator()).append("Java home: ")
            .append(System.getenv(JAVA_HOME)).append(System.lineSeparator()).append("NiFi Toolkit home: ").append(System.getenv(NIFI_TOOLKIT_HOME)).toString();

    private final Options options;
    private final String header;

    public BaseCommandLine(String header) {
        this.header = System.lineSeparator() + header + System.lineSeparator() + System.lineSeparator();
        this.options = new Options();
        this.options.addOption("h", HELP_ARG, false, "Print help and exit.");
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

    protected CommandLine doParse(String[] args) throws CommandLineParseException {
        CommandLineParser parser = new DefaultParser();
        CommandLine commandLine;
        try {
            commandLine = parser.parse(options, args);
            if (commandLine.hasOption(HELP_ARG)) {
                return printUsageAndThrow(null, ExitCode.HELP);
            }
            postParse(commandLine);
        } catch (ParseException e) {
            return printUsageAndThrow("Error parsing command line. (" + e.getMessage() + ")", ExitCode.ERROR_PARSING_COMMAND_LINE);
        }
        return commandLine;
    }

    protected void postParse(CommandLine commandLine) throws CommandLineParseException {

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
