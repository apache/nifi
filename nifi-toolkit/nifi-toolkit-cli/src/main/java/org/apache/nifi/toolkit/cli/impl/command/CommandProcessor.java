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
package org.apache.nifi.toolkit.cli.impl.command;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.Validate;
import org.apache.nifi.toolkit.cli.api.Command;
import org.apache.nifi.toolkit.cli.api.CommandGroup;
import org.apache.nifi.toolkit.cli.api.Context;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.Map;

/**
 * Takes the arguments from the shell and executes the appropriate command, or prints appropriate usage.
 */
public class CommandProcessor {

    private final Map<String,Command> topLevelCommands;
    private final Map<String,CommandGroup> commandGroups;
    private final Context context;
    private final PrintStream out;

    public CommandProcessor(final Map<String,Command> topLevelCommands, final Map<String,CommandGroup> commandGroups, final Context context) {
        this.topLevelCommands = topLevelCommands;
        this.commandGroups = commandGroups;
        this.context = context;
        this.out = context.getOutput();
        Validate.notNull(this.topLevelCommands);
        Validate.notNull(this.commandGroups);
        Validate.notNull(this.context);
        Validate.notNull(this.out);
    }

    public void printBasicUsage(String errorMessage) {
        printBasicUsage(errorMessage, false);
    }

    public void printBasicUsage(String errorMessage, boolean verbose) {
        out.println();

        if (errorMessage != null) {
            out.println("ERROR: " + errorMessage);
            out.println();
        }

        out.println("commands:");
        out.println();

        commandGroups.entrySet().stream().forEach(e -> e.getValue().printUsage(verbose));
        out.println("-------------------------------------------------------------------------------");
        topLevelCommands.keySet().stream().forEach(k -> out.println("\t" + k));
        out.println();
    }

    private CommandLine parseCli(Command command, String[] args) throws ParseException {
        final Options options = command.getOptions();
        final CommandLineParser parser = new DefaultParser();
        final CommandLine commandLine = parser.parse(options, args);

        if (commandLine.hasOption(CommandOption.HELP.getLongName())) {
            command.printUsage(null);
            return null;
        }

        return commandLine;
    }

    public void process(String[] args) {
        if (args == null || args.length == 0) {
            printBasicUsage(null);
            return;
        }

        if (CommandOption.HELP.getLongName().equalsIgnoreCase(args[0])) {
            if (args.length == 2 && "-v".equalsIgnoreCase(args[1])) {
                printBasicUsage(null, true);
                return;
            } else {
                printBasicUsage(null);
                return;
            }
        }

        final String commandStr = args[0];
        if (topLevelCommands.containsKey(commandStr)) {
            processTopLevelCommand(commandStr, args);
        } else if (commandGroups.containsKey(commandStr)) {
            processGroupCommand(commandStr, args);
        } else {
            printBasicUsage("Unknown command '" + commandStr + "'");
            return;
        }
    }

    private void processTopLevelCommand(final String commandStr, final String[] args) {
        try {
            final Command command = topLevelCommands.get(commandStr);

            final String[] otherArgs = Arrays.copyOfRange(args, 1, args.length, String[].class);
            final CommandLine commandLine = parseCli(command, otherArgs);
            if (commandLine == null) {
                out.println("Unable to parse command line");
                return;
            }

            try {
                if (otherArgs.length == 1 && CommandOption.HELP.getLongName().equalsIgnoreCase(otherArgs[0])) {
                    command.printUsage(null);
                } else {
                    command.execute(commandLine);
                }
            } catch (Exception e) {
                command.printUsage(e.getMessage());
                if (commandLine.hasOption(CommandOption.VERBOSE.getLongName())) {
                    out.println();
                    e.printStackTrace(out);
                    out.println();
                }
            }

        } catch (Exception e) {
            out.println();
            e.printStackTrace(out);
            out.println();
        }
    }

    private void processGroupCommand(final String commandGroupStr, final String[] args) {
        if (args.length <= 1) {
            printBasicUsage("No command provided to " + commandGroupStr);
            return;
        }

        final String commandStr = args[1];
        final CommandGroup commandGroup = commandGroups.get(commandGroupStr);

        final Command command = commandGroup.getCommands().stream()
                .filter(c -> c.getName().equals(commandStr))
                .findFirst()
                .orElse(null);

        if (command == null) {
            printBasicUsage("Unknown command '" + commandGroupStr + " " + commandStr + "'");
            return;
        }

        try {
            final String[] otherArgs = Arrays.copyOfRange(args, 2, args.length, String[].class);
            final CommandLine commandLine = parseCli(command, otherArgs);
            if (commandLine == null) {
                out.println("Unable to parse command line");
                return;
            }

            try {
                if (otherArgs.length == 1 && CommandOption.HELP.getLongName().equalsIgnoreCase(otherArgs[0])) {
                    command.printUsage(null);
                } else {
                    command.execute(commandLine);
                }
            } catch (Exception e) {
                command.printUsage(e.getMessage());
                if (commandLine.hasOption(CommandOption.VERBOSE.getLongName())) {
                    out.println();
                    e.printStackTrace(out);
                    out.println();
                }
            }

        } catch (Exception e) {
            out.println();
            e.printStackTrace(out);
            out.println();
        }
    }


}
