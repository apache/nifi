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
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.toolkit.cli.api.Command;
import org.apache.nifi.toolkit.cli.api.CommandException;
import org.apache.nifi.toolkit.cli.api.CommandGroup;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.api.ReferenceResolver;
import org.apache.nifi.toolkit.cli.api.Referenceable;
import org.apache.nifi.toolkit.cli.api.Result;
import org.apache.nifi.toolkit.cli.api.WritableResult;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Takes the arguments from the shell and executes the appropriate command, or prints appropriate usage.
 */
public class CommandProcessor {

    public static final String BACK_REF_INDICATOR = "&";

    private final Map<String,Command> topLevelCommands;
    private final Map<String,CommandGroup> commandGroups;
    private final Context context;
    private final PrintStream out;

    private final AtomicReference<ReferenceResolver> backReferenceHolder = new AtomicReference<>(null);

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
        if (verbose) {
            out.println("-------------------------------------------------------------------------------");
        }
        topLevelCommands.keySet().stream().forEach(k -> out.println("\t" + k));
        out.println();
    }

    private CommandLine parseCli(final Command command, final String[] args) throws ParseException {
        // resolve any back-references so the CommandLine ends up with the resolved values in the Options
        resolveBackReferences(args);

        final Options options = command.getOptions();
        final CommandLineParser parser = new DefaultParser();
        final CommandLine commandLine = parser.parse(options, args);

        if (commandLine.hasOption(CommandOption.HELP.getLongName())) {
            command.printUsage(null);
            return null;
        }

        return commandLine;
    }

    /**
     * Finds any args that indicate a back-reference and replaces the value of the arg with the
     * resolved back-reference.
     *
     * If the reference does not resolve, or non-numeric position is given, then the arg is left unchanged.
     *
     * @param args the args to process
     */
    private void resolveBackReferences(final String[] args) {
        final ReferenceResolver referenceResolver = backReferenceHolder.get();
        if (referenceResolver == null) {
            return;
        }

        for (int i=0; i < args.length; i++) {
            final String arg = args[i];
            if (arg == null || !arg.startsWith(BACK_REF_INDICATOR)) {
                continue;
            }

            if (context.isInteractive()) {
                context.getOutput().println();
            }

            try {
                final Integer pos = Integer.valueOf(arg.substring(1));
                final String resolvedReference = referenceResolver.resolve(pos);
                if (resolvedReference != null) {
                    args[i] = resolvedReference;
                }
            } catch (Exception e) {
                // skip
            }
        }
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
        final Command command = topLevelCommands.get(commandStr);

        if (command == null) {
            printBasicUsage("Unknown command '" + commandStr + "'");
            return;
        }

        try {
            final String[] otherArgs = Arrays.copyOfRange(args, 1, args.length, String[].class);
            final CommandLine commandLine = parseCli(command, otherArgs);
            if (commandLine == null) {
                out.println("Unable to parse command line");
                return;
            }

            processCommand(otherArgs, commandLine, command);

        } catch (Exception e) {
            command.printUsage(e.getMessage());
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

            processCommand(otherArgs, commandLine, command);

        } catch (Exception e) {
            command.printUsage(e.getMessage());
        }
    }

    private void processCommand(final String[] args, final CommandLine commandLine, final Command command) {
        try {
            if (args.length == 1 && CommandOption.HELP.getLongName().equalsIgnoreCase(args[0])) {
                command.printUsage(null);
            } else {
                final Result result = command.execute(commandLine);

                if (result instanceof WritableResult) {
                    final WritableResult writableResult = (WritableResult) result;
                    writableResult.write(out);
                }

                // if the Result is Referenceable then create the resolver and store it in the holder for the next command
                if (result instanceof Referenceable) {
                    final Referenceable referenceable = (Referenceable) result;
                    final ReferenceResolver referenceResolver = referenceable.createReferenceResolver(context);

                    // only set the resolve if its not empty so that a resolver that was already in there sticks around
                    // and can be used again if the current command didn't produce anything to resolve
                    if (!referenceResolver.isEmpty()) {
                        backReferenceHolder.set(referenceResolver);
                    }
                }
            }
        } catch (Exception e) {
            // CommandExceptions will wrap things like NiFiClientException, NiFiRegistryException, and IOException,
            // so for those we don't need to print the usage every time
            if (e instanceof CommandException) {
                out.println();
                out.println("ERROR: " + e.getMessage());
                out.println();
            } else {
                command.printUsage(e.getMessage());
            }

            if (commandLine.hasOption(CommandOption.VERBOSE.getLongName())) {
                out.println();
                e.printStackTrace(out);
                out.println();
            }
        }
    }


}
