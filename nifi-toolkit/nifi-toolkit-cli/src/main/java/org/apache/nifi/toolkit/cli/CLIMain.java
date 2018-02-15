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
package org.apache.nifi.toolkit.cli;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.registry.client.NiFiRegistryClient;
import org.apache.nifi.toolkit.cli.api.ClientFactory;
import org.apache.nifi.toolkit.cli.api.Command;
import org.apache.nifi.toolkit.cli.api.CommandGroup;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.api.Session;
import org.apache.nifi.toolkit.cli.impl.client.NiFiClientFactory;
import org.apache.nifi.toolkit.cli.impl.client.NiFiRegistryClientFactory;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClient;
import org.apache.nifi.toolkit.cli.impl.command.CommandFactory;
import org.apache.nifi.toolkit.cli.impl.command.CommandProcessor;
import org.apache.nifi.toolkit.cli.impl.context.StandardContext;
import org.apache.nifi.toolkit.cli.impl.session.InMemorySession;
import org.apache.nifi.toolkit.cli.impl.session.PersistentSession;
import org.jline.reader.Completer;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.ParsedLine;
import org.jline.reader.UserInterruptException;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Map;

/**
 * Main entry point for the CLI.
 *
 * If the main method is executed with no arguments, then the interactive shell is launched.
 *
 * If the main method is executed with arguments, then a single command will be executed, and the process will exit.
 */
public class CLIMain {

    public static final String SHELL_NAME = "nifi-registry";
    public static final String PROMPT = "#> ";
    public static final String BANNER_FILE = "nifi-banner.txt";
    public static final String SESSION_PERSISTENCE_FILE = ".nifi-cli.config";

    public static void main(String[] args) throws IOException {
        if (args == null || args.length == 0) {
            runInteractiveCLI();
        } else {
            // in standalone mode we want to make sure the process exits with the correct status
            try {
                final int returnCode = runSingleCommand(args);
                System.exit(returnCode);
            } catch (Exception e) {
                // shouldn't really get here, but just in case
                e.printStackTrace();
                System.exit(-1);
            }
        }
    }

    /**
     * Runs the interactive CLI.
     *
     * @throws IOException if an error occurs
     */
    private static void runInteractiveCLI() throws IOException {
        //Logger.getLogger("org.jline").setLevel(Level.FINE);
        try (final Terminal terminal = TerminalBuilder.builder()
                .name(SHELL_NAME)
                .system(true)
                .nativeSignals(true)
                .signalHandler(Terminal.SignalHandler.SIG_IGN)
                .build();
             final PrintStream output = new PrintStream(terminal.output(), true)) {

            printHeader(BANNER_FILE, output);

            final Context context = createContext(output, true);

            final Map<String,Command> topLevelCommands = CommandFactory.createTopLevelCommands(context);
            final Map<String,CommandGroup> commandGroups = CommandFactory.createCommandGroups(context);

            final CommandProcessor commandProcessor = new CommandProcessor(topLevelCommands, commandGroups, context);
            final Completer completer = new CLICompleter(topLevelCommands.values(), commandGroups.values());

            final LineReader reader = LineReaderBuilder.builder()
                    .appName(SHELL_NAME)
                    .terminal(terminal)
                    .completer(completer)
                    .build();
            reader.setOpt(LineReader.Option.AUTO_FRESH_LINE);
            reader.unsetOpt(LineReader.Option.INSERT_TAB);

            while (true) {
                try {
                    final String line = reader.readLine(PROMPT);
                    if (StringUtils.isBlank(line)) {
                        continue;
                    }

                    final ParsedLine parsedLine = reader.getParsedLine();
                    final String[] parsedArgs = parsedLine.words().toArray(new String[parsedLine.words().size()]);
                    commandProcessor.process(parsedArgs);
                } catch (UserInterruptException e) {
                    // Ignore
                } catch (EndOfFileException e) {
                    return;
                }
            }
        }
    }

    /**
     * Handles running a single command and exiting, non-interactive mode.
     *
     * @param args the args passed in from the command line
     */
    private static int runSingleCommand(final String[] args) {
        final Context context = createContext(System.out, false);
        final Map<String,Command> topLevelCommands = CommandFactory.createTopLevelCommands(context);
        final Map<String,CommandGroup> commandGroups = CommandFactory.createCommandGroups(context);

        final CommandProcessor commandProcessor = new CommandProcessor(topLevelCommands, commandGroups, context);
        return commandProcessor.process(args);
    }

    private static Context createContext(final PrintStream output, final boolean isInteractive) {
        Session session;

        final String userHomeValue = System.getProperty("user.home");
        final File userHome = Paths.get(userHomeValue).toFile();

        if (!userHome.exists() || !userHome.canRead() || !userHome.canWrite()) {
            session = new InMemorySession();
            if (isInteractive) {
                output.println();
                output.println("Unable to create session from " + userHomeValue + ", falling back to in-memory session");
                output.println();
            }
        } else {
            final InMemorySession inMemorySession = new InMemorySession();
            final File sessionState = new File(userHome.getAbsolutePath(), SESSION_PERSISTENCE_FILE);

            try {
                if (!sessionState.exists()) {
                    sessionState.createNewFile();
                }

                final PersistentSession persistentSession = new PersistentSession(sessionState, inMemorySession);
                persistentSession.loadSession();
                session = persistentSession;

                if (isInteractive) {
                    output.println();
                    output.println("Session loaded from " + sessionState.getAbsolutePath());
                    output.println();
                }
            } catch (Exception e) {
                session = inMemorySession;

                if (isInteractive) {
                    output.println();
                    output.println("Unable to load session from " + sessionState.getAbsolutePath()
                            + ", falling back to in-memory session");
                    output.println();
                }
            }

        }

        final ClientFactory<NiFiClient> niFiClientFactory = new NiFiClientFactory();
        final ClientFactory<NiFiRegistryClient> nifiRegClientFactory = new NiFiRegistryClientFactory();

        return new StandardContext.Builder()
                .output(output)
                .session(session)
                .nifiClientFactory(niFiClientFactory)
                .nifiRegistryClientFactory(nifiRegClientFactory)
                .interactive(isInteractive)
                .build();
    }

    protected static void printHeader(final String bannerFile, final PrintStream output) throws IOException {
        try (final InputStream bannerInput = CLIMain.class.getClassLoader().getResourceAsStream(bannerFile)) {
            final String bannerContent = IOUtils.toString(bannerInput, StandardCharsets.UTF_8);
            output.println(bannerContent);
            output.println();
            output.println("Type 'help' to see a list of available commands, use tab to auto-complete.");
            output.println();
        }
    }
}
