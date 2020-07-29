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

import org.apache.nifi.toolkit.cli.api.Command;
import org.apache.nifi.toolkit.cli.api.CommandGroup;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.session.SessionCommandGroup;
import org.apache.nifi.toolkit.cli.impl.session.SessionVariable;
import org.jline.builtins.Completers;
import org.jline.reader.Candidate;
import org.jline.reader.Completer;
import org.jline.reader.LineReader;
import org.jline.reader.ParsedLine;
import org.jline.reader.impl.completer.ArgumentCompleter;
import org.jline.utils.AttributedString;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Main Completer for the CLI.
 */
public class CLICompleter implements Completer {

    private static final Set<String> FILE_COMPLETION_ARGS;
    static {
        final Set<String> args = new HashSet<>();
        for (final CommandOption option : CommandOption.values()) {
            if (option.isFile()) {
                args.add("-" + option.getShortName());
            }
        }
        FILE_COMPLETION_ARGS = Collections.unmodifiableSet(args);
    }

    private static final Set<String> FILE_COMPLETION_VARS;
    static {
        final Set<String> vars = new HashSet<>();
        vars.add(SessionVariable.NIFI_CLIENT_PROPS.getVariableName());
        vars.add(SessionVariable.NIFI_REGISTRY_CLIENT_PROPS.getVariableName());
        FILE_COMPLETION_VARS = Collections.unmodifiableSet(vars);
    }

    /**
     * Maps top-level commands to possible second-level commands.
     *
     * Some top-level commands like "exit" will have no sub-commands, others will be command groups like "nifi-reg".
     */
    private final Map<String, List<String>> topLevelCommandMap;

    /**
     * Maps second-level commands to their available options.
     *
     * Second-level commands would be the values in topLevelCommandMap above, and options would arguments likes "-ks" or "-ts".
     */
    private final Map<String, List<String>> commandOptionsMap;

    /**
     * Initializes the completer based on the top-level commands and command groups.
     *
     * @param topLevelCommands top-level commands like "exit"
     * @param commandGroups all command groups like "nifi" and "nifi-reg"
     */
    public CLICompleter(final Collection<Command> topLevelCommands, final Collection<CommandGroup> commandGroups) {
        final Map<String,List<String>> topLevel = new TreeMap<>();

        // add top-level commands like "exit" to the topLevel map with an empty list of sub-commands
        for (final Command topLevelCommand : topLevelCommands) {
            topLevel.put(topLevelCommand.getName(), Collections.emptyList());
        }

        // add each command group to the top-level map, with a list of all the sub-commands for the group
        for (final CommandGroup commandGroup : commandGroups) {
            final List<String> subCommands = commandGroup.getCommands().stream()
                    .map(cmd -> cmd.getName()).collect(Collectors.toList());
            topLevel.put(commandGroup.getName(), subCommands);
        }

        this.topLevelCommandMap = Collections.unmodifiableMap(topLevel);

        // map second-level commands to their available options
        final Map<String,List<String>> commandOptions = new TreeMap<>();

        // map each command to its possible options
        for (final CommandGroup commandGroup : commandGroups) {
            for (final Command command : commandGroup.getCommands()) {
                final List<String> options = command.getOptions().getOptions()
                        .stream().map(o -> "-" + o.getOpt())
                        .collect(Collectors.toList());
                commandOptions.put(command.getName(), options);
            }
        }

        this.commandOptionsMap = Collections.unmodifiableMap(commandOptions);
    }

    public Collection<String> getTopLevelCommands() {
        return topLevelCommandMap.keySet();
    }

    public Collection<String> getSubCommands(final String topLevelCommand) {
        return topLevelCommandMap.containsKey(topLevelCommand) ? topLevelCommandMap.get(topLevelCommand) : Collections.emptyList();
    }

    public Collection<String> getOptions(final String secondLevelCommand) {
        return commandOptionsMap.containsKey(secondLevelCommand) ? commandOptionsMap.get(secondLevelCommand) : Collections.emptyList();
    }

    @Override
    public void complete(final LineReader reader, final ParsedLine line, final List<Candidate> candidates) {
        Objects.requireNonNull(line);
        Objects.requireNonNull(candidates);

        if (line.wordIndex() < 0) {
            return;
        }

        if (line.wordIndex() == 0) {
            addCandidates(topLevelCommandMap.keySet(), candidates);
            return;
        }

        if (line.wordIndex() == 1) {
            final String firstLevel = line.words().get(0);

            if (topLevelCommandMap.containsKey(firstLevel)) {
                final List<String> subCommands = topLevelCommandMap.get(firstLevel);
                if (subCommands != null) {
                    addCandidates(subCommands, candidates);
                }
            }

            return;
        }

        if (line.wordIndex() >= 2) {
            final String firstLevel = line.words().get(0);
            final String secondLevel = line.words().get(1);

            // if not a valid top-level command then return
            if (!topLevelCommandMap.containsKey(firstLevel)) {
                return;
            }

            // if second level is not a valid sub-command of first level, then return
            final List<String> subCommands = topLevelCommandMap.get(firstLevel);
            if (!subCommands.contains(secondLevel)) {
                return;
            }

            // if there are no options of the second level command, then return
            if (!commandOptionsMap.containsKey(secondLevel)) {
                return;
            }

            // session commands are a different format so we need different completion
            if (SessionCommandGroup.NAME.equals(firstLevel)) {
                // if we have two args then we are completing the variable name
                // if we have three args, and the third is one a variable that is a file path, then we need a file completer
                if (line.wordIndex() == 2) {
                    addCandidates(SessionVariable.getAllVariableNames(), candidates);
                } else if (line.wordIndex() == 3) {
                    final String currWord = line.word();
                    final String prevWord = line.words().get(line.wordIndex() - 1);
                    if (FILE_COMPLETION_VARS.contains(prevWord)) {
                        final Completers.FileNameCompleter fileNameCompleter = new Completers.FileNameCompleter();
                        fileNameCompleter.complete(reader, new ArgumentCompleter.ArgumentLine(currWord, currWord.length()), candidates);
                    }
                }
            } else {
                // we know we have at least 3 words here, so get the current word and the word before current
                final String currWord = line.word();
                final String prevWord = line.words().get(line.wordIndex() - 1);

                // determine if the word before the current is an arg that needs file completion, otherwise return all args
                if (FILE_COMPLETION_ARGS.contains(prevWord)) {
                    final Completers.FileNameCompleter fileNameCompleter = new Completers.FileNameCompleter();
                    fileNameCompleter.complete(reader, new ArgumentCompleter.ArgumentLine(currWord, currWord.length()), candidates);
                } else {
                    final List<String> options = commandOptionsMap.get(secondLevel);
                    if (options != null) {
                        addCandidates(options, candidates);
                    }
                }
            }
        }
    }

    private void addCandidates(final Collection<String> values, final List<Candidate> candidates) {
        for (final String value : values) {
            candidates.add(new Candidate(AttributedString.stripAnsi(value), value, null, null, null, null, true));
        }
    }
}
