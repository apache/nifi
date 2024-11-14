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

import org.apache.nifi.registry.client.NiFiRegistryClient;
import org.apache.nifi.toolkit.cli.api.ClientFactory;
import org.apache.nifi.toolkit.cli.api.Command;
import org.apache.nifi.toolkit.cli.api.CommandGroup;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.api.Session;
import org.apache.nifi.toolkit.cli.impl.client.NiFiClientFactory;
import org.apache.nifi.toolkit.cli.impl.client.NiFiRegistryClientFactory;
import org.apache.nifi.toolkit.cli.impl.command.CommandFactory;
import org.apache.nifi.toolkit.cli.impl.command.registry.NiFiRegistryCommandGroup;
import org.apache.nifi.toolkit.cli.impl.context.StandardContext;
import org.apache.nifi.toolkit.cli.impl.session.InMemorySession;
import org.apache.nifi.toolkit.cli.impl.session.SessionVariable;
import org.apache.nifi.toolkit.client.NiFiClient;
import org.apache.nifi.util.StringUtils;
import org.jline.reader.Candidate;
import org.jline.reader.LineReader;
import org.jline.reader.ParsedLine;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.mockito.Mockito;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisabledOnOs(OS.WINDOWS)
public class TestCLICompleter {

    private static final String TEST_RESOURCES_DIRECTORY = "src/test/resources";
    private static final String TEST_PROPERTIES = "test.properties";

    private static CLICompleter completer;
    private static LineReader lineReader;

    @BeforeAll
    public static void setupCompleter() {
        final Session session = new InMemorySession();
        final ClientFactory<NiFiClient> niFiClientFactory = new NiFiClientFactory();
        final ClientFactory<NiFiRegistryClient> nifiRegClientFactory = new NiFiRegistryClientFactory();

        final Context context = new StandardContext.Builder()
                .output(System.out)
                .session(session)
                .nifiClientFactory(niFiClientFactory)
                .nifiRegistryClientFactory(nifiRegClientFactory)
                .build();

        final Map<String, Command> commands = CommandFactory.createTopLevelCommands(context);
        final Map<String, CommandGroup> commandGroups = CommandFactory.createCommandGroups(context);

        completer = new CLICompleter(commands.values(), commandGroups.values());
        lineReader = Mockito.mock(LineReader.class);
    }

    @Test
    public void testCompletionWithWordIndexNegative() {
        final ParsedLine parsedLine = new TestParsedLine(Collections.emptyList(), -1);

        final List<Candidate> candidates = new ArrayList<>();
        completer.complete(lineReader, parsedLine, candidates);
        assertEquals(0, candidates.size());
    }

    @Test
    public void testCompletionWithWordIndexZero() {
        final ParsedLine parsedLine = new TestParsedLine(Collections.emptyList(), 0);

        final List<Candidate> candidates = new ArrayList<>();
        completer.complete(lineReader, parsedLine, candidates);
        assertEquals(completer.getTopLevelCommands().size(), candidates.size());
    }

    @Test
    public void testCompletionWithWordIndexOneAndMatching() {
        final String topCommand = NiFiRegistryCommandGroup.REGISTRY_COMMAND_GROUP;

        final ParsedLine parsedLine = new TestParsedLine(Collections.singletonList(topCommand), 1);

        final List<Candidate> candidates = new ArrayList<>();
        completer.complete(lineReader, parsedLine, candidates);
        assertEquals(completer.getSubCommands(topCommand).size(), candidates.size());
    }

    @Test
    public void testCompletionWithWordIndexOneAndNotMatching() {
        final String topCommand = "NOT-A-TOP-LEVEL-COMMAND";

        final ParsedLine parsedLine = new TestParsedLine(Collections.singletonList(topCommand), 1);

        final List<Candidate> candidates = new ArrayList<>();
        completer.complete(lineReader, parsedLine, candidates);
        assertEquals(0, candidates.size());
    }

    @Test
    public void testCompletionWithWordIndexTwoAndMatching() {
        final String topCommand = NiFiRegistryCommandGroup.REGISTRY_COMMAND_GROUP;
        final String subCommand = "list-buckets";

        final ParsedLine parsedLine = new TestParsedLine(Arrays.asList(topCommand, subCommand), 2);

        final List<Candidate> candidates = new ArrayList<>();
        completer.complete(lineReader, parsedLine, candidates);
        assertTrue(candidates.size() > 0);
        assertEquals(completer.getOptions(subCommand).size(), candidates.size());
    }

    @Test
    public void testCompletionWithWordIndexTwoAndNotMatching() {
        final String topCommand = NiFiRegistryCommandGroup.REGISTRY_COMMAND_GROUP;
        final String subCommand = "NOT-A-TOP-LEVEL-COMMAND";

        final ParsedLine parsedLine = new TestParsedLine(Arrays.asList(topCommand, subCommand), 2);

        final List<Candidate> candidates = new ArrayList<>();
        completer.complete(lineReader, parsedLine, candidates);
        assertEquals(0, candidates.size());
    }

    @Test
    public void testCompletionWithMultipleArguments() {
        final String topCommand = NiFiRegistryCommandGroup.REGISTRY_COMMAND_GROUP;
        final String subCommand = "list-buckets";

        final ParsedLine parsedLine = new TestParsedLine(Arrays.asList(topCommand, subCommand, "-ks", "foo", "-kst", "JKS"), 5);

        final List<Candidate> candidates = new ArrayList<>();
        completer.complete(lineReader, parsedLine, candidates);
        assertTrue(candidates.size() > 0);
        assertEquals(completer.getOptions(subCommand).size(), candidates.size());
    }

    @Test
    public void testCompletionWithFileArguments() {
        final String topCommand = NiFiRegistryCommandGroup.REGISTRY_COMMAND_GROUP;
        final String subCommand = "list-buckets";

        final String testResourcesDirectory = getTestResourcesDirectory();
        final ParsedLine parsedLine = new TestParsedLine(Arrays.asList(topCommand, subCommand, "-p", testResourcesDirectory), 3);

        final List<Candidate> candidates = new ArrayList<>();
        completer.complete(lineReader, parsedLine, candidates);

        assertTestPropertiesFound(candidates);
    }

    @Test
    public void testCompletionForSessionVariableNames() {
        final String topCommand = "session";
        final String subCommand = "set";

        final ParsedLine parsedLine = new TestParsedLine(Arrays.asList(topCommand, subCommand), 2);

        final List<Candidate> candidates = new ArrayList<>();
        completer.complete(lineReader, parsedLine, candidates);
        assertTrue(candidates.size() > 0);
        assertEquals(SessionVariable.values().length, candidates.size());
    }

    @Test
    public void testCompletionForSessionVariableWithFiles() {
        final String topCommand = "session";
        final String subCommand = "set";

        final String testResourcesDirectory = getTestResourcesDirectory();
        final ParsedLine parsedLine = new TestParsedLine(
                Arrays.asList(
                        topCommand,
                        subCommand,
                        SessionVariable.NIFI_CLIENT_PROPS.getVariableName(),
                        testResourcesDirectory),
                3);

        final List<Candidate> candidates = new ArrayList<>();
        completer.complete(lineReader, parsedLine, candidates);

        assertTestPropertiesFound(candidates);
    }

    private String getTestResourcesDirectory() {
        return Paths.get(TEST_RESOURCES_DIRECTORY).toAbsolutePath() + File.separator;
    }

    private void assertTestPropertiesFound(final List<Candidate> candidates) {
        final Optional<Candidate> candidateFound = candidates.stream()
                .filter(candidate -> candidate.value().endsWith(TEST_PROPERTIES))
                .findFirst();
        assertTrue(candidateFound.isPresent());
    }

    private static class TestParsedLine implements ParsedLine {
        private final List<String> words;

        private final int wordIndex;

        private TestParsedLine(
                final List<String> words,
                final int wordIndex) {
            this.words = words;
            this.wordIndex = wordIndex;
        }

        @Override
        public String word() {
            if (wordIndex >= words.size()) {
                return StringUtils.EMPTY;
            }
            return words.get(wordIndex);
        }

        @Override
        public int wordCursor() {
            return 0;
        }

        @Override
        public int wordIndex() {
            return wordIndex;
        }

        @Override
        public List<String> words() {
            return words;
        }

        @Override
        public String line() {
            return StringUtils.join(words, " ");
        }

        @Override
        public int cursor() {
            return 0;
        }
    }
}
