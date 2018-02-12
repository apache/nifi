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
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClient;
import org.apache.nifi.toolkit.cli.impl.command.CommandFactory;
import org.apache.nifi.toolkit.cli.impl.command.registry.NiFiRegistryCommandGroup;
import org.apache.nifi.toolkit.cli.impl.context.StandardContext;
import org.apache.nifi.toolkit.cli.impl.session.InMemorySession;
import org.apache.nifi.toolkit.cli.impl.session.SessionVariable;
import org.jline.reader.Candidate;
import org.jline.reader.LineReader;
import org.jline.reader.impl.DefaultParser;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestCLICompleter {

    private static CLICompleter completer;
    private static LineReader lineReader;

    @BeforeClass
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

        final Map<String,Command> commands = CommandFactory.createTopLevelCommands(context);
        final Map<String,CommandGroup> commandGroups = CommandFactory.createCommandGroups(context);

        completer = new CLICompleter(commands.values(), commandGroups.values());
        lineReader = Mockito.mock(LineReader.class);
    }

    @Test
    public void testCompletionWithWordIndexNegative() {
        final DefaultParser.ArgumentList parsedLine = new DefaultParser.ArgumentList(
                "", Collections.emptyList(), -1, -1, -1);

        final List<Candidate> candidates = new ArrayList<>();
        completer.complete(lineReader, parsedLine, candidates);
        assertEquals(0, candidates.size());
    }

    @Test
    public void testCompletionWithWordIndexZero() {
        final DefaultParser.ArgumentList parsedLine = new DefaultParser.ArgumentList(
                "", Collections.emptyList(), 0, -1, -1);

        final List<Candidate> candidates = new ArrayList<>();
        completer.complete(lineReader, parsedLine, candidates);
        assertEquals(completer.getTopLevelCommands().size(), candidates.size());
    }

    @Test
    public void testCompletionWithWordIndexOneAndMatching() {
        final String topCommand = NiFiRegistryCommandGroup.REGISTRY_COMMAND_GROUP;

        final DefaultParser.ArgumentList parsedLine = new DefaultParser.ArgumentList(
                "", Collections.singletonList(topCommand), 1, -1, -1);

        final List<Candidate> candidates = new ArrayList<>();
        completer.complete(lineReader, parsedLine, candidates);
        assertEquals(completer.getSubCommands(topCommand).size(), candidates.size());
    }

    @Test
    public void testCompletionWithWordIndexOneAndNotMatching() {
        final String topCommand = "NOT-A-TOP-LEVEL-COMMAND";

        final DefaultParser.ArgumentList parsedLine = new DefaultParser.ArgumentList(
                "", Collections.singletonList(topCommand), 1, -1, -1);

        final List<Candidate> candidates = new ArrayList<>();
        completer.complete(lineReader, parsedLine, candidates);
        assertEquals(0, candidates.size());
    }

    @Test
    public void testCompletionWithWordIndexTwoAndMatching() {
        final String topCommand = NiFiRegistryCommandGroup.REGISTRY_COMMAND_GROUP;
        final String subCommand = "list-buckets";

        final DefaultParser.ArgumentList parsedLine = new DefaultParser.ArgumentList(
                "", Arrays.asList(topCommand, subCommand), 2, -1, -1);

        final List<Candidate> candidates = new ArrayList<>();
        completer.complete(lineReader, parsedLine, candidates);
        assertTrue(candidates.size() > 0);
        assertEquals(completer.getOptions(subCommand).size(), candidates.size());
    }

    @Test
    public void testCompletionWithWordIndexTwoAndNotMatching() {
        final String topCommand = NiFiRegistryCommandGroup.REGISTRY_COMMAND_GROUP;
        final String subCommand = "NOT-A-TOP-LEVEL-COMMAND";

        final DefaultParser.ArgumentList parsedLine = new DefaultParser.ArgumentList(
                "", Arrays.asList(topCommand, subCommand), 2, -1, -1);

        final List<Candidate> candidates = new ArrayList<>();
        completer.complete(lineReader, parsedLine, candidates);
        assertEquals(0, candidates.size());
    }

    @Test
    public void testCompletionWithMultipleArguments() {
        final String topCommand = NiFiRegistryCommandGroup.REGISTRY_COMMAND_GROUP;
        final String subCommand = "list-buckets";

        final DefaultParser.ArgumentList parsedLine = new DefaultParser.ArgumentList(
                "", Arrays.asList(topCommand, subCommand, "-ks", "foo", "-kst", "JKS"), 6, -1, -1);

        final List<Candidate> candidates = new ArrayList<>();
        completer.complete(lineReader, parsedLine, candidates);
        assertTrue(candidates.size() > 0);
        assertEquals(completer.getOptions(subCommand).size(), candidates.size());
    }

    @Test
    public void testCompletionWithFileArguments() {
        final String topCommand = NiFiRegistryCommandGroup.REGISTRY_COMMAND_GROUP;
        final String subCommand = "list-buckets";

        final DefaultParser.ArgumentList parsedLine = new DefaultParser.ArgumentList(
                "", Arrays.asList(topCommand, subCommand, "-p", "src/test/resources/"), 3, -1, -1);

        final List<Candidate> candidates = new ArrayList<>();
        completer.complete(lineReader, parsedLine, candidates);
        assertTrue(candidates.size() > 0);

        boolean found = false;
        for (Candidate candidate : candidates) {
            if (candidate.value().equals("src/test/resources/test.properties")) {
                found = true;
                break;
            }
        }

        assertTrue(found);
    }

    @Test
    public void testCompletionForSessionVariableNames() {
        final String topCommand = "session";
        final String subCommand = "set";

        final DefaultParser.ArgumentList parsedLine = new DefaultParser.ArgumentList(
                "", Arrays.asList(topCommand, subCommand), 2, -1, -1);

        final List<Candidate> candidates = new ArrayList<>();
        completer.complete(lineReader, parsedLine, candidates);
        assertTrue(candidates.size() > 0);
        assertEquals(SessionVariable.values().length, candidates.size());
    }

    @Test
    public void testCompletionForSessionVariableWithFiles() {
        final String topCommand = "session";
        final String subCommand = "set";

        final DefaultParser.ArgumentList parsedLine = new DefaultParser.ArgumentList("",
                Arrays.asList(
                        topCommand,
                        subCommand,
                        SessionVariable.NIFI_CLIENT_PROPS.getVariableName(),
                        "src/test/resources/"),
                3, -1, -1);

        final List<Candidate> candidates = new ArrayList<>();
        completer.complete(lineReader, parsedLine, candidates);
        assertTrue(candidates.size() > 0);

        boolean found = false;
        for (Candidate candidate : candidates) {
            if (candidate.value().equals("src/test/resources/test.properties")) {
                found = true;
                break;
            }
        }

        assertTrue(found);
    }

}
