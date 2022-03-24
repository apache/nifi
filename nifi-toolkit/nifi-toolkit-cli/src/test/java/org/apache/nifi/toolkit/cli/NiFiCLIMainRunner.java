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
import org.apache.nifi.toolkit.cli.impl.command.CommandProcessor;
import org.apache.nifi.toolkit.cli.impl.context.StandardContext;
import org.apache.nifi.toolkit.cli.impl.session.InMemorySession;

import java.util.Map;

public class NiFiCLIMainRunner {

    public static void main(String[] args) {
        final String[] cmdArgs = ("registry list-buckets help " +
                "").split("[ ]");

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

        final CommandProcessor processor = new CommandProcessor(commands, commandGroups, context);
        processor.process(cmdArgs);
    }
}
