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

import org.apache.nifi.toolkit.cli.api.Command;
import org.apache.nifi.toolkit.cli.api.CommandGroup;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.command.composite.DemoCommandGroup;
import org.apache.nifi.toolkit.cli.impl.command.misc.Exit;
import org.apache.nifi.toolkit.cli.impl.command.misc.Help;
import org.apache.nifi.toolkit.cli.impl.command.nifi.NiFiCommandGroup;
import org.apache.nifi.toolkit.cli.impl.command.registry.NiFiRegistryCommandGroup;
import org.apache.nifi.toolkit.cli.impl.command.session.SessionCommandGroup;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Creates and initializes all of the available commands.
 */
public class CommandFactory {

    public static Map<String,Command> createTopLevelCommands(final Context context) {
        final List<Command> commandList = new ArrayList<>();
        commandList.add(new Help());
        commandList.add(new Exit());

        final Map<String,Command> commandMap = new TreeMap<>();
        commandList.stream().forEach(cmd -> {
            cmd.initialize(context);
            commandMap.put(cmd.getName(), cmd);
        });

        return Collections.unmodifiableMap(commandMap);
    }

    public static Map<String,CommandGroup> createCommandGroups(final Context context) {

        final List<CommandGroup> groups = new ArrayList<>();
        groups.add(new NiFiRegistryCommandGroup());
        groups.add(new NiFiCommandGroup());
        groups.add(new DemoCommandGroup());
        groups.add(new SessionCommandGroup());

        final Map<String,CommandGroup> groupMap = new TreeMap<>();
        groups.stream().forEach(g -> {
            g.initialize(context);
            groupMap.put(g.getName(), g);
        });
        return Collections.unmodifiableMap(groupMap);
    }
}
