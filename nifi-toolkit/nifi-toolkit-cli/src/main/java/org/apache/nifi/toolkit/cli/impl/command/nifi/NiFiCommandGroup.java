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
package org.apache.nifi.toolkit.cli.impl.command.nifi;

import org.apache.nifi.toolkit.cli.api.Command;
import org.apache.nifi.toolkit.cli.impl.command.AbstractCommandGroup;
import org.apache.nifi.toolkit.cli.impl.command.nifi.flow.ClusterSummary;
import org.apache.nifi.toolkit.cli.impl.command.nifi.flow.CurrentUser;
import org.apache.nifi.toolkit.cli.impl.command.nifi.flow.GetRootId;
import org.apache.nifi.toolkit.cli.impl.command.nifi.nodes.ConnectNode;
import org.apache.nifi.toolkit.cli.impl.command.nifi.nodes.OffloadNode;
import org.apache.nifi.toolkit.cli.impl.command.nifi.nodes.DeleteNode;
import org.apache.nifi.toolkit.cli.impl.command.nifi.nodes.DisconnectNode;
import org.apache.nifi.toolkit.cli.impl.command.nifi.nodes.GetNode;
import org.apache.nifi.toolkit.cli.impl.command.nifi.nodes.GetNodes;
import org.apache.nifi.toolkit.cli.impl.command.nifi.pg.PGChangeVersion;
import org.apache.nifi.toolkit.cli.impl.command.nifi.pg.PGDisableControllerServices;
import org.apache.nifi.toolkit.cli.impl.command.nifi.pg.PGEnableControllerServices;
import org.apache.nifi.toolkit.cli.impl.command.nifi.pg.PGGetAllVersions;
import org.apache.nifi.toolkit.cli.impl.command.nifi.pg.PGGetControllerServices;
import org.apache.nifi.toolkit.cli.impl.command.nifi.pg.PGGetVars;
import org.apache.nifi.toolkit.cli.impl.command.nifi.pg.PGGetVersion;
import org.apache.nifi.toolkit.cli.impl.command.nifi.pg.PGImport;
import org.apache.nifi.toolkit.cli.impl.command.nifi.pg.PGList;
import org.apache.nifi.toolkit.cli.impl.command.nifi.pg.PGSetVar;
import org.apache.nifi.toolkit.cli.impl.command.nifi.pg.PGStart;
import org.apache.nifi.toolkit.cli.impl.command.nifi.pg.PGStatus;
import org.apache.nifi.toolkit.cli.impl.command.nifi.pg.PGStop;
import org.apache.nifi.toolkit.cli.impl.command.nifi.registry.CreateRegistryClient;
import org.apache.nifi.toolkit.cli.impl.command.nifi.registry.GetRegistryClientId;
import org.apache.nifi.toolkit.cli.impl.command.nifi.registry.ListRegistryClients;
import org.apache.nifi.toolkit.cli.impl.command.nifi.registry.UpdateRegistryClient;

import java.util.ArrayList;
import java.util.List;

/**
 * CommandGroup for NiFi commands.
 */
public class NiFiCommandGroup extends AbstractCommandGroup {

    public static final String NIFI_COMMAND_GROUP = "nifi";

    public NiFiCommandGroup() {
        super(NIFI_COMMAND_GROUP);
    }

    @Override
    protected List<Command> createCommands() {
        final List<AbstractNiFiCommand> commands = new ArrayList<>();
        commands.add(new CurrentUser());
        commands.add(new ClusterSummary());
        commands.add(new ConnectNode());
        commands.add(new DeleteNode());
        commands.add(new DisconnectNode());
        commands.add(new GetRootId());
        commands.add(new GetNode());
        commands.add(new GetNodes());
        commands.add(new OffloadNode());
        commands.add(new ListRegistryClients());
        commands.add(new CreateRegistryClient());
        commands.add(new UpdateRegistryClient());
        commands.add(new GetRegistryClientId());
        commands.add(new PGImport());
        commands.add(new PGStart());
        commands.add(new PGStop());
        commands.add(new PGGetVars());
        commands.add(new PGSetVar());
        commands.add(new PGGetVersion());
        commands.add(new PGChangeVersion());
        commands.add(new PGGetAllVersions());
        commands.add(new PGList());
        commands.add(new PGStatus());
        commands.add(new PGGetControllerServices());
        commands.add(new PGEnableControllerServices());
        commands.add(new PGDisableControllerServices());
        return new ArrayList<>(commands);
    }
}
