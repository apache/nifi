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
package org.apache.nifi.toolkit.cli.impl.command.session;

import org.apache.nifi.toolkit.cli.api.Command;
import org.apache.nifi.toolkit.cli.impl.command.AbstractCommandGroup;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Command group for all session commands.
 */
public class SessionCommandGroup extends AbstractCommandGroup {

    public static final String NAME = "session";

    public SessionCommandGroup() {
        super(NAME);
    }

    @Override
    protected List<Command> createCommands() {
        final List<Command> commands = new ArrayList<>();
        commands.add(new ShowKeys());
        commands.add(new ShowSession());
        commands.add(new GetVariable());
        commands.add(new SetVariable());
        commands.add(new RemoveVariable());
        commands.add(new ClearSession());
        return Collections.unmodifiableList(commands);
    }
}
