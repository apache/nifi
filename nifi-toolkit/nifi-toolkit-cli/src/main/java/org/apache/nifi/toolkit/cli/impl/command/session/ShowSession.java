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

import org.apache.commons.cli.CommandLine;
import org.apache.nifi.toolkit.cli.api.CommandException;
import org.apache.nifi.toolkit.cli.api.Session;
import org.apache.nifi.toolkit.cli.api.SessionException;
import org.apache.nifi.toolkit.cli.impl.command.AbstractCommand;

import java.io.PrintStream;

/**
 * Command to list all variables and their values.
 */
public class ShowSession extends AbstractCommand {

    public ShowSession() {
        super("show");
    }

    @Override
    public void execute(final CommandLine cli) throws CommandException {
        try {
            final Session session = getContext().getSession();
            final PrintStream printStream = getContext().getOutput();
            session.printVariables(printStream);
        } catch (SessionException se) {
            throw new CommandException(se.getMessage(), se);
        }
    }

}
