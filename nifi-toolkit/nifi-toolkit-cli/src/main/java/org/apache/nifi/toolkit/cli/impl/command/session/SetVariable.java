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
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.toolkit.cli.api.CommandException;
import org.apache.nifi.toolkit.cli.api.SessionException;
import org.apache.nifi.toolkit.cli.impl.command.AbstractCommand;
import org.apache.nifi.toolkit.cli.impl.result.VoidResult;

/**
 * Sets a variable in the session.
 */
public class SetVariable extends AbstractCommand<VoidResult> {

    public static final String NAME = "set";

    public SetVariable() {
        super(NAME, VoidResult.class);
    }

    @Override
    public String getDescription() {
        return "Sets the given variable in the session. " +
                "Use the 'keys' command to show the variable names that are supported.";
    }

    @Override
    public VoidResult execute(final CommandLine commandLine) throws CommandException {
        final String[] args = commandLine.getArgs();

        if (args == null || args.length < 2 || StringUtils.isBlank(args[0]) || StringUtils.isBlank(args[1])) {
            throw new CommandException("Incorrect number of arguments, should be: <var> <value>");
        }

        try {
            getContext().getSession().set(args[0], args[1]);
            return VoidResult.getInstance();
        } catch (SessionException se) {
            throw new CommandException(se.getMessage(), se);
        }
    }

}
