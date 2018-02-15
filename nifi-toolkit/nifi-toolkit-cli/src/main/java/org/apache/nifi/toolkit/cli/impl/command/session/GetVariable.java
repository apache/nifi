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
import org.apache.nifi.toolkit.cli.api.Session;
import org.apache.nifi.toolkit.cli.api.SessionException;
import org.apache.nifi.toolkit.cli.impl.command.AbstractCommand;
import org.apache.nifi.toolkit.cli.impl.result.StringResult;

/**
 * Gets a the value of a variable from the session.
 */
public class GetVariable extends AbstractCommand<StringResult> {

    public GetVariable() {
        super("get", StringResult.class);
    }

    @Override
    public String getDescription() {
        return "Returns the value of the given variable in the session.";
    }

    @Override
    public StringResult execute(final CommandLine commandLine) throws CommandException {
        final String[] args = commandLine.getArgs();

        if (args == null || args.length != 1 || StringUtils.isBlank(args[0])) {
            throw new CommandException("Incorrect number of arguments, should be: <var>");
        }

        final Session session = getContext().getSession();
        try {
            final String value = session.get(args[0]);
            if (value == null) {
                return new StringResult("", getContext().isInteractive());
            } else {
                return new StringResult(value, getContext().isInteractive());
            }
        } catch (SessionException se) {
            throw new CommandException(se.getMessage(), se);
        }
    }

}
