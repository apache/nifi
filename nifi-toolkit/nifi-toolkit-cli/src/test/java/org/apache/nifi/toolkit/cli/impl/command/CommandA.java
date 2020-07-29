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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.nifi.toolkit.cli.api.Command;
import org.apache.nifi.toolkit.cli.api.CommandException;
import org.apache.nifi.toolkit.cli.api.Context;

import java.util.List;

public class CommandA implements Command<CommandAResult> {

    private final List<String> results;

    private CommandLine cli;

    public CommandA(final List<String> results) {
        this.results = results;
    }

    @Override
    public void initialize(Context context) {

    }

    @Override
    public String getName() {
        return "command-a";
    }

    @Override
    public String getDescription() {
        return "command-a";
    }

    @Override
    public Options getOptions() {
        Options options = new Options();
        options.addOption(CommandOption.BUCKET_ID.createOption());
        options.addOption(CommandOption.FLOW_ID.createOption());
        return options;
    }

    @Override
    public void printUsage(String errorMessage) {

    }

    @Override
    public CommandAResult execute(CommandLine cli) throws CommandException {
        this.cli = cli;
        return new CommandAResult(results);
    }

    @Override
    public Class<CommandAResult> getResultImplType() {
        return CommandAResult.class;
    }

    public CommandLine getCli() {
        return this.cli;
    }

}
