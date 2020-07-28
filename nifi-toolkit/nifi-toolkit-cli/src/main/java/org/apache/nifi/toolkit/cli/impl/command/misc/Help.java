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
package org.apache.nifi.toolkit.cli.impl.command.misc;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.nifi.toolkit.cli.api.Command;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.result.VoidResult;

/**
 * Place-holder so "help" shows up in top-level commands.
 */
public class Help implements Command<VoidResult> {

    @Override
    public void initialize(final Context context) {

    }

    @Override
    public String getName() {
        return "help";
    }

    @Override
    public String getDescription() {
        return "";
    }

    @Override
    public Options getOptions() {
        return new Options();
    }

    @Override
    public void printUsage(String errorMessage) {

    }

    @Override
    public VoidResult execute(final CommandLine cli) {
        return VoidResult.getInstance();
    }

    @Override
    public Class<VoidResult> getResultImplType() {
        return VoidResult.class;
    }
}
