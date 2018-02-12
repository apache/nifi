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
import org.apache.nifi.toolkit.cli.impl.command.AbstractCommand;
import org.apache.nifi.toolkit.cli.impl.result.VoidResult;
import org.apache.nifi.toolkit.cli.impl.session.SessionVariable;

/**
 * Command for listing available variables.
 */
public class ShowKeys extends AbstractCommand<VoidResult> {

    public ShowKeys() {
        super("keys", VoidResult.class);
    }

    @Override
    public String getDescription() {
        return "Returns the available variable names that can be set in the session.";
    }

    @Override
    public VoidResult execute(CommandLine cli) throws CommandException {
        println();
        for (final SessionVariable variable : SessionVariable.values()) {
            println("\t" + variable.getVariableName());
        }
        println();

        return VoidResult.getInstance();
    }
}
