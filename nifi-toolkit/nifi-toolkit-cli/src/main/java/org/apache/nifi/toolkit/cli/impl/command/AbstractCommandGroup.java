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

import org.apache.commons.cli.help.TextHelpAppendable;
import org.apache.commons.lang3.Validate;
import org.apache.nifi.toolkit.cli.api.Command;
import org.apache.nifi.toolkit.cli.api.CommandGroup;
import org.apache.nifi.toolkit.cli.api.Context;

import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Base class for CommandGroups to extend from.
 */
public abstract class AbstractCommandGroup implements CommandGroup {

    private final String name;
    private PrintStream output;
    private List<Command> commands;

    public AbstractCommandGroup(final String name) {
        this.name = name;
        Validate.notBlank(this.name);
    }

    @Override
    public final void initialize(final Context context) {
        Objects.requireNonNull(context);
        this.output = context.getOutput();
        this.commands = Collections.unmodifiableList(createCommands());
        this.commands.stream().forEach(c -> c.initialize(context));
    }

    /**
     * Subclasses override to provide the appropriate commands for the given group.
     *
     * @return the list of commands for this group
     */
    protected abstract List<Command> createCommands();

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public List<Command> getCommands() {
        return this.commands;
    }

    @Override
    public void printUsage(final boolean verbose) {
        if (verbose) {

            final PrintWriter printWriter = new PrintWriter(output);
            final TextHelpAppendable appendable = new TextHelpAppendable(printWriter);
            appendable.setMaxWidth(80);

            commands.stream().forEach(c -> {
                try {
                    appendable.appendParagraph("-------------------------------------------------------------------------------");
                    appendable.appendParagraph("COMMAND: " + getName() + " " + c.getName());
                    appendable.appendParagraph("- " + c.getDescription());

                    if (c.isReferencable()) {
                        appendable.appendParagraph("PRODUCES BACK-REFERENCES");
                    }
                } catch (IOException e) {
                    throw new RuntimeException("Unable to print command usage for " + c.getName(), e);
                }
            });

            printWriter.flush();

        } else {
            commands.stream().forEach(c -> output.println("\t" + getName() + " " + c.getName()));
        }
        output.flush();
    }

}
