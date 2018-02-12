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
package org.apache.nifi.toolkit.cli.api;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

/**
 * Represents a command to execute.
 */
public interface Command<R extends Result> {

    /**
     * Called directly after instantiation of the given command before any other method is called.
     *
     * @param context the context of the CLI
     */
    void initialize(Context context);

    /**
     * @return the name of the command
     */
    String getName();

    /**
     * @return the description of the command to be printed in help messages
     */
    String getDescription();

    /**
     * @return the CLI options of the command
     */
    Options getOptions();

    /**
     * Prints the usage of this command.
     *
     * @param errorMessage an optional error message
     */
    void printUsage(String errorMessage);

    /**
     * Executes the command with the given CLI params.
     *
     * @param cli the parsed CLI for the command
     * @return the Result of the command
     */
    R execute(CommandLine cli) throws CommandException;

    /**
     * @return the implementation class of the result
     */
    Class<R> getResultImplType();

    /**
     * @return true if the type of result produced is considered Referenceable
     */
    default boolean isReferencable() {
        return Referenceable.class.isAssignableFrom(getResultImplType());
    }

}
