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
package org.apache.nifi.bootstrap.command.io;

import java.util.Arrays;
import java.util.Optional;

/**
 * Standard implementation of Bootstrap Argument Parser supporting enumerated arguments as the first element in an array
 */
public class StandardBootstrapArgumentParser implements BootstrapArgumentParser {
    private static final char HYPHEN = '-';

    private static final char UNDERSCORE = '_';

    /**
     * Get Bootstrap Argument from first argument provided
     *
     * @param arguments Application array of arguments
     * @return Bootstrap Argument or empty when not found
     */
    @Override
    public Optional<BootstrapArgument> getBootstrapArgument(final String[] arguments) {
        final Optional<BootstrapArgument> bootstrapArgumentFound;

        if (arguments == null || arguments.length == 0) {
            bootstrapArgumentFound = Optional.empty();
        } else {
            final String firstArgument = arguments[0];
            final String formattedArgument = getFormattedArgument(firstArgument);
            bootstrapArgumentFound = Arrays.stream(BootstrapArgument.values())
                    .filter(bootstrapArgument -> bootstrapArgument.name().equals(formattedArgument))
                    .findFirst();
        }

        return bootstrapArgumentFound;
    }

    private String getFormattedArgument(final String argument) {
        final String upperCased = argument.toUpperCase();
        return upperCased.replace(HYPHEN, UNDERSCORE);
    }
}
