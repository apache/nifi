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

import org.apache.nifi.toolkit.cli.impl.command.CommandOption;

/**
 * An object that is capable of resolving a positional reference to some value that corresponds with the reference.
 */
public interface ReferenceResolver {

    /**
     * Resolves the passed in positional reference to it's corresponding value.
     *
     * @param option the option that the reference is being resolved for, implementers should protect against a possible null option
     * @param position a position in this back reference
     * @return the resolved value for the given position
     */
    ResolvedReference resolve(CommandOption option, Integer position);

    /**
     * @return true if the there are no references to resolve, false otherwise
     */
    boolean isEmpty();

}
