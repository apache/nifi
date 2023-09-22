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

import java.util.Objects;

import org.apache.nifi.toolkit.cli.impl.command.CommandOption;

/**
 * Represents a resolved back-reference produced by a ReferenceResolver.
 */
public class ResolvedReference {

    private final CommandOption option;

    private final Integer position;

    private final String displayName;

    private final String resolvedValue;

    public ResolvedReference(
            final CommandOption option,
            final Integer position,
            final String displayName,
            final String resolvedValue) {
        this.option = option;
        this.position = Objects.requireNonNull(position);
        this.displayName = Objects.requireNonNull(displayName);
        this.resolvedValue = Objects.requireNonNull(resolvedValue);
    }

    public CommandOption getOption() {
        return option;
    }

    public Integer getPosition() {
        return position;
    }

    public String getDisplayName() {
        return displayName;
    }

    public String getResolvedValue() {
        return resolvedValue;
    }
}
