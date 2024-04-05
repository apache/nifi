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
package org.apache.nifi.questdb.embedded;

import org.apache.nifi.questdb.rollover.RolloverStrategy;

import java.util.Objects;

public final class ManagedTableDefinition {
    private final String name;
    private final String definition;
    private final RolloverStrategy rolloverStrategy;

    public ManagedTableDefinition(final String name, final String definition, final RolloverStrategy rolloverStrategy) {
        this.name = name;
        this.definition = definition;
        this.rolloverStrategy = rolloverStrategy;
    }

    public String getName() {
        return name;
    }

    public String getDefinition() {
        return definition;
    }

    public RolloverStrategy getRolloverStrategy() {
        return rolloverStrategy;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final ManagedTableDefinition that = (ManagedTableDefinition) o;
        return Objects.equals(name, that.name) && Objects.equals(definition, that.definition) && Objects.equals(rolloverStrategy, that.rolloverStrategy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, definition, rolloverStrategy);
    }
}
