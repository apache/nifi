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

package org.apache.nifi.components.connector;

import java.util.Objects;

public class StandardConnectorAction implements ConnectorAction {

    private final String name;
    private final String description;
    private final boolean allowed;
    private final String reasonNotAllowed;

    public StandardConnectorAction(final String name, final String description, final boolean allowed, final String reasonNotAllowed) {
        this.name = Objects.requireNonNull(name, "name is required");
        this.description = Objects.requireNonNull(description, "description is required");
        this.allowed = allowed;
        this.reasonNotAllowed = reasonNotAllowed;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public boolean isAllowed() {
        return allowed;
    }

    @Override
    public String getReasonNotAllowed() {
        return reasonNotAllowed;
    }

    @Override
    public boolean equals(final Object other) {
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        final StandardConnectorAction that = (StandardConnectorAction) other;
        return allowed == that.allowed
            && Objects.equals(name, that.name)
            && Objects.equals(description, that.description)
            && Objects.equals(reasonNotAllowed, that.reasonNotAllowed);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, description, allowed, reasonNotAllowed);
    }

    @Override
    public String toString() {
        if (reasonNotAllowed == null) {
            return "StandardConnectorAction[name=" + name + ", allowed=" + allowed + "]";
        }
        return "StandardConnectorAction[name=" + name + ", allowed=" + allowed + ", reason=" + reasonNotAllowed + "]";
    }
}

