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
package org.apache.nifi.logging;

import org.apache.nifi.groups.ProcessGroup;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class StandardLoggingContext implements LoggingContext {
    private static final String KEY = "logFileSuffix";

    private volatile GroupedComponent component;

    /**
     * Standard Logging Context default constructor for separate component initialization
     *
     */
    public StandardLoggingContext() {
        this.component = null;
    }

    /**
     * Standard Logging Context constructor with Grouped Component required
     *
     * @param component Grouped Component required
     */
    public StandardLoggingContext(final GroupedComponent component) {
        this.component = Objects.requireNonNull(component, "Group Component required");
    }

    /**
     * Get Attributes from Component Process Group
     *
     * @return Attributes from Process Group or empty when Component or Process Group not found
     */
    @Override
    public Map<String, String> getAttributes() {
        final Map<String, String> attributes;

        if (component == null) {
            attributes = Map.of();
        } else {
            final ProcessGroup processGroup = component.getProcessGroup();
            if (processGroup == null) {
                attributes = Map.of();
            } else {
                attributes = processGroup.getLoggingAttributes();
            }
        }

        return attributes;
    }

    @Override
    public Optional<String> getLogFileSuffix() {
        if (component != null) {
            return getSuffix(component.getProcessGroup());
        } else {
            return Optional.empty();
        }
    }

    @Override
    public String getDiscriminatorKey() {
        return KEY;
    }

    private Optional<String> getSuffix(final ProcessGroup group) {
        if (group == null) {
            return Optional.empty();
        } else if (group.getLogFileSuffix() != null && !group.getLogFileSuffix().isEmpty()) {
            return Optional.of(group.getLogFileSuffix());
        } else if (group.isRootGroup()) {
            return Optional.empty();
        } else {
            return getSuffix(group.getParent());
        }
    }

    public void setComponent(final GroupedComponent component) {
        this.component = component;
    }
}
