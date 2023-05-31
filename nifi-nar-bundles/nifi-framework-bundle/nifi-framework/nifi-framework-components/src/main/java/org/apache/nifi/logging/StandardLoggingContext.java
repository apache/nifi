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

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

public class StandardLoggingContext<T extends PerProcessGroupLoggable> implements LoggingContext {
    private static final String KEY = "logFileSuffix";
    private final AtomicReference<T> component = new AtomicReference<>();

    public StandardLoggingContext(final T component) {
        this.component.set(component);
    }

    @Override
    public Optional<String> getLogFileSuffix() {
        final T componentNode = component.get();
        if (componentNode != null) {
            return getSuffix(componentNode.getProcessGroup());
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
        } else if (group.isLogToOwnFile()) {
            return Optional.of(group.getLogFileSuffix());
        } else if (group.isRootGroup()) {
            return Optional.empty();
        } else {
            return getSuffix(group.getParent());
        }
    }

    public void setComponent(final T component) {
        this.component.set(component);
    }
}
