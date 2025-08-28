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
package org.apache.nifi.schemaregistry.services;

import java.util.Objects;
import java.util.Optional;

/**
 * Factory class for creating StandardMessageName instances from various input formats.
 */
public final class StandardMessageNameFactory {
    /**
     * Parses a fully qualified message name that may include a package namespace.
     * If the name contains a dot (.), everything before the last dot is treated as the namespace,
     * and everything after the last dot is treated as the message name.
     *
     * @param fullMessageName the fully qualified message name (e.g., "mypackage.MyMessage" or "MyMessage")
     * @return a new StandardMessageName instance with parsed namespace and name
     * @throws NullPointerException if fullMessageName is null
     */
    public static StandardMessageName fromName(final String fullMessageName) {
        Objects.requireNonNull(fullMessageName, "fullMessageName must not be null");
        final int lastDotIndex = fullMessageName.lastIndexOf('.');
        if (lastDotIndex > 0) {
            final String namespace = fullMessageName.substring(0, lastDotIndex);
            final String name = fullMessageName.substring(lastDotIndex + 1);
            return new StandardMessageName(Optional.of(namespace), name);
        } else {
            return new StandardMessageName(Optional.empty(), fullMessageName);
        }
    }
}
