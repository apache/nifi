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

public class StandardMessageName implements MessageName {

    private final Optional<String> namespace;
    private final String name;

    public StandardMessageName(final Optional<String> namespace, final String name) {
        this.name = Objects.requireNonNull(name, "name must not be null");
        this.namespace = namespace;
    }

    public StandardMessageName(final String name) {
        Objects.requireNonNull(name, "name must not be null");
        final int lastDotIndex = name.lastIndexOf('.');
        if (lastDotIndex > 0) {
            this.namespace = Optional.of(name.substring(0, lastDotIndex));
            this.name = name.substring(lastDotIndex + 1);
        } else {
            this.namespace = Optional.empty();
            this.name = name;
        }
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Optional<String> getNamespace() {
        return namespace;
    }

    @Override
    public final boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof final StandardMessageName that)) {
            return false;
        }

        return name.equals(that.name) && namespace.equals(that.namespace);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + namespace.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return String.format("StandardMessageName{name='%s', namespace='%s'}", name, namespace);
    }
}
