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

package org.apache.nifi.serialization.record;

import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

public class StandardSchemaIdentifier implements SchemaIdentifier {
    private final Optional<String> name;
    private final OptionalLong identifier;
    private final OptionalInt version;

    StandardSchemaIdentifier(final String name, final Long identifier, final Integer version) {
        this.name = Optional.ofNullable(name);
        this.identifier = identifier == null ? OptionalLong.empty() : OptionalLong.of(identifier);;
        this.version = version == null ? OptionalInt.empty() : OptionalInt.of(version);;
    }

    @Override
    public Optional<String> getName() {
        return name;
    }

    @Override
    public OptionalLong getIdentifier() {
        return identifier;
    }

    @Override
    public OptionalInt getVersion() {
        return version;
    }

    @Override
    public int hashCode() {
        return 31 + 41 * getName().hashCode() + 41 * getIdentifier().hashCode() + 41 * getVersion().hashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof SchemaIdentifier)) {
            return false;
        }
        final SchemaIdentifier other = (SchemaIdentifier) obj;
        return getName().equals(other.getName()) && getIdentifier().equals(other.getIdentifier()) && getVersion().equals(other.getVersion());
    }
}
