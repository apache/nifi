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

public interface SchemaIdentifier {

    /**
     * @return the name of the schema, if one has been defined.
     */
    Optional<String> getName();

    /**
     * @return the identifier of the schema, if one has been defined.
     */
    OptionalLong getIdentifier();

    /**
     * @return the version of the schema, if one has been defined.
     */
    OptionalInt getVersion();


    public static SchemaIdentifier EMPTY = new StandardSchemaIdentifier(null, null, null);

    public static SchemaIdentifier ofName(final String name) {
        return new StandardSchemaIdentifier(name, null, null);
    }

    public static SchemaIdentifier of(final String name, final long identifier, final int version) {
        return new StandardSchemaIdentifier(name, identifier, version);
    }
}