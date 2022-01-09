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
package org.apache.nifi.schema.access;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Constants related to Protocol Versions for Hortonworks Schema Registry.
 */
public class HortonworksProtocolVersions {

    /**
     * The minimum valid protocol version.
     */
    public static final int MIN_VERSION = 1;

    /**
     * The maximum valid protocol version.
     */
    public static final int MAX_VERSION = 3;

    /**
     * Map from protocol version to the required schema fields for the given version.
     */
    private static final Map<Integer, Set<SchemaField>> REQUIRED_SCHEMA_FIELDS_BY_PROTOCOL;
    static {
        final Map<Integer,Set<SchemaField>> requiredFieldsByProtocol = new HashMap<>();
        requiredFieldsByProtocol.put(1, EnumSet.of(SchemaField.SCHEMA_IDENTIFIER, SchemaField.SCHEMA_VERSION));
        requiredFieldsByProtocol.put(2, EnumSet.of(SchemaField.SCHEMA_VERSION_ID));
        requiredFieldsByProtocol.put(3, EnumSet.of(SchemaField.SCHEMA_VERSION_ID));
        REQUIRED_SCHEMA_FIELDS_BY_PROTOCOL = Collections.unmodifiableMap(requiredFieldsByProtocol);
    }

    public static Set<SchemaField> getRequiredSchemaFields(final Integer protocolVersion) {
        return REQUIRED_SCHEMA_FIELDS_BY_PROTOCOL.get(protocolVersion);
    }

}
