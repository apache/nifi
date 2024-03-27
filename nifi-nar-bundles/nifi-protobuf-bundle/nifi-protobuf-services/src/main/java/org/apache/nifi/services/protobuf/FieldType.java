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
package org.apache.nifi.services.protobuf;

import java.util.Arrays;

/**
 * Type for proto scalar fields.
 */
public enum FieldType {
    DOUBLE("double"),
    FLOAT("float"),
    INT32("int32"),
    INT64("int64"),
    UINT32("uint32"),
    UINT64("uint64"),
    SINT32("sint32"),
    SINT64("sint64"),
    FIXED32("fixed32"),
    FIXED64("fixed64"),
    SFIXED32("sfixed32"),
    SFIXED64("sfixed64"),
    BOOL("bool"),
    STRING("string"),
    BYTES("bytes");

    private final String type;

    FieldType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public static FieldType findValue(final String value) {
        return Arrays.stream(FieldType.values())
                .filter((type -> type.getType().equalsIgnoreCase(value)))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(String.format("ProtoType [%s] not found", value)));
    }
}
