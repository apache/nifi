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
package org.apache.nifi.services.protobuf.converter;

import com.squareup.wire.schema.Field;
import com.squareup.wire.schema.ProtoType;

public class ProtoField {

    private final String fieldName;
    private final ProtoType protoType;
    private final boolean repeatable;

    public ProtoField(Field field) {
        this(field.getName(), field.getType(), field.isRepeated());
    }

    public ProtoField(String fieldName, ProtoType protoType) {
        this(fieldName, protoType, false);
    }

    private ProtoField(String fieldName, ProtoType protoType, boolean repeatable) {
        this.fieldName = fieldName;
        this.protoType = protoType;
        this.repeatable = repeatable;
    }

    public String getFieldName() {
        return fieldName;
    }

    public ProtoType getProtoType() {
        return protoType;
    }

    public boolean isRepeatable() {
        return repeatable;
    }
}
