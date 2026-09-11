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

package org.apache.nifi.service.cassandra.mapping;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;

import java.nio.ByteBuffer;

/**
 * A CQL {@code text} codec that binds a {@link Character} (what {@code DataTypeUtils} produces for a
 * CHAR-typed record field) as a one-character string, since Cassandra has no single-character type.
 */
public class CharacterCodec implements TypeCodec<Character> {
    private final TypeCodec<String> stringCodec;

    public CharacterCodec() {
        this.stringCodec = TypeCodecs.TEXT;
    }

    @Override
    public GenericType<Character> getJavaType() {
        return GenericType.of(Character.class);
    }

    @Override
    public DataType getCqlType() {
        return stringCodec.getCqlType(); // maps to CQL `text`
    }

    @Override
    public ByteBuffer encode(Character value, ProtocolVersion protocolVersion) {
        return value == null ? null : stringCodec.encode(value.toString(), protocolVersion);
    }

    @Override
    public Character decode(ByteBuffer bytes, ProtocolVersion protocolVersion) {
        final String string = stringCodec.decode(bytes, protocolVersion);
        return (string == null || string.isEmpty()) ? null : string.charAt(0);
    }

    @Override
    public String format(Character value) {
        return value == null ? "NULL" : stringCodec.format(value.toString());
    }

    @Override
    public Character parse(String value) {
        final String string = stringCodec.parse(value);
        return (string == null || string.isEmpty()) ? null : string.charAt(0);
    }
}
