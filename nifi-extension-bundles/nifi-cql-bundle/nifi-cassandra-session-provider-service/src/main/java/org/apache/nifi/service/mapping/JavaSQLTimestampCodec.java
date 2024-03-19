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

package org.apache.nifi.service.mapping;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.Instant;

public class JavaSQLTimestampCodec implements TypeCodec<Timestamp> {
    private final TypeCodec<Instant> instantCodec;

    public JavaSQLTimestampCodec() {
        this.instantCodec = TypeCodecs.TIMESTAMP;
    }

    @Override
    public GenericType<Timestamp> getJavaType() {
        return GenericType.of(Timestamp.class);
    }

    @Override
    public DataType getCqlType() {
        return instantCodec.getCqlType(); // maps to CQL `timestamp`
    }

    @Override
    public ByteBuffer encode(Timestamp value, ProtocolVersion protocolVersion) {
        return value == null ? null : instantCodec.encode(value.toInstant(), protocolVersion);
    }

    @Override
    public Timestamp decode(ByteBuffer bytes, ProtocolVersion protocolVersion) {
        Instant instant = instantCodec.decode(bytes, protocolVersion);
        return instant == null ? null : Timestamp.from(instant);
    }

    @Override
    public String format(Timestamp value) {
        return value == null ? "NULL" : instantCodec.format(value.toInstant());
    }

    @Override
    public Timestamp parse(String value) {
        Instant instant = instantCodec.parse(value);
        return instant == null ? null : Timestamp.from(instant);
    }
}
