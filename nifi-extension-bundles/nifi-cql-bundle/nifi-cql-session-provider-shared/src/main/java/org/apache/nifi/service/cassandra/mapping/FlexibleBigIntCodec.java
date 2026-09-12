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
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import java.nio.ByteBuffer;

/**
 * A fallback CQL {@code bigint} codec that accepts any long-compatible value (a numeric String, a
 * differently-sized {@link Number}), not just an exact {@link Long}. Declared against {@link Object} so
 * the registry treats it as a fallback behind the built-in codec rather than rejecting it as a duplicate.
 */
public class FlexibleBigIntCodec implements TypeCodec<Object> {
    private final TypeCodec<Long> bigIntCodec;

    public FlexibleBigIntCodec() {
        this.bigIntCodec = TypeCodecs.BIGINT;
    }

    @Override
    public GenericType<Object> getJavaType() {
        return GenericType.of(Object.class);
    }

    @Override
    public DataType getCqlType() {
        return bigIntCodec.getCqlType(); // maps to CQL `bigint`
    }

    @Override
    public ByteBuffer encode(Object value, ProtocolVersion protocolVersion) {
        return value == null ? null : bigIntCodec.encode(DataTypeUtils.toLong(value, getCqlType().toString()), protocolVersion);
    }

    @Override
    public Object decode(ByteBuffer bytes, ProtocolVersion protocolVersion) {
        return bigIntCodec.decode(bytes, protocolVersion);
    }

    @Override
    public String format(Object value) {
        return value == null ? "NULL" : bigIntCodec.format(DataTypeUtils.toLong(value, getCqlType().toString()));
    }

    @Override
    public Object parse(String value) {
        return bigIntCodec.parse(value);
    }
}
