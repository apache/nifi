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
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;

import java.nio.ByteBuffer;

public class FlexibleCounterCodec implements TypeCodec<Number> {

    private final TypeCodec<Long> inner = TypeCodecs.BIGINT;

    @Override
    public GenericType<Number> getJavaType() {
        return GenericType.of(Number.class);
    }

    @Override
    public DataType getCqlType() {
        return DataTypes.COUNTER;
    }

    @Override
    public ByteBuffer encode(Number value, ProtocolVersion protocolVersion) {
        if (value == null) return null;
        return inner.encode(value.longValue(), protocolVersion);
    }

    @Override
    public Number decode(ByteBuffer bytes, ProtocolVersion protocolVersion) {
        Long val = inner.decode(bytes, protocolVersion);
        return val; // returns as Long, but you can cast if needed
    }

    @Override
    public String format(Number value) {
        return inner.format(value == null ? null : value.longValue());
    }

    @Override
    public Number parse(String value) {
        return inner.parse(value);
    }
}

