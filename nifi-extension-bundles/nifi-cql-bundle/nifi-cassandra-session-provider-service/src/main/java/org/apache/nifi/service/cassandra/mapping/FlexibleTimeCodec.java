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
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import java.nio.ByteBuffer;
import java.sql.Time;
import java.time.LocalTime;
import java.util.Optional;

/**
 * A fallback CQL {@code time} codec that accepts any time-compatible value {@code DataTypeUtils} recognizes,
 * including a {@link String} parsed with the fixed {@code HH:mm:ss} pattern (what a RecordPath {@code format()}
 * primary key override produces). Declared against {@link Object} so the registry treats it as a fallback
 * behind the built-in {@link LocalTime} codec rather than rejecting it as a duplicate.
 */
public class FlexibleTimeCodec implements TypeCodec<Object> {
    private static final String TIME_PATTERN = "HH:mm:ss";

    private final TypeCodec<LocalTime> timeCodec;

    public FlexibleTimeCodec() {
        this.timeCodec = TypeCodecs.TIME;
    }

    @Override
    public GenericType<Object> getJavaType() {
        return GenericType.of(Object.class);
    }

    @Override
    public DataType getCqlType() {
        return timeCodec.getCqlType(); // maps to CQL `time`
    }

    @Override
    public ByteBuffer encode(Object value, ProtocolVersion protocolVersion) {
        return value == null ? null : timeCodec.encode(toLocalTime(value), protocolVersion);
    }

    @Override
    public Object decode(ByteBuffer bytes, ProtocolVersion protocolVersion) {
        return timeCodec.decode(bytes, protocolVersion);
    }

    @Override
    public String format(Object value) {
        return value == null ? "NULL" : timeCodec.format(toLocalTime(value));
    }

    @Override
    public Object parse(String value) {
        return timeCodec.parse(value);
    }

    private static LocalTime toLocalTime(final Object value) {
        final Object converted = DataTypeUtils.convertType(value, RecordFieldType.TIME.getDataType(),
                Optional.empty(), Optional.of(TIME_PATTERN), Optional.empty(), "value");
        return ((Time) converted).toLocalTime();
    }
}
