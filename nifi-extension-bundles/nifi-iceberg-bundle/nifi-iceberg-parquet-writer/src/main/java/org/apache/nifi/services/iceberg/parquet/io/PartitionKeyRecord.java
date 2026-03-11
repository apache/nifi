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
package org.apache.nifi.services.iceberg.parquet.io;

import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.function.Function;

/**
 * Partition Key Record Wrapper based on Apache Iceberg InternalRecordWrapper to avoid iceberg-data dependency
 */
class PartitionKeyRecord implements StructLike {
    private final Function<Object, Object>[] converters;

    private StructLike wrapped = null;

    @SuppressWarnings("unchecked")
    PartitionKeyRecord(final Types.StructType structType) {
        final List<Types.NestedField> fields = structType.fields();

        converters = fields.stream()
                .map(Types.NestedField::type)
                .map(PartitionKeyRecord::getConverter)
                .toArray(length -> (Function<Object, Object>[]) Array.newInstance(Function.class, length));
    }

    @Override
    public int size() {
        return wrapped.size();
    }

    @Override
    public <T> T get(final int position, final Class<T> javaClass) {
        final T processed;

        final Function<Object, Object> converter = converters[position];
        if (converter == null) {
            processed = wrapped.get(position, javaClass);
        } else {
            final Object value = wrapped.get(position, Object.class);
            if (value == null) {
                processed = null;
            } else {
                final Object converted = converter.apply(value);
                processed = javaClass.cast(converted);
            }
        }

        return processed;
    }

    @Override
    public <T> void set(final int position, final T value) {
        throw new UnsupportedOperationException("Set method not supported");
    }

    PartitionKeyRecord wrap(final StructLike wrapped) {
        this.wrapped = wrapped;
        return this;
    }

    private static Function<Object, Object> getConverter(final Type fieldType) {
        final Type.TypeID typeId = fieldType.typeId();

        final Function<Object, Object> converter;

        if (Type.TypeID.TIMESTAMP_NANO == typeId) {
            final Types.TimestampNanoType timestampNanoType = (Types.TimestampNanoType) fieldType;
            if (timestampNanoType.shouldAdjustToUTC()) {
                converter = dateTime -> DateTimeUtil.nanosFromTimestamptz((OffsetDateTime) dateTime);
            } else {
                converter = dateTime -> DateTimeUtil.nanosFromTimestamp((LocalDateTime) dateTime);
            }
        } else if (Type.TypeID.TIMESTAMP == typeId) {
            final Types.TimestampType timestampType = (Types.TimestampType) fieldType;
            if (timestampType.shouldAdjustToUTC()) {
                converter = dateTime -> DateTimeUtil.nanosFromTimestamptz((OffsetDateTime) dateTime);
            } else {
                converter = dateTime -> DateTimeUtil.nanosFromTimestamp((LocalDateTime) dateTime);
            }
        } else if (Type.TypeID.DATE == typeId) {
            converter = date -> DateTimeUtil.daysFromDate((LocalDate) date);
        } else if (Type.TypeID.TIME == typeId) {
            converter = time -> DateTimeUtil.microsFromTime((LocalTime) time);
        } else if (Type.TypeID.FIXED == typeId) {
            converter = bytes -> ByteBuffer.wrap((byte[]) bytes);
        } else if (Type.TypeID.STRUCT == typeId) {
            final Types.StructType fieldStructType = fieldType.asStructType();
            final PartitionKeyRecord partitionKeyRecord = new PartitionKeyRecord(fieldStructType);
            converter = struct -> partitionKeyRecord.wrap((StructLike) struct);
        } else {
            converter = null;
        }

        return converter;
    }
}
