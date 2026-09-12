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

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.internal.core.type.DefaultTupleType;
import com.datastax.oss.driver.internal.core.type.UserDefinedTypeBuilder;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The Avro schema {@link CassandraUdtSchemaMapper} declares for a column, resolved through
 * {@link AvroTypeUtil#determineDataType} into the {@link RecordFieldType} a reader will actually see.
 *
 * <p>The service places the driver's own decoded value into the record unchanged, so these tests double as a
 * guard on the pairing between declared type and runtime value. Only UUID/TIMEUUID get a native record type:
 * the driver decodes those to {@code java.util.UUID}, which the native UUID type accepts directly. The
 * temporal and arbitrary-precision types stay STRING deliberately - see
 * {@link #timestampStaysStringBecauseTheNativeTypeWouldRejectTheDriverValue()} and
 * {@link #timeStaysStringBecauseTheNativeTypeWouldTruncateNanoseconds()} for the two concrete reasons, both
 * of which are worse than the declared/runtime type difference they would be fixing.
 *
 * <p>A CQL type with no direct Avro equivalent (DURATION, TUPLE, VECTOR, ...) must still produce a schema -
 * not throw and fail the whole query over one exotic column.
 */
public class CassandraUdtSchemaMapperTest {

    private RecordFieldType resolvedFieldType(final DataType cqlType) {
        final Schema avroSchema = CassandraUdtSchemaMapper.toAvroSchema(cqlType, new HashMap<>());
        return AvroTypeUtil.determineDataType(avroSchema).getFieldType();
    }

    /**
     * The native TIMESTAMP record type is backed by {@code java.sql.Timestamp}, and NiFi's own field
     * converter rejects the {@code java.time.Instant} the driver decodes a CQL timestamp into. Declaring the
     * native type here would turn a working, lossless string coercion into a hard conversion failure.
     */
    @Test
    @DisplayName("TIMESTAMP resolves to the STRING record type, which accepts the driver's Instant")
    public void timestampStaysStringBecauseTheNativeTypeWouldRejectTheDriverValue() {
        assertEquals(RecordFieldType.STRING, resolvedFieldType(DataTypes.TIMESTAMP));

        final Instant driverValue = Instant.parse("2026-08-08T19:15:09.621Z");
        assertTrue(DataTypeUtils.isCompatibleDataType(driverValue, RecordFieldType.STRING.getDataType()),
                "a STRING field must accept the Instant the driver decodes a CQL timestamp into");
        assertFalse(DataTypeUtils.isCompatibleDataType(driverValue, RecordFieldType.TIMESTAMP.getDataType()),
                "if the native TIMESTAMP type ever starts accepting Instant, revisit declaring it here");
    }

    @Test
    @DisplayName("DATE resolves to the STRING record type, which accepts the driver's LocalDate")
    public void testDateStaysString() {
        assertEquals(RecordFieldType.STRING, resolvedFieldType(DataTypes.DATE));
        assertTrue(DataTypeUtils.isCompatibleDataType(LocalDate.of(2024, 3, 15), RecordFieldType.STRING.getDataType()));
    }

    /**
     * CQL {@code time} is nanosecond-resolution and {@code java.sql.Time} - what the native TIME record type
     * is backed by - is not. Declaring the native type, or converting the value to suit it, silently
     * truncates. The STRING coercion keeps every digit.
     */
    @Test
    @DisplayName("TIME resolves to the STRING record type, which preserves nanosecond precision")
    public void timeStaysStringBecauseTheNativeTypeWouldTruncateNanoseconds() {
        assertEquals(RecordFieldType.STRING, resolvedFieldType(DataTypes.TIME));

        final LocalTime nanoPrecision = LocalTime.of(15, 14, 54, 899_676_065);
        final Object asString = DataTypeUtils.convertType(
                nanoPrecision, RecordFieldType.STRING.getDataType(), Optional.empty(), Optional.empty(), Optional.empty(), "value_field");
        assertEquals("15:14:54.899676065", asString, "the STRING coercion must not drop sub-second precision");

        final Object asNativeTime = DataTypeUtils.convertType(
                nanoPrecision, RecordFieldType.TIME.getDataType(), Optional.empty(), Optional.empty(), Optional.empty(), "value_field");
        assertEquals("15:14:54", asNativeTime.toString(),
                "documents why the native TIME type is not used - it truncates to whole seconds");
    }

    @Test
    @DisplayName("UUID resolves to the native UUID record type")
    public void testUuidResolvesToUuidType() {
        assertEquals(RecordFieldType.UUID, resolvedFieldType(DataTypes.UUID));
    }

    @Test
    @DisplayName("TIMEUUID resolves to the native UUID record type")
    public void testTimeUuidResolvesToUuidType() {
        assertEquals(RecordFieldType.UUID, resolvedFieldType(DataTypes.TIMEUUID));
    }

    @Test
    @DisplayName("DECIMAL resolves to the STRING record type, not a fixed-precision decimal type")
    public void testDecimalStaysString() {
        assertEquals(RecordFieldType.STRING, resolvedFieldType(DataTypes.DECIMAL));
    }

    @Test
    @DisplayName("VARINT resolves to the STRING record type")
    public void testVarintStaysString() {
        assertEquals(RecordFieldType.STRING, resolvedFieldType(DataTypes.VARINT));
    }

    @Test
    @DisplayName("INET resolves to the STRING record type")
    public void testInetStaysString() {
        assertEquals(RecordFieldType.STRING, resolvedFieldType(DataTypes.INET));
    }

    @Test
    @DisplayName("A CQL type with no Avro equivalent (DURATION) still produces a schema instead of throwing")
    public void testDurationDoesNotThrow() {
        final RecordFieldType fieldType = assertDoesNotThrow(() -> resolvedFieldType(DataTypes.DURATION));
        assertEquals(RecordFieldType.STRING, fieldType);
    }

    @Test
    @DisplayName("A compound CQL type with no Avro equivalent (TUPLE) still produces a schema instead of throwing")
    public void testTupleDoesNotThrow() {
        final DataType tupleType = new DefaultTupleType(List.of(DataTypes.INT, DataTypes.TEXT));
        final RecordFieldType fieldType = assertDoesNotThrow(() -> resolvedFieldType(tupleType));
        assertEquals(RecordFieldType.STRING, fieldType);
    }

    @Test
    @DisplayName("A UDT field resolves to the same record type a top-level column of that CQL type would")
    public void testUdtFieldResolvesTheSameWayAsATopLevelColumn() {
        final DataType udtType = new UserDefinedTypeBuilder("ks", "event")
                .withField("event_id", DataTypes.UUID)
                .withField("occurred_at", DataTypes.TIMESTAMP)
                .build();

        final Schema avroSchema = CassandraUdtSchemaMapper.toAvroSchema(udtType, new HashMap<>());
        final org.apache.nifi.serialization.record.DataType recordDataType = AvroTypeUtil.determineDataType(avroSchema);

        assertEquals(RecordFieldType.RECORD, recordDataType.getFieldType());
        final RecordSchema nested = ((RecordDataType) recordDataType).getChildSchema();
        assertEquals(RecordFieldType.UUID, nested.getField("event_id").orElseThrow().getDataType().getFieldType());
        assertEquals(RecordFieldType.STRING, nested.getField("occurred_at").orElseThrow().getDataType().getFieldType());
    }

    @Test
    @DisplayName("The uuid logical type is attached, matching how AvroTypeUtil's own Record-to-Avro writer declares a UUID field")
    public void testUuidLogicalTypeMatchesAvroTypeUtilConvention() {
        final Schema avroSchema = CassandraUdtSchemaMapper.toAvroSchema(DataTypes.UUID, new HashMap<>());
        final Schema nonNullBranch = avroSchema.getTypes().stream()
                .filter(s -> s.getType() != Schema.Type.NULL)
                .findFirst().orElseThrow();
        assertEquals(LogicalTypes.uuid().getName(), nonNullBranch.getLogicalType().getName());
    }
}
