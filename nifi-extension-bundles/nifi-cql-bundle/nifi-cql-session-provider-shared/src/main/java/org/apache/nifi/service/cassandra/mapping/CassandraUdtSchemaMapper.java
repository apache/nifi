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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import java.util.List;
import java.util.Map;

import static com.datastax.oss.driver.api.core.type.DataTypes.ASCII;

/**
 * Recursively converts a Cassandra CQL {@link DataType} into a nullable-union Avro {@link Schema}, mapping
 * User Defined Types (including nested ones and those held in a list/set/map) to nested named Avro records.
 * Any CQL type with no Avro equivalent ({@code DURATION}, {@code TUPLE}, {@code VECTOR}, custom types) falls
 * back to a plain string so one exotic column never fails an entire query.
 */
public final class CassandraUdtSchemaMapper {

    private CassandraUdtSchemaMapper() {
    }

    /**
     * Converts the given CQL type into a nullable Avro union schema.
     *
     * @param cqlType the CQL type to convert
     * @param udtSchemaCache UDT schemas already built in this conversion, keyed by "keyspace.typeName", so a
     *                        UDT referenced more than once reuses one {@link Schema} instance (Avro rejects
     *                        duplicate record names). Pass a fresh, empty map per top-level call.
     */
    public static Schema toAvroSchema(final DataType cqlType, final Map<String, Schema> udtSchemaCache) {
        if (cqlType instanceof UserDefinedType udtType) {
            return nullableUnion(toUdtRecordSchema(udtType, udtSchemaCache));
        }

        if (cqlType instanceof ListType listType) {
            return nullableUnion(SchemaBuilder.array().items(toAvroSchema(listType.getElementType(), udtSchemaCache)));
        }

        if (cqlType instanceof SetType setType) {
            return nullableUnion(SchemaBuilder.array().items(toAvroSchema(setType.getElementType(), udtSchemaCache)));
        }

        if (cqlType instanceof MapType mapType) {
            return nullableUnion(SchemaBuilder.map().values(toAvroSchema(mapType.getValueType(), udtSchemaCache)));
        }

        return nullableUnion(toPrimitiveAvroSchema(cqlType));
    }

    private static Schema toUdtRecordSchema(final UserDefinedType udtType, final Map<String, Schema> udtSchemaCache) {
        final String namespace = udtType.getKeyspace() == null ? "cassandra" : udtType.getKeyspace().asInternal();
        final String schemaKey = namespace + "." + udtType.getName().asInternal();

        final Schema cached = udtSchemaCache.get(schemaKey);
        if (cached != null) {
            return cached;
        }

        SchemaBuilder.FieldAssembler<Schema> fields = SchemaBuilder.record(udtType.getName().asInternal())
                .namespace(namespace)
                .fields();

        final List<CqlIdentifier> fieldNames = udtType.getFieldNames();
        final List<DataType> fieldTypes = udtType.getFieldTypes();

        for (int i = 0; i < fieldNames.size(); i++) {
            fields = fields.name(fieldNames.get(i).asInternal()).type(toAvroSchema(fieldTypes.get(i), udtSchemaCache)).noDefault();
        }

        final Schema schema = fields.endRecord();
        udtSchemaCache.put(schemaKey, schema);
        return schema;
    }

    private static Schema nullableUnion(final Schema schema) {
        return SchemaBuilder.builder().unionOf().nullBuilder().endNull().and().type(schema).endUnion();
    }

    private static Schema toPrimitiveAvroSchema(final DataType cqlType) {
        final SchemaBuilder.TypeBuilder<Schema> typeBuilder = SchemaBuilder.builder();

        if (cqlType.equals(ASCII) || cqlType.equals(DataTypes.TEXT)) {
            return typeBuilder.stringType();
        }

        // Declared with the Avro `uuid` logical type, so AvroTypeUtil#determineDataType resolves the record
        // field to the native UUID type rather than STRING - matching the java.util.UUID the driver's own
        // codec decodes the column into, with no value-side conversion needed on either side.
        if (cqlType.equals(DataTypes.UUID) || cqlType.equals(DataTypes.TIMEUUID)) {
            final Schema schema = Schema.create(Schema.Type.STRING);
            LogicalTypes.uuid().addToSchema(schema);
            return schema;
        }

        // TIMESTAMP/DATE/TIME, DECIMAL/VARINT and INET are all declared as a plain Avro string, and the
        // driver's own decoded value (Instant/LocalDate/LocalTime, BigDecimal/BigInteger, InetAddress) is
        // placed into the record unchanged. The declared type and the runtime class differ, but that is a
        // supported combination rather than a defect: RecordFieldType.STRING is a *widening* type over
        // DATE/TIME/TIMESTAMP/DECIMAL/BIGINT among others, so DataTypeUtils accepts each of these values for
        // a string field and coerces them losslessly (a nanosecond LocalTime renders in full, for instance)
        // if and when a downstream writer asks for a string.
        //
        // Declaring the native NiFi types instead is not an option here:
        //   - TIMESTAMP/DATE/TIME are backed by java.sql.*, not java.time.*, so pairing them with the
        //     driver's values makes DataTypeUtils#convertType throw for TIMESTAMP and silently truncate a
        //     nanosecond TIME to whole seconds. Converting the values to java.sql.* to suit instead loses
        //     that same sub-second precision, since CQL `time` is nanosecond-resolution and java.sql.Time is
        //     not.
        //   - DECIMAL would need Avro's fixed precision/scale, which CQL's arbitrary-precision `decimal`
        //     cannot supply up front, and a native BIGINT is unreachable through the Avro round trip at all
        //     (AvroTypeUtil maps record BIGINT to Avro string, which resolves back to record STRING).
        //   - INET has no native NiFi record type in the first place.
        if (cqlType.equals(DataTypes.TIMESTAMP)
                || cqlType.equals(DataTypes.DATE)
                || cqlType.equals(DataTypes.TIME)
                || cqlType.equals(DataTypes.INET)
                || cqlType.equals(DataTypes.VARINT)
                || cqlType.equals(DataTypes.DECIMAL)) {
            return typeBuilder.stringType();
        }

        if (cqlType.equals(DataTypes.BOOLEAN)) {
            return typeBuilder.booleanType();
        }

        if (cqlType.equals(DataTypes.INT)
                // Avro has no byte/short primitive, so these widen to Avro's int
                || cqlType.equals(DataTypes.TINYINT)
                || cqlType.equals(DataTypes.SMALLINT)) {
            return typeBuilder.intType();
        }

        if (cqlType.equals(DataTypes.BIGINT) || cqlType.equals(DataTypes.COUNTER)) {
            return typeBuilder.longType();
        }

        if (cqlType.equals(DataTypes.FLOAT)) {
            return typeBuilder.floatType();
        }

        if (cqlType.equals(DataTypes.DOUBLE)) {
            return typeBuilder.doubleType();
        }

        if (cqlType.equals(DataTypes.BLOB)) {
            return typeBuilder.bytesType();
        }

        // A CQL type with no case above - DURATION, TUPLE, VECTOR, and any future or custom type - is declared
        // as STRING rather than failing schema generation. This used to throw IllegalArgumentException here,
        // which failed the entire query over one exotic column; CassandraCQLExecutionService#toRecordValue
        // converts the value to match via toString(), the same fallback DECIMAL/VARINT/INET already use.
        return typeBuilder.stringType();
    }
}
