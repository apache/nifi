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
package org.apache.nifi.processors.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.cassandra.CassandraSessionProviderService;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * AbstractCassandraProcessor is a base class for Cassandra processors and contains logic and variables common to most
 * processors integrating with Apache Cassandra.
 */
public abstract class AbstractCassandraProcessor extends AbstractProcessor {

    static final PropertyDescriptor CONNECTION_PROVIDER_SERVICE = new PropertyDescriptor.Builder()
            .name("Cassandra Connection Provider")
            .description("Specifies the Cassandra connection providing controller service to be used to connect to Cassandra cluster.")
            .required(true)
            .identifiesControllerService(CassandraSessionProviderService.class)
            .build();

    static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
            .name("Character Set")
            .description("Specifies the character set of the record data.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("UTF-8")
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile is transferred to this relationship if the operation completed successfully.")
            .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is transferred to this relationship if the operation failed.")
            .build();

    static final Relationship REL_RETRY = new Relationship.Builder()
            .name("retry")
            .description("A FlowFile is transferred to this relationship if the operation cannot be completed but attempting "
                    + "it again may succeed.")
            .build();

    protected static final List<PropertyDescriptor> COMMON_PROPERTY_DESCRIPTORS = List.of(
            CONNECTION_PROVIDER_SERVICE,
            CHARSET
    );

    protected final AtomicReference<CqlSession> cassandraSession = new AtomicReference<>(null);

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        Set<ValidationResult> results = new HashSet<>();

        if (!validationContext.getProperty(CONNECTION_PROVIDER_SERVICE).isSet()) {
            results.add(new ValidationResult.Builder()
                    .subject(CONNECTION_PROVIDER_SERVICE.getDisplayName())
                    .valid(false)
                    .explanation("Cassandra Connection Provider must be specified.")
                    .build());
        }

        return results;
    }

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        final CassandraSessionProviderService sessionProvider = context.getProperty(CONNECTION_PROVIDER_SERVICE)
                .asControllerService(CassandraSessionProviderService.class);
        cassandraSession.set(sessionProvider.getCassandraSession());
    }

    public void stop(ProcessContext context) {
        cassandraSession.set(null);
    }

    protected static Object getCassandraObject(Row row, int i) {
        DataType dataType = row.getColumnDefinitions().get(i).getType();

        try {
            if (dataType.equals(DataTypes.BLOB)) {
                return row.getByteBuffer(i);

            } else if (dataType.equals(DataTypes.VARINT) || dataType.equals(DataTypes.DECIMAL)) {
                Object obj = row.getObject(i);
                return obj != null ? obj.toString() : null;

            } else if (dataType.equals(DataTypes.BOOLEAN)) {
                return row.getBoolean(i);

            } else if (dataType.equals(DataTypes.INT)) {
                return row.getInt(i);

            } else if (dataType.equals(DataTypes.BIGINT) || dataType.equals(DataTypes.COUNTER)) {
                return row.getLong(i);

            } else if (dataType.equals(DataTypes.ASCII) || dataType.equals(DataTypes.TEXT)) {
                return row.getString(i);

            } else if (dataType.equals(DataTypes.FLOAT)) {
                return row.getFloat(i);

            } else if (dataType.equals(DataTypes.DOUBLE)) {
                return row.getDouble(i);

            } else if (dataType.equals(DataTypes.TIMESTAMP)) {
                Instant instant = row.getInstant(i);
                return instant != null ? instant.toString() : null;
            } else if (dataType.equals(DataTypes.DATE)) {
                LocalDate localDate = row.getLocalDate(i);
                return localDate != null ? localDate.toString() : null;
            } else if (dataType.equals(DataTypes.TIME)) {
                LocalTime time = row.getLocalTime(i);
                return time != null ? time.toString() : null;
            } else if (dataType instanceof ListType) {
                ListType listType = (ListType) dataType;
                Class<?> elementClass = getJavaClassForCassandraType(listType.getElementType());
                return row.getList(i, elementClass);

            } else if (dataType instanceof SetType) {
                SetType setType = (SetType) dataType;
                Class<?> elementClass = getJavaClassForCassandraType(setType.getElementType());
                return row.getSet(i, elementClass);

            } else if (dataType instanceof MapType) {
                MapType mapType = (MapType) dataType;
                Class<?> keyClass = getJavaClassForCassandraType(mapType.getKeyType());
                Class<?> valueClass = getJavaClassForCassandraType(mapType.getValueType());
                return row.getMap(i, keyClass, valueClass);
            }

            // Fallback for any other types
            Object value = row.getObject(i);
            return value != null ? value.toString() : null;

        } catch (Exception e) {
            return row.getObject(i) != null ? row.getObject(i).toString() : null;
        }
    }

    static Class<?> getJavaClassForCassandraType(DataType type) {
        if (type.equals(DataTypes.ASCII) || type.equals(DataTypes.TEXT)) {
            return String.class;
        } else if (type.equals(DataTypes.INT)) {
            return Integer.class;
        } else if (type.equals(DataTypes.BIGINT) || type.equals(DataTypes.COUNTER)) {
            return Long.class;
        } else if (type.equals(DataTypes.BOOLEAN)) {
            return Boolean.class;
        } else if (type.equals(DataTypes.FLOAT)) {
            return Float.class;
        } else if (type.equals(DataTypes.DOUBLE)) {
            return Double.class;
        } else if (type.equals(DataTypes.VARINT) || type.equals(DataTypes.DECIMAL)) {
            return String.class;
        } else if (type.equals(DataTypes.UUID) || type.equals(DataTypes.TIMEUUID)) {
            return java.util.UUID.class;
        } else if (type.equals(DataTypes.TIMESTAMP)) {
            return java.time.Instant.class;
        } else if (type.equals(DataTypes.DATE)) {
            return java.time.LocalDate.class;
        } else if (type.equals(DataTypes.TIME)) {
            return Long.class;
        } else if (type.equals(DataTypes.BLOB)) {
            return java.nio.ByteBuffer.class;
        } else {
            return Object.class;
        }
    }

    /**
     * This method will create a schema a union field consisting of null and the specified type.
     *
     * @param dataType The data type of the field
     */
    protected static Schema getUnionFieldType(String dataType) {
        return SchemaBuilder.builder().unionOf().nullBuilder().endNull().and().type(getSchemaForType(dataType)).endUnion();
    }

    /**
     * This method will create an Avro schema for the specified type.
     *
     * @param dataType The data type of the field
     */
    protected static Schema getSchemaForType(String dataType) {
        SchemaBuilder.TypeBuilder<Schema> typeBuilder = SchemaBuilder.builder();
        Schema returnSchema;
        switch (dataType) {
            case "string":
                returnSchema = typeBuilder.stringType();
                break;
            case "boolean":
                returnSchema = typeBuilder.booleanType();
                break;
            case "int":
                returnSchema = typeBuilder.intType();
                break;
            case "long":
                returnSchema = typeBuilder.longType();
                break;
            case "float":
                returnSchema = typeBuilder.floatType();
                break;
            case "double":
                returnSchema = typeBuilder.doubleType();
                break;
            case "bytes":
                returnSchema = typeBuilder.bytesType();
                break;
            default: throw new IllegalArgumentException(String.format("Unknown Avro primitive type: %s", dataType));
        }
        return returnSchema;
    }

    protected static String getPrimitiveAvroTypeFromCassandraType(DataType dataType) {

        if (dataType.equals(DataTypes.ASCII)
                || dataType.equals(DataTypes.TEXT)
                || dataType.equals(DataTypes.TIMESTAMP)
                || dataType.equals(DataTypes.DATE)
                || dataType.equals(DataTypes.TIME)
                || dataType.equals(DataTypes.TIMEUUID)
                || dataType.equals(DataTypes.UUID)
                || dataType.equals(DataTypes.INET)
                || dataType.equals(DataTypes.VARINT)
                || dataType.equals(DataTypes.DECIMAL)
                || dataType.equals(DataTypes.DURATION)) {

            return "string";

        } else if (dataType.equals(DataTypes.BOOLEAN)) {
            return "boolean";

        } else if (dataType.equals(DataTypes.INT)
                || dataType.equals(DataTypes.SMALLINT)
                || dataType.equals(DataTypes.TINYINT)) {
            return "int";

        } else if (dataType.equals(DataTypes.BIGINT)
                || dataType.equals(DataTypes.COUNTER)) {
            return "long";

        } else if (dataType.equals(DataTypes.FLOAT)) {
            return "float";

        } else if (dataType.equals(DataTypes.DOUBLE)) {
            return "double";

        } else if (dataType.equals(DataTypes.BLOB)) {
            return "bytes";

        } else {
            throw new IllegalArgumentException(
                    String.format("createSchema: Unknown Cassandra data type %s cannot be converted to Avro type", dataType)
            );
        }
    }

    protected static DataType getPrimitiveDataTypeFromString(String dataTypeName) {
        List<DataType> primitiveTypes = List.of(
                DataTypes.ASCII,
                DataTypes.BIGINT,
                DataTypes.BLOB,
                DataTypes.BOOLEAN,
                DataTypes.COUNTER,
                DataTypes.DATE,
                DataTypes.DECIMAL,
                DataTypes.DOUBLE,
                DataTypes.FLOAT,
                DataTypes.INET,
                DataTypes.INT,
                DataTypes.SMALLINT,
                DataTypes.TEXT,
                DataTypes.TIME,
                DataTypes.TIMESTAMP,
                DataTypes.TIMEUUID,
                DataTypes.TINYINT,
                DataTypes.UUID,
                DataTypes.VARINT
        );

        for (DataType primitiveType : primitiveTypes) {
            if (primitiveType.asCql(false, false).equalsIgnoreCase(dataTypeName)) {
                return primitiveType;
            }
        }
        return null;
    }
}
