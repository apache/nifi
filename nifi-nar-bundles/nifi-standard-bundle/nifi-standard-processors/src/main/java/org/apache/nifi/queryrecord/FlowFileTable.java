/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.queryrecord;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.Schema.TableType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.Pair;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.ChoiceDataType;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class FlowFileTable extends AbstractTable implements QueryableTable, TranslatableTable {

    private final RecordReaderFactory recordReaderFactory;
    private final ComponentLog logger;

    private RecordSchema recordSchema;
    private RelDataType relDataType = null;

    private volatile ProcessSession session;
    private volatile FlowFile flowFile;
    private volatile int maxRecordsRead;

    private final Set<FlowFileEnumerator> enumerators = new HashSet<>();

    /**
     * Creates a FlowFile table.
     */
    public FlowFileTable(final ProcessSession session, final FlowFile flowFile, final RecordSchema schema, final RecordReaderFactory recordReaderFactory, final ComponentLog logger) {
        this.session = session;
        this.flowFile = flowFile;
        this.recordSchema = schema;
        this.recordReaderFactory = recordReaderFactory;
        this.logger = logger;
    }

    public void setFlowFile(final ProcessSession session, final FlowFile flowFile) {
        this.session = session;
        this.flowFile = flowFile;
        this.maxRecordsRead = 0;
    }


    @Override
    public String toString() {
        return "FlowFileTable";
    }

    public void close() {
        synchronized (enumerators) {
            for (final FlowFileEnumerator enumerator : enumerators) {
                enumerator.close();
            }
        }
    }

    /**
     * Returns an enumerable over a given projection of the fields.
     *
     * <p>
     * Called from generated code.
     */
    public Enumerable<Object> project(final int[] fields) {
        return new AbstractEnumerable<Object>() {
            @Override
            @SuppressWarnings({"unchecked", "rawtypes"})
            public Enumerator<Object> enumerator() {
                final FlowFileEnumerator flowFileEnumerator = new FlowFileEnumerator(session, flowFile, logger, recordReaderFactory, fields) {
                    @Override
                    protected void onFinish() {
                        final int recordCount = getRecordsRead();
                        if (recordCount > maxRecordsRead) {
                            maxRecordsRead = recordCount;
                        }
                    }

                    @Override
                    public void close() {
                        synchronized (enumerators) {
                            enumerators.remove(this);
                        }
                        super.close();
                    }
                };

                synchronized (enumerators) {
                    enumerators.add(flowFileEnumerator);
                }

                return flowFileEnumerator;
            }
        };
    }

    public int getRecordsRead() {
        return maxRecordsRead;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public Expression getExpression(final SchemaPlus schema, final String tableName, final Class clazz) {
        return Schemas.tableExpression(schema, getElementType(), tableName, clazz);
    }

    @Override
    public Type getElementType() {
        return Object[].class;
    }

    @Override
    public <T> Queryable<T> asQueryable(final QueryProvider queryProvider, final SchemaPlus schema, final String tableName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RelNode toRel(final RelOptTable.ToRelContext context, final RelOptTable relOptTable) {
        // Request all fields.
        final int fieldCount = relOptTable.getRowType().getFieldCount();
        final int[] fields = new int[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            fields[i] = i;
        }

        return new FlowFileTableScan(context.getCluster(), relOptTable, this, fields);
    }

    @Override
    public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
        if (relDataType != null) {
            return relDataType;
        }

        final List<String> names = new ArrayList<>();
        final List<RelDataType> types = new ArrayList<>();

        final JavaTypeFactory javaTypeFactory = (JavaTypeFactory) typeFactory;
        for (final RecordField field : recordSchema.getFields()) {
            names.add(field.getFieldName());
            final RelDataType relDataType = getRelDataType(field.getDataType(), javaTypeFactory);
            types.add(javaTypeFactory.createTypeWithNullability(relDataType, field.isNullable()));
        }

        relDataType = typeFactory.createStructType(Pair.zip(names, types));
        return relDataType;
    }

    private RelDataType getRelDataType(final DataType fieldType, final JavaTypeFactory typeFactory) {
        switch (fieldType.getFieldType()) {
            case BOOLEAN:
                return typeFactory.createJavaType(boolean.class);
            case BYTE:
                return typeFactory.createJavaType(byte.class);
            case CHAR:
                return typeFactory.createJavaType(char.class);
            case DATE:
                return typeFactory.createJavaType(java.sql.Date.class);
            case DOUBLE:
                return typeFactory.createJavaType(double.class);
            case FLOAT:
                return typeFactory.createJavaType(float.class);
            case INT:
                return typeFactory.createJavaType(int.class);
            case SHORT:
                return typeFactory.createJavaType(short.class);
            case TIME:
                return typeFactory.createJavaType(java.sql.Time.class);
            case TIMESTAMP:
                return typeFactory.createJavaType(java.sql.Timestamp.class);
            case LONG:
                return typeFactory.createJavaType(long.class);
            case STRING:
                return typeFactory.createJavaType(String.class);
            case ARRAY:
                ArrayDataType array = (ArrayDataType) fieldType;
                return typeFactory.createArrayType(getRelDataType(array.getElementType(), typeFactory), -1);
            case RECORD:
                return typeFactory.createJavaType(Record.class);
            case MAP:
                return typeFactory.createJavaType(HashMap.class);
            case BIGINT:
                return typeFactory.createJavaType(BigInteger.class);
            case DECIMAL:
                return typeFactory.createJavaType(BigDecimal.class);
            case CHOICE:
                final ChoiceDataType choiceDataType = (ChoiceDataType) fieldType;
                DataType widestDataType = choiceDataType.getPossibleSubTypes().get(0);
                for (final DataType possibleType : choiceDataType.getPossibleSubTypes()) {
                    if (possibleType == widestDataType) {
                        continue;
                    }
                    if (possibleType.getFieldType().isWiderThan(widestDataType.getFieldType())) {
                        widestDataType = possibleType;
                        continue;
                    }
                    if (widestDataType.getFieldType().isWiderThan(possibleType.getFieldType())) {
                        continue;
                    }

                    // Neither is wider than the other.
                    widestDataType = null;
                    break;
                }

                // If one of the CHOICE data types is the widest, use it.
                if (widestDataType != null) {
                    return getRelDataType(widestDataType, typeFactory);
                }

                // None of the data types is strictly the widest. Check if all data types are numeric.
                // This would happen, for instance, if the data type is a choice between float and integer.
                // If that is the case, we can use a String type for the table schema because all values will fit
                // into a String. This will still allow for casting, etc. if the query requires it.
                boolean allNumeric = true;
                for (final DataType possibleType : choiceDataType.getPossibleSubTypes()) {
                    if (!isNumeric(possibleType)) {
                        allNumeric = false;
                        break;
                    }
                }

                if (allNumeric) {
                    return typeFactory.createJavaType(String.class);
                }

                // There is no specific type that we can use for the schema. This would happen, for instance, if our
                // CHOICE is between an integer and a Record.
                return typeFactory.createJavaType(Object.class);
        }

        throw new IllegalArgumentException("Unknown Record Field Type: " + fieldType);
    }

    private boolean isNumeric(final DataType dataType) {
        switch (dataType.getFieldType()) {
            case BIGINT:
            case BYTE:
            case DECIMAL:
            case DOUBLE:
            case FLOAT:
            case INT:
            case LONG:
            case SHORT:
                return true;
            default:
                return false;
        }
    }

    @Override
    public TableType getJdbcTableType() {
        return TableType.TEMPORARY_TABLE;
    }
}
