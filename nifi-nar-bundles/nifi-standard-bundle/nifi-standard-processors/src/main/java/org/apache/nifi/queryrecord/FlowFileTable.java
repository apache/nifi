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

import java.io.InputStream;
import java.lang.reflect.Type;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;


public class FlowFileTable<S, E> extends AbstractTable implements QueryableTable, TranslatableTable {

    private final RecordReaderFactory recordParserFactory;
    private final ComponentLog logger;

    private RecordSchema recordSchema;
    private RelDataType relDataType = null;

    private volatile ProcessSession session;
    private volatile FlowFile flowFile;
    private volatile int maxRecordsRead;

    private final Set<FlowFileEnumerator<?>> enumerators = new HashSet<>();

    /**
     * Creates a FlowFile table.
     */
    public FlowFileTable(final ProcessSession session, final FlowFile flowFile, final RecordReaderFactory recordParserFactory, final ComponentLog logger) {
        this.session = session;
        this.flowFile = flowFile;
        this.recordParserFactory = recordParserFactory;
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
            for (final FlowFileEnumerator<?> enumerator : enumerators) {
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
                final FlowFileEnumerator flowFileEnumerator = new FlowFileEnumerator(session, flowFile, logger, recordParserFactory, fields) {
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

        RecordSchema schema;
        try (final InputStream in = session.read(flowFile)) {
            final RecordReader recordParser = recordParserFactory.createRecordReader(flowFile, in, logger);
            schema = recordParser.getSchema();
        } catch (final Exception e) {
            throw new ProcessException("Failed to determine schema of data records for " + flowFile, e);
        }

        final List<String> names = new ArrayList<>();
        final List<RelDataType> types = new ArrayList<>();

        final JavaTypeFactory javaTypeFactory = (JavaTypeFactory) typeFactory;
        for (final RecordField field : schema.getFields()) {
            names.add(field.getFieldName());
            final RelDataType relDataType = getRelDataType(field.getDataType(), javaTypeFactory);
            types.add(javaTypeFactory.createTypeWithNullability(relDataType, field.isNullable()));
        }

        logger.debug("Found Schema: {}", new Object[] {schema});

        if (recordSchema == null) {
            recordSchema = schema;
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
                return typeFactory.createJavaType(Object[].class);
            case RECORD:
                return typeFactory.createJavaType(Record.class);
            case MAP:
                return typeFactory.createJavaType(HashMap.class);
            case BIGINT:
                return typeFactory.createJavaType(BigInteger.class);
            case CHOICE:
                return typeFactory.createJavaType(Object.class);
        }

        throw new IllegalArgumentException("Unknown Record Field Type: " + fieldType);
    }

    @Override
    public TableType getJdbcTableType() {
        return TableType.TEMPORARY_TABLE;
    }
}
