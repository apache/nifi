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
package org.apache.nifi.reporting.sql.provenance;

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
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.reporting.ReportingContext;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class ProvenanceTable extends AbstractTable implements QueryableTable, TranslatableTable {

    private final ComponentLog logger;

    private RelDataType relDataType = null;

    private volatile ReportingContext context;
    private volatile int maxRecordsRead;

    private final Set<ProvenanceEnumerator> enumerators = new HashSet<>();

    /**
     * Creates a Provenance events table.
     */
    public ProvenanceTable(final ReportingContext context, final ComponentLog logger) {
        this.context = context;
        this.logger = logger;
    }

    @Override
    public String toString() {
        return "ProvenanceTable";
    }

    public void close() {
        synchronized (enumerators) {
            for (final ProvenanceEnumerator enumerator : enumerators) {
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
            public Enumerator<Object> enumerator() {
                final ProvenanceEnumerator provenanceEnumerator = new ProvenanceEnumerator(context, logger, fields) {
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
                    enumerators.add(provenanceEnumerator);
                }

                return provenanceEnumerator;
            }
        };
    }

    public int getRecordsRead() {
        return maxRecordsRead;
    }

    @Override
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

        return new ProvenanceTableScan(context.getCluster(), relOptTable, this, fields);
    }

    @Override
    public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
        if (relDataType != null) {
            return relDataType;
        }

        final List<String> names = Arrays.asList(
                "eventId",
                "eventType",
                "timestampMillis",
                "durationMillis",
                "lineageStart",
                "details",
                "componentId",
                "componentName",
                "componentType",
                "processGroupId",
                "processGroupName",
                "entityId",
                "entityType",
                "entitySize",
                "previousEntitySize",
                "updatedAttributes",
                "previousAttributes",
                "contentPath",
                "previousContentPath",
                "parentIds",
                "childIds",
                "transitUri",
                "remoteIdentifier",
                "alternateIdentifier"
        );

        final List<RelDataType> types = new ArrayList<>();
        types.add(typeFactory.createJavaType(long.class));
        types.add(typeFactory.createJavaType(String.class));
        types.add(typeFactory.createJavaType(long.class));
        types.add(typeFactory.createJavaType(long.class));
        types.add(typeFactory.createJavaType(long.class));
        types.add(typeFactory.createJavaType(String.class));
        types.add(typeFactory.createJavaType(String.class));
        types.add(typeFactory.createJavaType(String.class));
        types.add(typeFactory.createJavaType(String.class));
        types.add(typeFactory.createJavaType(String.class));
        types.add(typeFactory.createJavaType(String.class));
        types.add(typeFactory.createJavaType(String.class));
        types.add(typeFactory.createJavaType(String.class));
        types.add(typeFactory.createJavaType(long.class));
        types.add(typeFactory.createJavaType(long.class));
        types.add(typeFactory.createMapType(typeFactory.createJavaType(String.class), typeFactory.createJavaType(String.class)));
        types.add(typeFactory.createMapType(typeFactory.createJavaType(String.class), typeFactory.createJavaType(String.class)));
        types.add(typeFactory.createJavaType(String.class));
        types.add(typeFactory.createJavaType(String.class));
        types.add(typeFactory.createArrayType(typeFactory.createJavaType(String.class), -1));
        types.add(typeFactory.createArrayType(typeFactory.createJavaType(String.class), -1));
        types.add(typeFactory.createJavaType(String.class));
        types.add(typeFactory.createJavaType(String.class));
        types.add(typeFactory.createJavaType(String.class));

        relDataType = typeFactory.createStructType(Pair.zip(names, types));
        return relDataType;
    }

    @Override
    public TableType getJdbcTableType() {
        return TableType.TEMPORARY_TABLE;
    }
}
