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

package org.apache.nifi.sql.internal;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;

import java.lang.reflect.Type;
import java.util.function.Function;

/**
 * <p>
 * An internal representation of a NiFiTable that is necessary for Apache Calcite.
 * This class never be referenced outside of the module in which it is defined.
 * </p>
 */
public class CalciteTable extends AbstractTable implements QueryableTable, TranslatableTable {
    private final Function<RelDataTypeFactory, RelDataType> tableDefinitionFactory;
    private final Function<int[], Enumerable<Object>> projectionFactory;

    public CalciteTable(final Function<RelDataTypeFactory, RelDataType> tableDefinitionFactory, final Function<int[], Enumerable<Object>> projectionFactory) {
        this.tableDefinitionFactory = tableDefinitionFactory;
        this.projectionFactory = projectionFactory;
    }

    // Returns an enumerable over a given projection of the fields.
    // Called from generated code. While the DataContent is not used, it is provided by the calling code and must be present.
    public Enumerable<Object> project(final DataContext dataContext, final int[] fields) {
        return projectionFactory.apply(fields);
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

        return new NiFiTableScan(context.getCluster(), relOptTable, fields);
    }

    @Override
    public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
        return tableDefinitionFactory.apply(typeFactory);
    }
}
