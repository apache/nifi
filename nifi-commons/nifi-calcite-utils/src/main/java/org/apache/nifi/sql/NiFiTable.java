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

package org.apache.nifi.sql;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.util.Pair;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.sql.internal.CalciteTable;
import org.apache.nifi.sql.internal.NiFiTableEnumerator;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A table that can be queried in a {@link CalciteDatabase}
 */
public class NiFiTable implements Closeable {
    private final String name;
    private final NiFiTableSchema tableSchema;
    private final ComponentLog logger;

    private final Set<NiFiTableEnumerator> enumerators = Collections.synchronizedSet(new HashSet<>());

    private volatile ResettableDataSource dataSource;
    private volatile int maxRecordsRead;


    /**
     * Created a NiFi table with a datasource already established. The table's schema will be established as the schema
     * of the datasource.
     *
     * @param name       the name of the table. This is the name that should be referenced in SQL statements.
     * @param dataSource the source of data
     * @param logger     the logger to use
     */
    public NiFiTable(final String name, final ResettableDataSource dataSource, final ComponentLog logger) {
        this(name, dataSource.getSchema(), logger);
        this.dataSource = dataSource;
    }

    /**
     * Created a NiFi table without yet establishing the datasource. This table cannot be queried until the datasource
     * is established by calling {@link #setDataSource(ResettableDataSource)}.
     *
     * @param name   the name of the table. This is the name that should be referenced in SQL statements.
     * @param schema the table's schema
     * @param logger the logger to use
     */
    public NiFiTable(final String name, final NiFiTableSchema schema, final ComponentLog logger) {
        this.name = name;
        this.tableSchema = schema;
        this.logger = logger;
    }

    public String getName() {
        return name;
    }

    /**
     * Updates the datasource to use for retrieving rows of data. The given data source must have the same schema as was provided when the
     * table was created.
     *
     * @param dataSource the source of data
     */
    public void setDataSource(final ResettableDataSource dataSource) {
        if (!tableSchema.equals(dataSource.getSchema())) {
            throw new IllegalArgumentException("Cannot update data source because the newly provided data source [%s] has a different schema than the current data source [%s]".formatted(
                dataSource, this.dataSource));
        }

        this.dataSource = dataSource;
        maxRecordsRead = 0;
    }

    /**
     * @return the number of records that were read from the datasource
     */
    public int getRecordsRead() {
        return maxRecordsRead;
    }

    CalciteTable createCalciteTable() {
        return new CalciteTable(this::createTableDefinition, NiFiTableEnumerable::new);
    }

    private RelDataType createTableDefinition(final RelDataTypeFactory typeFactory) {
        final List<String> names = new ArrayList<>();
        final List<RelDataType> types = new ArrayList<>();

        final JavaTypeFactory javaTypeFactory = (JavaTypeFactory) typeFactory;
        for (final ColumnSchema column : tableSchema.columns()) {
            names.add(column.getName());
            types.add(column.toRelationalDataType(javaTypeFactory));
        }

        return typeFactory.createStructType(Pair.zip(names, types));
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final NiFiTable nifiTable = (NiFiTable) o;
        return Objects.equals(name, nifiTable.name) && Objects.equals(tableSchema, nifiTable.tableSchema);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableSchema);
    }

    @Override
    public String toString() {
        return "NiFiTable[name=" + name + "]";
    }

    public void close() {
        for (final NiFiTableEnumerator enumerator : enumerators) {
            enumerator.close();
        }
    }


    private class NiFiTableEnumerable extends AbstractEnumerable<Object> {
        private final int[] fields;

        public NiFiTableEnumerable(final int[] fields) {
            this.fields = fields;
        }

        public Enumerator<Object> enumerator() {
            final NiFiTableEnumerator flowFileEnumerator = new NiFiTableEnumerator(dataSource, logger, fields, this::onFinish, enumerators::remove);
            enumerators.add(flowFileEnumerator);
            return flowFileEnumerator;
        }

        private void onFinish() {
            final int recordCount = maxRecordsRead;
            if (recordCount > maxRecordsRead) {
                maxRecordsRead = recordCount;
            }
        }
    }
}
