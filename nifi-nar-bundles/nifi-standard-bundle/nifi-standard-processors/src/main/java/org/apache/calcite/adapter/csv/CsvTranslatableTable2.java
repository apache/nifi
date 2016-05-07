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
package org.apache.calcite.adapter.csv;

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
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.TranslatableTable;

import au.com.bytecode.opencsv.CSVReader;

import java.io.IOException;
import java.io.Reader;
import java.lang.reflect.Type;
import java.util.ArrayList;

/**
 * Table based on a CSV stream.
 */
public class CsvTranslatableTable2 extends CsvTable
    implements QueryableTable, TranslatableTable {

  final private CSVReader csvReader;
  private CsvEnumerator2<Object> csvEnumerator2;
  final private String[] firstLine;

  /** Creates a CsvTable.
   */
  CsvTranslatableTable2(Reader readerx, RelProtoDataType protoRowType) {
    super(null, protoRowType);
    this.csvReader = new CSVReader(readerx);
    try {
        this.firstLine = csvReader.readNext();
    } catch (IOException e) {
        throw new RuntimeException("csvReader.readNext() failed ", e);
    }
  }

  public String toString() {
    return "CsvTranslatableTable2";
  }

  /** Returns an enumerable over a given projection of the fields.
   *
   * <p>Called from generated code. */
  public Enumerable<Object> project(final int[] fields) {
    return new AbstractEnumerable<Object>() {
      public Enumerator<Object> enumerator() {
        return csvEnumerator2;
      }
    };
  }

  public Expression getExpression(SchemaPlus schema, String tableName,
      Class clazz) {
    return Schemas.tableExpression(schema, getElementType(), tableName, clazz);
  }

  public Type getElementType() {
    return Object[].class;
  }

  public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
      SchemaPlus schema, String tableName) {
    throw new UnsupportedOperationException();
  }

  public RelNode toRel(
      RelOptTable.ToRelContext context,
      RelOptTable relOptTable) {
    // Request all fields.
    final int fieldCount = relOptTable.getRowType().getFieldCount();
    final int[] fields = CsvEnumerator.identityList(fieldCount);
    return new CsvTableScan2(context.getCluster(), relOptTable, this, fields);
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      RelDataType rowType = null;

      if (fieldTypes == null) {
          fieldTypes = new ArrayList<CsvFieldType>();
          rowType =  CsvEnumerator2.deduceRowType((JavaTypeFactory) typeFactory, firstLine, fieldTypes);
      } else {
          rowType = CsvEnumerator2.deduceRowType((JavaTypeFactory) typeFactory, firstLine, null);
      }

      if (csvEnumerator2==null)
          csvEnumerator2 = new CsvEnumerator2<Object>(csvReader, fieldTypes);

          return rowType;
      }
}

// End CsvTranslatableTable2.java
