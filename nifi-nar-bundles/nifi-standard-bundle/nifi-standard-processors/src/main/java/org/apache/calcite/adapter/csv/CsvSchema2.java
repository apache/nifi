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

import java.io.Reader;
import java.util.Map;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import com.google.common.collect.ImmutableMap;

/**
 * Schema mapped onto a directory of CSV files. Each table in the schema
 * is a CSV file in that directory.
 */
public class CsvSchema2 extends AbstractSchema {
  final private Map<String, Reader> inputs;
  private final CsvTable.Flavor flavor;
  private Map<String, Table> tableMap;

  /**
   * Creates a CSV schema.
   *
   * @param inputs     Inputs map
   * @param flavor     Whether to instantiate flavor tables that undergo
   *                   query optimization
   */
  public CsvSchema2(Map<String, Reader> inputs, CsvTable.Flavor flavor) {
    super();
    this.inputs = inputs;
    this.flavor = flavor;
  }

  /** Looks for a suffix on a string and returns
   * either the string with the suffix removed
   * or the original string. */
  private static String trim(String s, String suffix) {
    String trimmed = trimOrNull(s, suffix);
    return trimmed != null ? trimmed : s;
  }

  /** Looks for a suffix on a string and returns
   * either the string with the suffix removed
   * or null. */
  private static String trimOrNull(String s, String suffix) {
    return s.endsWith(suffix)
        ? s.substring(0, s.length() - suffix.length())
        : null;
  }

  @Override protected Map<String, Table> getTableMap() {

    if (tableMap!=null)
      return tableMap;

    // Build a map from table name to table; each file becomes a table.
    final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();

    for (Map.Entry<String, Reader> entry : inputs.entrySet()) {
      final Table table = createTable(entry.getValue());
      builder.put(entry.getKey(), table);
    }

    tableMap = builder.build();
    return tableMap;
  }

  /** Creates different sub-type of table based on the "flavor" attribute. */
  private Table createTable(Reader readerx) {
    switch (flavor) {
    case TRANSLATABLE:
      return new CsvTranslatableTable2(readerx, null);
//    case SCANNABLE:
//      return new CsvScannableTable(file, null);
//    case FILTERABLE:
//      return new CsvFilterableTable(file, null);
    default:
      throw new AssertionError("Unknown flavor " + flavor);
    }
  }
}

// End CsvSchema2.java
