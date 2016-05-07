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

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

/**
 * Factory that creates a {@link CsvSchema}.
 *
 * <p>Allows a custom schema to be included in a <code><i>model</i>.json</code>
 * file.</p>
 */
@SuppressWarnings("UnusedDeclaration")
public class CsvSchemaFactory2 implements SchemaFactory {
  final private Map<String, Reader> inputs;
  // public constructor, per factory contract
  public CsvSchemaFactory2(Map<String, Reader> inputs) {
      this.inputs = inputs;
  }

  public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
    String flavorName = (String) operand.get("flavor");
    CsvTable.Flavor flavor;
    if (flavorName == null) {
      flavor = CsvTable.Flavor.SCANNABLE;
    } else {
      flavor = CsvTable.Flavor.valueOf(flavorName.toUpperCase());
    }

    return new CsvSchema2(inputs, flavor);
  }
}

// End CsvSchemaFactory2.java
