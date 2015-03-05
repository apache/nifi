/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.nifi.processors.kite;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

import static org.apache.avro.generic.GenericData.StringType;

class AvroUtil {

  @SuppressWarnings("unchecked")
  public static <D> DatumWriter<D> newDatumWriter(Schema schema, Class<D> dClass) {
    return (DatumWriter<D>) GenericData.get().createDatumWriter(schema);
  }

  @SuppressWarnings("unchecked")
  public static <D> DatumReader<D> newDatumReader(Schema schema, Class<D> dClass) {
    return (DatumReader<D>) GenericData.get().createDatumReader(schema);
  }

}
