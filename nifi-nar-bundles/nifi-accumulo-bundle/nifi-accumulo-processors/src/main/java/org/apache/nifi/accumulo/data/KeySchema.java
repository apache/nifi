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
package org.apache.nifi.accumulo.data;


import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class KeySchema implements RecordSchema {
    private static final List<RecordField> KEY_FIELDS = new ArrayList<>();

    private static final List<DataType> DATA_TYPES = new ArrayList<>();

    private static final List<String> FIELD_NAMES = new ArrayList<>();

    static {
        KEY_FIELDS.add(new RecordField("row", RecordFieldType.STRING.getDataType(),false));
        KEY_FIELDS.add(new RecordField("columnFamily",RecordFieldType.STRING.getDataType(),true));
        KEY_FIELDS.add(new RecordField("columnQualifier",RecordFieldType.STRING.getDataType(),true));
        KEY_FIELDS.add(new RecordField("columnVisibility",RecordFieldType.STRING.getDataType(),true));
        KEY_FIELDS.add(new RecordField("timestamp",RecordFieldType.LONG.getDataType(),true));
        DATA_TYPES.add(RecordFieldType.STRING.getDataType());
        DATA_TYPES.add(RecordFieldType.LONG.getDataType());
        FIELD_NAMES.addAll(KEY_FIELDS.stream().map( x-> x.getFieldName()).collect(Collectors.toList()));
    }
    @Override
    public List<RecordField> getFields() {
        return KEY_FIELDS;
    }

    @Override
    public int getFieldCount() {
        return KEY_FIELDS.size();
    }

    @Override
    public RecordField getField(int i) {
        return KEY_FIELDS.get(i);
    }

    @Override
    public List<DataType> getDataTypes() {
         return DATA_TYPES;
    }

    @Override
    public List<String> getFieldNames() {
        return FIELD_NAMES;
    }

    @Override
    public Optional<DataType> getDataType(String s) {
        if (s.equalsIgnoreCase("timestamp")){
            return Optional.of( RecordFieldType.LONG.getDataType() );
        } else{
            if (FIELD_NAMES.stream().filter(x -> s.equalsIgnoreCase(s)).count() > 0){
                return  Optional.of(RecordFieldType.STRING.getDataType());
            }
        }
        return Optional.empty();
    }

    @Override
    public Optional<String> getSchemaText() {
        return Optional.empty();
    }

    @Override
    public Optional<String> getSchemaFormat() {
        return Optional.empty();
    }

    @Override
    public Optional<RecordField> getField(final String s) {
        return KEY_FIELDS.stream().filter(x -> x.getFieldName().equalsIgnoreCase(s)).findFirst();
    }

    @Override
    public SchemaIdentifier getIdentifier() {
        return SchemaIdentifier.builder().name("AccumuloKeySchema").version(1).branch("nifi-accumulo").build();
    }

    @Override
    public Optional<String> getSchemaName() {
        return Optional.of("AccumuloKeySchema");
    }

    @Override
    public Optional<String> getSchemaNamespace() {
        return Optional.of("nifi-accumulo");
    }
}
