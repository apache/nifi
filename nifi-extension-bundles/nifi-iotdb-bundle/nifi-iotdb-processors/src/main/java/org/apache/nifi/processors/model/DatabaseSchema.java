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
package org.apache.nifi.processors.model;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

public class DatabaseSchema {
    private final Map<String, DatabaseField> fieldMap;
    private final List<String> fieldNames;

    @JsonCreator
    public DatabaseSchema(@JsonProperty("fields") List<DatabaseField> fields) {
        this.fieldMap = new LinkedHashMap<>();
        this.fieldNames = new ArrayList<>();
        fields.forEach(
                field -> {
                    fieldMap.put(field.getTsName(), field);
                    fieldNames.add(field.getTsName());
                });
    }

    public List<String> getFieldNames(String prefix) {
        return fieldNames.stream()
                .map(field -> prefix+field)
                .collect(Collectors.toList());
    }

    public List<TSDataType> getDataTypes() {
        return fieldMap.values().stream()
                .map(DatabaseField::getDataType)
                .collect(Collectors.toList());
    }

    public List<TSEncoding> getEncodingTypes() {
        return fieldMap.values().stream()
                .map(DatabaseField::getEncoding)
                .collect(Collectors.toList());
    }

    public List<CompressionType> getCompressionTypes() {
        return fieldMap.values().stream()
                .map(DatabaseField::getCompressionType)
                .collect(Collectors.toList());
    }

    public TSDataType getDataType(String fieldName) {
        return fieldMap.get(fieldName).getDataType();
    }

    public TSEncoding getEncodingType(String fieldName) {
        return fieldMap.get(fieldName).getEncoding();
    }

    public CompressionType getCompressionType(String fieldName) {
        return fieldMap.get(fieldName).getCompressionType();
    }

}
