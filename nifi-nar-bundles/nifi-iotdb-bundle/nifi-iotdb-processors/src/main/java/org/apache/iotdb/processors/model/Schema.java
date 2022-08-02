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
package org.apache.iotdb.processors.model;

import java.util.*;
import java.util.stream.Collectors;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

public class Schema {
    private TimeType timeType;
    private HashMap<String, Field> fieldMap;
    private ArrayList<String> fieldNames;

    public enum TimeType {
        LONG,
        STRING
    }

    public Schema(String timeType, List<Field> fields) {
        this.timeType = "long".equals(timeType) ? TimeType.LONG : TimeType.STRING;
        this.fieldMap = new HashMap<>();
        this.fieldNames = new ArrayList<>();
        fields.forEach(
                field -> {
                    fieldMap.put(field.getTsName(), field);
                    fieldNames.add(field.getTsName());
                });
    }

    public TimeType getTimeType() {
        return timeType;
    }

    public ArrayList<String> getFieldNames() {
        return fieldNames;
    }

    public List<TSDataType> getDataTypes() {
        return fieldMap.values().stream()
                .map(field -> field.getDataType())
                .collect(Collectors.toList());
    }

    public List<TSEncoding> getEncodingTypes() {
        return fieldMap.values().stream()
                .map(field -> field.getEncoding())
                .collect(Collectors.toList());
    }

    public List<CompressionType> getCompressionTypes() {
        return fieldMap.values().stream()
                .map(field -> field.getCompressionType())
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

    public List<Field> getFields() {
        return (List<Field>) fieldMap.values();
    }

    public static Set<String> getSupportedTimeType() {
        return new HashSet<String>() {
            {
                add("long");
                add("string");
            }
        };
    }
}
