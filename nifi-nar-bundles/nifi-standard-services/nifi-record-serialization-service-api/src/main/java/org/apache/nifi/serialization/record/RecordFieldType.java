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

package org.apache.nifi.serialization.record;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public enum RecordFieldType {
    STRING("string"),
    BOOLEAN("boolean"),
    BYTE("byte"),
    CHAR("char"),
    SHORT("short"),
    INT("int"),
    BIGINT("bigint"),
    LONG("long"),
    FLOAT("float"),
    DOUBLE("double"),
    DATE("date", "yyyy-MM-dd"),
    TIME("time", "HH:mm:ss"),
    TIMESTAMP("timestamp", "yyyy-MM-dd HH:mm:ss"),
    RECORD("record"),
    CHOICE("choice"),
    ARRAY("array");


    private static final Map<String, RecordFieldType> SIMPLE_NAME_MAP = new HashMap<String, RecordFieldType>();

    static {
      for (RecordFieldType value : values()) {
        SIMPLE_NAME_MAP.put(value.simpleName, value);
      }
    }

    private final String simpleName;
    private final String defaultFormat;
    private final DataType defaultDataType;

    private RecordFieldType(final String simpleName) {
        this(simpleName, null);
    }

    private RecordFieldType(final String simpleName, final String defaultFormat) {
        this.simpleName = simpleName;
        this.defaultFormat = defaultFormat;
        this.defaultDataType = new DataType(this, defaultFormat);
    }

    public String getDefaultFormat() {
        return defaultFormat;
    }

    /**
     * @return the DataType with the default format
     */
    public DataType getDataType() {
        return defaultDataType;
    }

    public DataType getDataType(final String format) {
        return new DataType(this, format);
    }

    /**
     * Returns a Data Type that represents a "RECORD" type with the given schema.
     *
     * @param childSchema the Schema for the Record
     * @return a DataType that represents a Record with the given schema, or <code>null</code> if this RecordFieldType
     *         is not the RECORD type.
     */
    public DataType getDataType(final RecordSchema childSchema) {
        if (this != RECORD) {
            return null;
        }

        return new DataType(this, getDefaultFormat(), childSchema);
    }

    /**
     * Returns a Data Type that represents a "CHOICE" of multiple possible types. This method is
     * only applicable for a RecordFieldType of {@link #CHOICE}.
     *
     * @param possibleChildTypes the possible types that are allowable
     * @return a DataType that represents a "CHOICE" of multiple possible types, or <code>null</code> if this RecordFieldType
     *         is not the CHOICE type.
     */
    public DataType getDataType(final List<DataType> possibleChildTypes) {
        if (this != CHOICE) {
            return null;
        }

        return new DataType(this, getDefaultFormat(), possibleChildTypes);
    }

    public static RecordFieldType of(final String typeString) {
      return SIMPLE_NAME_MAP.get(typeString);
    }
}
