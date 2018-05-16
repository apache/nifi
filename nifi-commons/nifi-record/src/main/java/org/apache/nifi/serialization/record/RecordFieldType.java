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

import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.ChoiceDataType;
import org.apache.nifi.serialization.record.type.MapDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public enum RecordFieldType {
    /**
     * A String field type. Fields of this type use a {@code java.lang.String} value.
     */
    STRING("string"),

    /**
     * A boolean field type. Fields of this type use a {@code boolean} value.
     */
    BOOLEAN("boolean"),

    /**
     * A byte field type. Fields of this type use a {@code byte} value.
     */
    BYTE("byte"),

    /**
     * A char field type. Fields of this type use a {@code char} value.
     */
    CHAR("char"),

    /**
     * A short field type. Fields of this type use a {@code short} value.
     */
    SHORT("short"),

    /**
     * An int field type. Fields of this type use an {@code int} value.
     */
    INT("int"),

    /**
     * A bigint field type. Fields of this type use a {@code java.math.BigInteger} value.
     */
    BIGINT("bigint"),

    /**
     * A long field type. Fields of this type use a {@code long} value.
     */
    LONG("long"),

    /**
     * A float field type. Fields of this type use a {@code float} value.
     */
    FLOAT("float"),

    /**
     * A double field type. Fields of this type use a {@code double} value.
     */
    DOUBLE("double"),

    /**
     * A date field type. Fields of this type use a {@code java.sql.Date} value.
     */
    DATE("date", "yyyy-MM-dd"),

    /**
     * A time field type. Fields of this type use a {@code java.sql.Time} value.
     */
    TIME("time", "HH:mm:ss"),

    /**
     * A timestamp field type. Fields of this type use a {@code java.sql.Timestamp} value.
     */
    TIMESTAMP("timestamp", "yyyy-MM-dd HH:mm:ss"),

    /**
     * <p>
     * A record field type. Fields of this type use a {@code org.apache.nifi.serialization.record.Record} value. A Record DataType should be
     * created by providing the {@link RecordSchema} for the record:
     * </p>
     *
     * <code>
     * final DataType recordType = RecordFieldType.RECORD.getRecordDataType(recordSchema);
     * </code>
     *
     * <p>
     * A field of type RECORD should always have a {@link RecordDataType}, so the following idiom is acceptable for use:
     * </p>
     *
     * <code>
     * <pre>
     * final DataType dataType = ...;
     * if (dataType.getFieldType() == RecordFieldType.RECORD) {
     *     final RecordDataType recordDataType = (RecordDataType) dataType;
     *     final RecordSchema childSchema = recordDataType.getChildSchema();
     *     ...
     * }
     * </pre>
     * </code>
     */
    RECORD("record", null, new RecordDataType(null)),

    /**
     * <p>
     * A choice field type. A field of type choice can be one of any number of different types, which are defined by the DataType that is used.
     * For example, if a field should allow either a Long or an Integer, this can be accomplished by using:
     * </p>
     *
     * <code>
     * final DataType choiceType = RecordFieldType.CHOICE.getChoiceDataType( RecordFieldType.INT.getDataType(), RecordFieldType.LONG.getDataType() );
     * </code>
     *
     * <p>
     * A field of type CHOICE should always have a {@link ChoiceDataType}, so the following idiom is acceptable for use:
     * </p>
     *
     * <code>
     * <pre>
     * final DataType dataType = ...;
     * if (dataType.getFieldType() == RecordFieldType.CHOICE) {
     *     final ChoiceDataType choiceDataType = (ChoiceDataType) dataType;
     *     final List&lt;DataType&gt; allowableTypes = choiceDataType.getPossibleSubTypes();
     *     ...
     * }
     * </pre>
     * </code>
     */
    CHOICE("choice", null, new ChoiceDataType(Collections.emptyList())),

    /**
     * <p>
     * An array field type. Fields of this type use a {@code Object[]} value. Note that we are explicitly indicating that
     * Object[] should be used here and not primitive array types. For instance, setting a value of {@code int[]} is not allowed. The DataType for
     * this field should be created using the {@link #getArrayDataType(DataType)} method:
     * </p>
     *
     * <code>
     * final DataType arrayType = RecordFieldType.ARRAY.getArrayDataType( RecordFieldType.INT.getDataType() );
     * </code>
     *
     * <p>
     * A field of type ARRAY should always have an {@link ArrayDataType}, so the following idiom is acceptable for use:
     * </p>
     *
     * <code>
     * <pre>
     * final DataType dataType = ...;
     * if (dataType.getFieldType() == RecordFieldType.ARRAY) {
     *     final ArrayDataType arrayDataType = (ArrayDataType) dataType;
     *     final DataType elementType = arrayDataType.getElementType();
     *     ...
     * }
     * </pre>
     * </code>
     */
    ARRAY("array", null, new ArrayDataType(null)),

    /**
     * <p>
     * A record field type. Fields of this type use a {@code Map<String, Object>} value. A Map DataType should be
     * created by providing the {@link DataType} for the values:
     * </p>
     *
     * <code>
     * final DataType recordType = RecordFieldType.MAP.getRecordDataType( RecordFieldType.STRING.getDataType() );
     * </code>
     *
     * <p>
     * A field of type MAP should always have a {@link MapDataType}, so the following idiom is acceptable for use:
     * </p>
     *
     * <code>
     * <pre>
     * final DataType dataType = ...;
     * if (dataType.getFieldType() == RecordFieldType.MAP) {
     *     final MapDataType mapDataType = (MapDataType) dataType;
     *     final DataType valueType = mapDataType.getValueType();
     *     ...
     * }
     * </pre>
     * </code>
     */
    MAP("map", null, new MapDataType(null));


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

    private RecordFieldType(final String simpleName, final String defaultFormat, final DataType defaultDataType) {
        this.simpleName = simpleName;
        this.defaultFormat = defaultFormat;
        this.defaultDataType = defaultDataType;
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
     * Returns a Data Type that represents a "RECORD" or "ARRAY" type with the given schema.
     *
     * @param childSchema the Schema for the Record or Array
     * @return a DataType that represents a Record or Array with the given schema, or <code>null</code> if this RecordFieldType
     *         is not the RECORD or ARRAY type.
     */
    public DataType getRecordDataType(final RecordSchema childSchema) {
        if (this != RECORD) {
            return null;
        }

        return new RecordDataType(childSchema);
    }

    /**
     * Returns a Data Type that represents an "ARRAY" type with the given element type.
     *
     * @param elementType the type of the arrays in the element
     * @return a DataType that represents an Array with the given element type, or <code>null</code> if this RecordFieldType
     *         is not the ARRAY type.
     */
    public DataType getArrayDataType(final DataType elementType) {
        if (this != ARRAY) {
            return null;
        }

        return new ArrayDataType(elementType);
    }


    /**
     * Returns a Data Type that represents a "CHOICE" of multiple possible types. This method is
     * only applicable for a RecordFieldType of {@link #CHOICE}.
     *
     * @param possibleChildTypes the possible types that are allowable
     * @return a DataType that represents a "CHOICE" of multiple possible types, or <code>null</code> if this RecordFieldType
     *         is not the CHOICE type.
     */
    public DataType getChoiceDataType(final List<DataType> possibleChildTypes) {
        if (this != CHOICE) {
            return null;
        }

        return new ChoiceDataType(possibleChildTypes);
    }

    /**
     * Returns a Data Type that represents a "CHOICE" of multiple possible types. This method is
     * only applicable for a RecordFieldType of {@link #CHOICE}.
     *
     * @param possibleChildTypes the possible types that are allowable
     * @return a DataType that represents a "CHOICE" of multiple possible types, or <code>null</code> if this RecordFieldType
     *         is not the CHOICE type.
     */
    public DataType getChoiceDataType(final DataType... possibleChildTypes) {
        if (this != CHOICE) {
            return null;
        }

        final List<DataType> list = new ArrayList<>(possibleChildTypes.length);
        for (final DataType type : possibleChildTypes) {
            list.add(type);
        }

        return new ChoiceDataType(list);
    }

    /**
     * Returns a Data Type that represents a "MAP" type with the given value type.
     *
     * @param valueDataType the type of the values in the map
     * @return a DataType that represents a Map with the given value type, or <code>null</code> if this RecordFieldType
     *         is not the MAP type.
     */
    public DataType getMapDataType(final DataType valueDataType) {
        if (this != MAP) {
            return null;
        }

        return new MapDataType(valueDataType);
    }


    public static RecordFieldType of(final String typeString) {
      return SIMPLE_NAME_MAP.get(typeString);
    }
}
