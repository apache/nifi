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

import java.util.Date;
import java.util.Optional;

import org.apache.nifi.serialization.record.util.IllegalTypeConversionException;

public interface Record {

    RecordSchema getSchema();

    /**
     * Updates the Record's schema to to incorporate all of the fields in the given schema. If both schemas have a
     * field with the same name but a different type, then the existing schema will be updated to have a
     * {@link RecordFieldType#CHOICE} field with both types as choices. If two fields have the same name but different
     * default values, then the default value that is already in place will remain the default value, unless the current
     * default value is <code>null</code>. Note that all values for this Record will still be valid according
     * to this Record's Schema after this operation completes, as no type will be changed except to become more
     * lenient. However, if incorporating the other schema does modify this schema, then the schema text
     * returned by {@link #getSchemaText()}, the schema format returned by {@link #getSchemaFormat()}, and
     * the SchemaIdentifier returned by {@link #getIdentifier()} for this record's schema may all become Empty.
     *
     * @param other the other schema to incorporate into this Record's schema
     *
     * @throws UnsupportedOperationException if this record does not support incorporating other schemas
     */
    void incorporateSchema(RecordSchema other);

    /**
     * <p>
     * Returns a view of the the values of the fields in this Record.
     * </p>
     *
     * <b>NOTE:</b> The array that is returned may be an underlying array that is backing
     * the contents of the Record. As such, modifying the array in any way may result in
     * modifying the record.
     *
     * @return a view of the values of the fields in this Record
     */
    Object[] getValues();

    Object getValue(String fieldName);

    Object getValue(RecordField field);

    String getAsString(String fieldName);

    String getAsString(String fieldName, String format);

    String getAsString(RecordField field, String format);

    Long getAsLong(String fieldName);

    Integer getAsInt(String fieldName);

    Double getAsDouble(String fieldName);

    Float getAsFloat(String fieldName);

    Record getAsRecord(String fieldName, RecordSchema schema);

    Boolean getAsBoolean(String fieldName);

    Date getAsDate(String fieldName, String format);

    Object[] getAsArray(String fieldName);

    Optional<SerializedForm> getSerializedForm();

    /**
     * Updates the value of the field with the given name to the given value. If the field specified
     * is not present in this Record's schema, this method will do nothing. If this method does change
     * any value in the Record, any {@link SerializedForm} that was provided will be removed (i.e., any
     * subsequent call to {@link #getSerializedForm()} will return an empty Optional).
     *
     * @param fieldName the name of the field to update
     * @param value the new value to set
     *
     * @throws IllegalTypeConversionException if the value is not of the correct type, as defined
     *             by the schema, and cannot be coerced into the correct type.
     */
    void setValue(String fieldName, Object value);

    /**
     * Updates the value of a the specified index of a field. If the field specified
     * is not present in this Record's schema, this method will do nothing. If the field specified
     * is not an Array, an IllegalArgumentException will be thrown. If the field specified is an array
     * but the array has fewer elements than the specified index, this method will do nothing. If this method does change
     * any value in the Record, any {@link SerializedForm} that was provided will be removed (i.e., any
     * subsequent call to {@link #getSerializedForm()} will return an empty Optional).
     *
     * @param fieldName the name of the field to update
     * @param arrayIndex the 0-based index into the array that should be updated. If this value is larger than the
     *            number of elements in the array, or if the array is null, this method will do nothing.
     * @param value the new value to set
     *
     * @throws IllegalTypeConversionException if the value is not of the correct type, as defined
     *             by the schema, and cannot be coerced into the correct type; or if the field with the given
     *             name is not an Array
     * @throws IllegalArgumentException if the arrayIndex is less than 0.
     */
    void setArrayValue(String fieldName, int arrayIndex, Object value);

    /**
     * Updates the value of a the specified key in a Map field. If the field specified
     * is not present in this Record's schema, this method will do nothing. If the field specified
     * is not a Map field, an IllegalArgumentException will be thrown. If this method does change
     * any value in the Record, any {@link SerializedForm} that was provided will be removed (i.e., any
     * subsequent call to {@link #getSerializedForm()} will return an empty Optional).
     *
     * @param fieldName the name of the field to update
     * @param mapKey the key in the map of the entry to update
     * @param value the new value to set
     *
     * @throws IllegalTypeConversionException if the value is not of the correct type, as defined
     *             by the schema, and cannot be coerced into the correct type; or if the field with the given
     *             name is not a Map
     */
    void setMapValue(String fieldName, String mapKey, Object value);
}
