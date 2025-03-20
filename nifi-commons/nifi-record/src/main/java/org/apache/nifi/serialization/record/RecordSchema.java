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

import java.util.List;
import java.util.Optional;

public interface RecordSchema {
    /**
     * @return the list of fields that are present in the schema
     */
    List<RecordField> getFields();

    /**
     * @return the number of fields in the schema
     */
    int getFieldCount();

    /**
     * @param index the 0-based index of which field to return
     * @return the index'th field
     *
     * @throws IndexOutOfBoundsException if the index is < 0 or >= the number of fields (determined by {@link #getFieldCount()}).
     */
    RecordField getField(int index);

    /**
     * @return the data types of the fields
     */
    List<DataType> getDataTypes();

    /**
     * @return the names of the fields
     */
    List<String> getFieldNames();

    /**
     * @param fieldName the name of the field whose type is desired
     * @return the RecordFieldType associated with the field that has the given name, or
     *         <code>null</code> if the schema does not contain a field with the given name
     */
    Optional<DataType> getDataType(String fieldName);

    /**
     * @return the textual representation of the schema, if one is available
     */
    Optional<String> getSchemaText();

    /**
     * @return the format of the schema text, if schema text is present
     */
    Optional<String> getSchemaFormat();

    /**
     * @param fieldName the name of the field
     * @return an Optional RecordField for the field with the given name
     */
    Optional<RecordField> getField(String fieldName);

    /**
     * @return the SchemaIdentifier, which provides various attributes for identifying a schema
     */
    SchemaIdentifier getIdentifier();

    /**
     * @return the name of the schema's root record.
     */
    Optional<String> getSchemaName();

    /**
     * @return the namespace of the schema.
     */
    Optional<String> getSchemaNamespace();

    /**
     * @param fieldName the name of the field to be removed from the schema
     */
    void removeField(String fieldName);

    /**
     * @param path the sequence of field names to be removed from the schema
     */
    void removePath(RecordFieldRemovalPath path);

    /**
     * Renames the field that corresponds to the given RecordField so that its new name is equal to the given name
     *
     * @param currentName the current name of the field
     * @param newName     the new name for the field
     * @return <code>true</code> if the field is renamed, <code>false</code> if the field with the given current name cannot be found
     * @throws IllegalArgumentException if unable to rename the field due to a naming conflict
     */
    boolean renameField(String currentName, String newName);

    /**
     * @return true if the schema contains itself as a nested field type, false if it does not
     */
    boolean isRecursive();

    /**
     * @param schemas the list of schemas to check whether the current schema is contained within
     * @return true if the current schema is present within the list of schemas (object reference equality), false otherwise
     */
    default boolean sameAsAny(final List<RecordSchema> schemas) {
        for (final RecordSchema schema : schemas) {
            if (schema == this) {
                return true;
            }
        }
        return false;
    }
}
