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

package org.apache.nifi.repository.schema;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.List;

public class MapRecordField implements RecordField {
    private final String fieldName;
    private final RecordField keyField;
    private final RecordField valueField;
    private final Repetition repetition;
    private final List<RecordField> subFields;

    public MapRecordField(final String fieldName, final RecordField keyField, final RecordField valueField, final Repetition repetition) {
        this.fieldName = requireNonNull(fieldName);
        this.keyField = requireNonNull(keyField);
        this.valueField = requireNonNull(valueField);
        this.repetition = requireNonNull(repetition);

        subFields = new ArrayList<>(2);
        subFields.add(keyField);
        subFields.add(valueField);
    }

    @Override
    public String getFieldName() {
        return fieldName;
    }

    @Override
    public FieldType getFieldType() {
        return FieldType.MAP;
    }

    @Override
    public Repetition getRepetition() {
        return repetition;
    }

    @Override
    public List<RecordField> getSubFields() {
        return subFields;
    }

    public RecordField getKeyField() {
        return keyField;
    }

    public RecordField getValueField() {
        return valueField;
    }

    @Override
    public String toString() {
        return "MapRecordField[" + fieldName + "]";
    }
}
