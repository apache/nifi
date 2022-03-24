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

import java.util.Arrays;
import java.util.List;

public class UnionRecordField implements RecordField {
    private final String fieldName;
    private final Repetition repetition;
    private final List<RecordField> possibilities;

    public UnionRecordField(final String fieldName, final Repetition repetition, final RecordField... possibilities) {
        this(fieldName, repetition, Arrays.asList(possibilities));
    }

    public UnionRecordField(final String fieldName, final Repetition repetition, final List<RecordField> possibilities) {
        this.fieldName = requireNonNull(fieldName);
        this.repetition = requireNonNull(repetition);
        this.possibilities = requireNonNull(possibilities);
    }

    @Override
    public String getFieldName() {
        return fieldName;
    }

    @Override
    public FieldType getFieldType() {
        return FieldType.UNION;
    }

    @Override
    public Repetition getRepetition() {
        return repetition;
    }

    @Override
    public List<RecordField> getSubFields() {
        return possibilities;
    }

    @Override
    public String toString() {
        return "UnionRecordField[name=" + fieldName + ", possible types=" + possibilities + "]";
    }
}
