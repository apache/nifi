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
package org.apache.nifi.serialization.record.validation;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Immutable container that carries field-level and record-level validators independently of the schema model.
 * Field validators are keyed by field name. Record validators apply to the entire record.
 * Nested validators mirror the schema tree: a RECORD-type or ARRAY-of-RECORD field maps to a child
 * {@code SchemaValidators} that applies to the nested record.
 */
public class SchemaValidators {
    public static final SchemaValidators EMPTY = new SchemaValidators(Map.of(), List.of(), Map.of());

    private final Map<String, List<FieldValidator>> fieldValidators;
    private final List<RecordValidator> recordValidators;
    private final Map<String, SchemaValidators> nestedValidators;

    public SchemaValidators(final Map<String, List<FieldValidator>> fieldValidators, final List<RecordValidator> recordValidators) {
        this(fieldValidators, recordValidators, Map.of());
    }

    public SchemaValidators(final Map<String, List<FieldValidator>> fieldValidators, final List<RecordValidator> recordValidators,
            final Map<String, SchemaValidators> nestedValidators) {
        if (fieldValidators == null || fieldValidators.isEmpty()) {
            this.fieldValidators = Map.of();
        } else {
            final Map<String, List<FieldValidator>> defensiveCopy = new HashMap<>(fieldValidators.size());
            for (final Map.Entry<String, List<FieldValidator>> entry : fieldValidators.entrySet()) {
                defensiveCopy.put(entry.getKey(), List.copyOf(entry.getValue()));
            }
            this.fieldValidators = Collections.unmodifiableMap(defensiveCopy);
        }
        this.recordValidators = recordValidators == null || recordValidators.isEmpty() ? List.of() : List.copyOf(recordValidators);
        this.nestedValidators = nestedValidators == null || nestedValidators.isEmpty() ? Map.of() : Map.copyOf(nestedValidators);
    }

    public List<FieldValidator> getFieldValidators(final String fieldName) {
        return fieldValidators.getOrDefault(fieldName, List.of());
    }

    public Map<String, List<FieldValidator>> getAllFieldValidators() {
        return fieldValidators;
    }

    public List<RecordValidator> getRecordValidators() {
        return recordValidators;
    }

    public SchemaValidators getNestedValidators(final String fieldName) {
        return nestedValidators.getOrDefault(fieldName, EMPTY);
    }

    public boolean isEmpty() {
        return fieldValidators.isEmpty() && recordValidators.isEmpty() && nestedValidators.isEmpty();
    }
}
