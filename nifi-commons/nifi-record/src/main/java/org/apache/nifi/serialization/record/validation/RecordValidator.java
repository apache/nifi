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

import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import java.util.Collection;

/**
 * Provides validation logic for an entire {@link Record} instance against a {@link RecordSchema}.
 * Record Validators are expected to be immutable and thread-safe.
 */
public interface RecordValidator {

    /**
     * Validates the provided record.
     *
     * @param record the record instance to validate
     * @param schema the schema the record should adhere to
     * @param fieldPath the path within the overall document that identifies the record (root records use the empty string)
     * @return a collection of validation errors. The collection must be empty when the record is valid.
     */
    Collection<ValidationError> validate(Record record, RecordSchema schema, String fieldPath);

    /**
     * @return a short human readable description of what the validator enforces
     */
    String getDescription();
}
