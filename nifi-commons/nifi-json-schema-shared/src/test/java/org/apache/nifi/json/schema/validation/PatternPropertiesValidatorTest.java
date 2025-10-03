/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.json.schema.validation;

import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.validation.ValidationError;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PatternPropertiesValidatorTest {

    @Test
    void testDynamicFieldsValidatedAgainstTemplate() {
        final RecordField templateField = new RecordField(
                "pattern",
                RecordFieldType.STRING.getDataType(),
                null,
                Collections.emptySet(),
                true,
                List.of(new StringLengthValidator(2, null))
        );

        final PatternPropertiesValidator validator = new PatternPropertiesValidator(List.of(
                new PatternPropertiesValidator.PatternPropertyDefinition(Pattern.compile("^x-"), templateField)
        ));

        final RecordField fixedField = new RecordField("fixed", RecordFieldType.STRING.getDataType());
        final RecordSchema schema = new SimpleRecordSchema(List.of(fixedField));
        final MapRecord record = new MapRecord(schema, Map.of(
                "fixed", "value",
                "x-good", "ok",
                "x-bad", "a"
        ));

        final Collection<ValidationError> validationErrors = validator.validate(record, schema, "");

        assertEquals(1, validationErrors.size());
        final ValidationError error = validationErrors.iterator().next();
        assertEquals("/x-bad", error.getFieldName().orElse(null));
        assertTrue(error.getExplanation().contains("String length"));
    }

    @Test
    void testExplicitFieldsAreNotRevalidatedByPattern() {
        final RecordField templateField = new RecordField(
                "pattern",
                RecordFieldType.STRING.getDataType(),
                null,
                Collections.emptySet(),
                true,
                List.of(new StringLengthValidator(2, null))
        );

        final PatternPropertiesValidator validator = new PatternPropertiesValidator(List.of(
                new PatternPropertiesValidator.PatternPropertyDefinition(Pattern.compile("^x-"), templateField)
        ));

        final RecordField fixedField = new RecordField("fixed", RecordFieldType.STRING.getDataType());
        final RecordField explicitField = new RecordField("x-fixed", RecordFieldType.STRING.getDataType());
        final RecordSchema schema = new SimpleRecordSchema(List.of(fixedField, explicitField));
        final MapRecord record = new MapRecord(schema, Map.of(
                "fixed", "value",
                "x-fixed", "a",
                "x-dynamic", "ab"
        ));

        final Collection<ValidationError> validationErrors = validator.validate(record, schema, "");

        assertTrue(validationErrors.isEmpty(), "Expected no validation errors for explicit field");
    }
}
