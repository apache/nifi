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

package org.apache.nifi.schemaregistry.services;

import org.apache.avro.Schema;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.serialization.record.SchemaIdentifier;

public class AvroSchemaValidator implements Validator {

    @Override
    public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
        if (context.isExpressionLanguagePresent(input)) {
            return new ValidationResult.Builder()
                .input(input)
                .subject(subject)
                .valid(true)
                .explanation("Expression Language is present")
                .build();
        }

        try {
            final Schema avroSchema = new Schema.Parser().parse(input);
            AvroTypeUtil.createSchema(avroSchema, input, SchemaIdentifier.EMPTY);

            return new ValidationResult.Builder()
                .input(input)
                .subject(subject)
                .valid(true)
                .explanation("Schema is valid")
                .build();
        } catch (final Exception e) {
            return new ValidationResult.Builder()
                .input(input)
                .subject(subject)
                .valid(false)
                .explanation("Not a valid Avro Schema: " + e.getMessage())
                .build();
        }
    }

}
