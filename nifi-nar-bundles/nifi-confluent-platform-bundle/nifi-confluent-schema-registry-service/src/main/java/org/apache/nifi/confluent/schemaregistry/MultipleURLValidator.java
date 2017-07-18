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

package org.apache.nifi.confluent.schemaregistry;

import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.processor.util.StandardValidators;

public class MultipleURLValidator implements Validator {

    @Override
    public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
        final String[] splits = input.split(",");
        if (splits.length == 0) {
            return new ValidationResult.Builder()
                .subject(subject)
                .input(input)
                .valid(false)
                .explanation("At least one URL must be specified")
                .build();
        }

        for (final String split : splits) {
            final String url = split.trim();

            final ValidationResult result = StandardValidators.URL_VALIDATOR.validate(subject, url, context);
            if (result != null && !result.isValid()) {
                return result;
            }
        }

        return new ValidationResult.Builder().subject(subject).input(input).valid(true).build();
    }

}
