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

package org.apache.nifi.record.path.validation;

import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.exception.RecordPathException;

public class RecordPathValidator implements Validator {

    @Override
    public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
        if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(input)) {
            return new ValidationResult.Builder()
                .input(input)
                .subject(subject)
                .valid(true)
                .explanation("Property uses Expression Language so no further validation is possible")
                .build();
        }

        try {
            RecordPath.compile(input);
            return new ValidationResult.Builder()
                .input(input)
                .subject(subject)
                .valid(true)
                .explanation("Valid RecordPath")
                .build();
        } catch (final RecordPathException e) {
            return new ValidationResult.Builder()
                .input(input)
                .subject(subject)
                .valid(false)
                .explanation("Property Value is not a valid RecordPath value: " + e.getMessage())
                .build();
        }
    }

}
