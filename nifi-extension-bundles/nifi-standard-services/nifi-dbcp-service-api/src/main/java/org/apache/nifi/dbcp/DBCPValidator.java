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
package org.apache.nifi.dbcp;

import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.util.FormatUtils;

import java.util.regex.Pattern;

public class DBCPValidator {

    public static final Validator CUSTOM_TIME_PERIOD_VALIDATOR = new Validator() {
        private final Pattern TIME_DURATION_PATTERN = Pattern.compile(FormatUtils.TIME_DURATION_REGEX);

        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
            if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(input)) {
                return new ValidationResult.Builder().subject(subject).input(input).explanation("Expression Language Present").valid(true).build();
            }

            if (input == null) {
                return new ValidationResult.Builder().subject(subject).input(input).valid(false).explanation("Time Period cannot be null").build();
            }
            if (TIME_DURATION_PATTERN.matcher(input.toLowerCase()).matches() || input.equals("-1")) {
                return new ValidationResult.Builder().subject(subject).input(input).valid(true).build();
            } else {
                return new ValidationResult.Builder()
                        .subject(subject)
                        .input(input)
                        .valid(false)
                        .explanation("Must be of format <duration> <TimeUnit> where <duration> is a "
                                + "non-negative integer and TimeUnit is a supported Time Unit, such "
                                + "as: nanos, millis, secs, mins, hrs, days")
                        .build();
            }
        }
    };
}
