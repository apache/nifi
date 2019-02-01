/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.nifi.processors.mongodb.util;

import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Validator class for MongoDB database names. It is derived from the more restrictive rules for
 * database names on Windows to ensure that this works on all of our supported platforms. See this for more details:
 *
 * https://docs.mongodb.com/manual/reference/limits/#restrictions-on-db-names
 */
public class DatabaseNameValidator implements Validator {
    private static final DatabaseNameValidator INSTANCE = new DatabaseNameValidator();
    private static final Pattern STRICT_NAME_VALIDATOR = Pattern.compile("[\\/\\\\. \\\"\\$\\*\\<\\>\\:\\|\\?]");

    private DatabaseNameValidator() {

    }

    @Override
    public ValidationResult validate(String subject, String input, ValidationContext context) {
        ValidationResult elCheck = StandardValidators.NON_EMPTY_EL_VALIDATOR.validate(subject, input, context);

        ValidationResult.Builder builder = new ValidationResult.Builder()
            .subject(subject)
            .input(input);
        Matcher matcher = STRICT_NAME_VALIDATOR.matcher(input);
        if (elCheck.isValid()) {
            builder.valid(true);
        } else if (matcher.find()) {
            builder
                .valid(false)
                .explanation("Found an illegal character in the name.");
        } else if (input.length() > 64) {
            builder
                .valid(false)
                .explanation("Name cannot be longer than 64 characters.");
        } else {
            builder.valid(true);
        }

        return builder.build();
    }

    public static DatabaseNameValidator getInstance() {
        return INSTANCE;
    }
}
