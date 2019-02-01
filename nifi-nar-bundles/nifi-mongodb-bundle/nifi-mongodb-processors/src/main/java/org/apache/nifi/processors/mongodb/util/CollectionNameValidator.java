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

/**
 * Validator for the collection name field. It is derived from the rules published in the documentation for
 * MongoDB here:
 *
 * https://docs.mongodb.com/manual/reference/limits/#restrictions-on-collection-names
 */
public class CollectionNameValidator implements Validator {
    private static final CollectionNameValidator INSTANCE = new CollectionNameValidator();

    private CollectionNameValidator() {

    }

    @Override
    public ValidationResult validate(String subject, String input, ValidationContext context) {
        ValidationResult.Builder builder = new ValidationResult.Builder();
        builder.subject(subject).input(input);

        ValidationResult elCheck = StandardValidators.NON_EMPTY_EL_VALIDATOR.validate(subject, input, context);

        if (!elCheck.isValid()) {
            builder.valid(false).explanation(elCheck.getExplanation());
        } else if (input.contains("$") && !elCheck.isValid()) {
            builder.valid(false).explanation("Collection name cannot have the \"$\" character in it.");
        } else if (input.startsWith("system.")) {
            builder.valid(false).explanation("The \"system.\" prefix is off limits to user-defined collections.");
        } else if (input.indexOf(Character.MIN_VALUE) >= 0) {
            builder.valid(false).explanation("Collection name cannot contain the null character.");
        } else {
            builder.valid(true);
        }

        return builder.build();
    }

    public static CollectionNameValidator getInstance() {
        return INSTANCE;
    }
}
