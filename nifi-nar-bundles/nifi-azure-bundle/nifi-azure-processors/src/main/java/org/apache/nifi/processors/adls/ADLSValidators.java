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
package org.apache.nifi.processors.adls;

import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;

public class ADLSValidators {


    /**
     * Validates that a property is a valid umask, i.e. a short octal number that is not negative.
     */
    public static final Validator UMASK_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(final String subject, final String value, final ValidationContext context) {
            String reason = null;
            try {
                final short shortVal = Short.parseShort(value, 8);
                if (shortVal < 0) {
                    reason = "octal umask [" + value + "] cannot be negative";
                } else if (shortVal > 511) {
                    reason = "octal umask [" + value + "] is not a valid umask";
                }
            } catch (final NumberFormatException e) {
                reason = "[" + value + "] is not a valid short octal number";
            }
            return new ValidationResult.Builder().subject(subject).input(value).explanation(reason).valid(reason == null)
                    .build();
        }
    };

}
