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

package org.apache.nifi.csv;

import java.util.HashSet;
import java.util.Set;

import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;

public class SingleCharacterValidator implements Validator {
    private static final Set<String> illegalChars = new HashSet<>();
    static {
        illegalChars.add("\r");
        illegalChars.add("\n");
    }

    @Override
    public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
        final String unescaped = CSVUtils.unescape(input);
        if (unescaped.length() != 1) {
            return new ValidationResult.Builder()
                .input(input)
                .subject(subject)
                .valid(false)
                .explanation("Value must be exactly 1 character but was " + input.length() + " in length")
                .build();
        }

        if (illegalChars.contains(input)) {
            return new ValidationResult.Builder()
                .input(input)
                .subject(subject)
                .valid(false)
                .explanation(input + " is not a valid character for this property")
                .build();
        }

        return new ValidationResult.Builder()
            .input(input)
            .subject(subject)
            .valid(true)
            .build();
    }

}
