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

package org.apache.nifi.serialization;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;

public class DataTypeValidator implements Validator {
    private static final Set<String> validValues;
    private static final Set<String> allowsFormatting;

    static {
        final Set<String> values = new HashSet<>();
        values.add("string");
        values.add("boolean");
        values.add("byte");
        values.add("char");
        values.add("int");
        values.add("long");
        values.add("float");
        values.add("double");
        values.add("time");
        values.add("date");
        values.add("timestamp");
        validValues = Collections.unmodifiableSet(values);

        final Set<String> formattable = new HashSet<>();
        formattable.add("date");
        formattable.add("time");
        formattable.add("timestmap");
        allowsFormatting = Collections.unmodifiableSet(formattable);
    }

    @Override
    public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
        final String[] splits = input.split("\\:");

        final boolean valid;
        if (splits.length == 2) {
            final String type = splits[0].trim();
            if (validValues.contains(type)) {
                if (allowsFormatting.contains(splits[0].trim())) {
                    valid = true;
                } else {
                    valid = false;
                }
            } else {
                valid = false;
            }
        } else {
            valid = validValues.contains(input.trim());
        }

        return new ValidationResult.Builder()
            .subject(subject)
            .input(input)
            .valid(valid)
            .explanation("Valid values for this property are: " + validValues
                + ", where date, time, and timestamp may optionally contain a format (e.g., date:MM-dd-yyyy)")
            .build();
    }
}
