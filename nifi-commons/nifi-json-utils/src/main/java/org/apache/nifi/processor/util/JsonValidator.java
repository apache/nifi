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

package org.apache.nifi.processor.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;

import java.util.List;
import java.util.Map;

public class JsonValidator implements Validator {
    public static final JsonValidator INSTANCE = new JsonValidator();

    @Override
    public ValidationResult validate(String subject, String input, ValidationContext context) {
        ObjectMapper mapper = new ObjectMapper();
        if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(input)) {
            return new ValidationResult.Builder().subject(subject).input(input).explanation("Expression Language Present").valid(true).build();
        }

        try {
            Class clz = input.startsWith("[") ? List.class : Map.class;
            mapper.readValue(input, clz);
        } catch (Exception e) {
            return new ValidationResult.Builder().subject(subject).input(input).valid(false)
                    .explanation(subject + " is not a valid JSON representation due to " + e.getLocalizedMessage())
                    .build();
        }

        return new ValidationResult.Builder().subject(subject).input(input).valid(true).build();
    }
}
