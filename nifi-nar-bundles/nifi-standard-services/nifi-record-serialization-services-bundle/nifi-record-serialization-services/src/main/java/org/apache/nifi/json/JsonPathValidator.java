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

package org.apache.nifi.json;

import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;

import com.jayway.jsonpath.JsonPath;

public class JsonPathValidator implements Validator {

    @Override
    public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
        try {
            JsonPath.compile(input);
        } catch (final Exception e) {
            return new ValidationResult.Builder()
                .subject(subject)
                .input(input)
                .valid(false)
                .explanation("Invalid JSON Path Expression: " + e.getMessage())
                .build();
        }

        return new ValidationResult.Builder()
            .subject(subject)
            .input(input)
            .valid(true)
            .build();
    }

}
