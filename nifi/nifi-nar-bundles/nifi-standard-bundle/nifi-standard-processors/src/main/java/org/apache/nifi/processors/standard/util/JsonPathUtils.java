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
package org.apache.nifi.processors.standard.util;

import com.jayway.jsonpath.InvalidPathException;
import com.jayway.jsonpath.JsonPath;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;

/**
 * Provides utilities for interacting with JsonPath expressions and results
 *
 * @see <a href="https://github.com/jayway/JsonPath">https://github.com/jayway/JsonPath</a>
 */
public class JsonPathUtils {

    public static final Validator JSON_PATH_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
            String error = null;
            try {
                JsonPath compile = JsonPath.compile(input);
            } catch (InvalidPathException ipe) {
                error = ipe.toString();
            }
            return new ValidationResult.Builder().valid(error == null).explanation(error).build();
        }
    };
}
