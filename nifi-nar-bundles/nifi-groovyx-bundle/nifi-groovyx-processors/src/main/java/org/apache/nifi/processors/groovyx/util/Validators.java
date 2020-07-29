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
package org.apache.nifi.processors.groovyx.util;

import java.io.File;

import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;

/***
 * class with validators
 */

public class Validators {
    /**
     * differs from standard file exists validator by supporting expression language values. TODO: maybe there is a bug in standard validator?
     */
    public static Validator createFileExistsAndReadableValidator() {
        return (subject, input, context) -> {
            final String substituted;
            try {
                substituted = context.newPropertyValue(input).evaluateAttributeExpressions().getValue();
            } catch (final Exception e) {
                return new ValidationResult.Builder()
                        .subject(subject)
                        .input(input)
                        .valid(false)
                        .explanation("Not a valid Expression Language value: " + e.getMessage())
                        .build();
            }

            final File file = new File(substituted);
            final boolean valid = file.exists() && file.canRead();
            final String explanation = valid ? null : "File " + file + " does not exist or cannot be read";
            return new ValidationResult.Builder()
                    .subject(subject)
                    .input(input)
                    .valid(valid)
                    .explanation(explanation)
                    .build();
        };
    }
}
