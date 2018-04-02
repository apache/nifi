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

package org.apache.nifi.mongodb;

import com.mongodb.MongoClientURI;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;

public class Validation {
    public static final Validator DOCUMENT_VALIDATOR = new Validator() {

        @Override
        public ValidationResult validate(String subject, String value, ValidationContext context) {
            final ValidationResult.Builder builder = new ValidationResult.Builder();
            builder.subject(subject).input(value);

            if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(value)) {
                return builder.valid(true).explanation("Contains Expression Language").build();
            }

            String reason = null;
            try {
                new MongoClientURI(value);
            } catch (final Exception e) {
                reason = e.getLocalizedMessage();
            }

            return builder.explanation(reason).valid(reason == null).build();
        }
    };
}
