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
package org.apache.nifi.hbase.validate;

import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.processor.util.StandardValidators;

public class ConfigFilesValidator implements Validator {

    @Override
    public ValidationResult validate(final String subject, final String value, final ValidationContext context) {
        final String[] filenames = value.split(",");
        for (final String filename : filenames) {
            final ValidationResult result = StandardValidators.FILE_EXISTS_VALIDATOR.validate(subject, filename.trim(), context);
            if (!result.isValid()) {
                return result;
            }
        }

        return new ValidationResult.Builder().subject(subject).input(value).valid(true).build();
    }
}
