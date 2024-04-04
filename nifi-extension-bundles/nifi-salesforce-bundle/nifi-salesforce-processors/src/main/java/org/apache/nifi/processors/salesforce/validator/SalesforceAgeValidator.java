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
package org.apache.nifi.processors.salesforce.validator;

import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;

import java.util.List;

import static org.apache.nifi.processors.salesforce.QuerySalesforceObject.AGE_FIELD;
import static org.apache.nifi.processors.salesforce.QuerySalesforceObject.INITIAL_AGE_FILTER;

public final class SalesforceAgeValidator {

    private SalesforceAgeValidator() {
    }

    public static List<ValidationResult> validate(ValidationContext validationContext, List<ValidationResult> results) {
        if (validationContext.getProperty(INITIAL_AGE_FILTER).isSet() && !validationContext.getProperty(AGE_FIELD).isSet()) {
            results.add(
                    new ValidationResult.Builder()
                            .subject(INITIAL_AGE_FILTER.getDisplayName())
                            .valid(false)
                            .explanation("it requires " + AGE_FIELD.getDisplayName() + " also to be set.")
                            .build()
            );
        }
        return results;
    }
}
