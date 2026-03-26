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

package org.apache.nifi.components.validation;

import org.apache.nifi.components.ValidationResult;

import java.util.Collection;
import java.util.Collections;

public class ValidationState {
    private final ValidationStatus status;
    private final Collection<ValidationResult> validationErrors;

    public ValidationState(final ValidationStatus status, final Collection<ValidationResult> validationResults) {
        this.status = status;
        // Ensure that if we are provided any valid results, they are filtered out because we only want to store validation failures.
        this.validationErrors = removeValidResults(validationResults);
    }

    private Collection<ValidationResult> removeValidResults(final Collection<ValidationResult> validationResults) {
        if (validationResults.isEmpty()) {
            return Collections.emptyList();
        }

        return validationResults.stream()
            .filter(vr -> !vr.isValid())
            .toList();
    }

    public ValidationStatus getStatus() {
        return status;
    }

    public Collection<ValidationResult> getValidationErrors() {
        return validationErrors;
    }

    @Override
    public String toString() {
        return status.toString();
    }
}
