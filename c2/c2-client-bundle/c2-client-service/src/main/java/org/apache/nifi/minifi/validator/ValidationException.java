/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.nifi.minifi.validator;

import java.util.List;
import java.util.Objects;
import org.apache.nifi.components.ValidationResult;

public class ValidationException extends IllegalStateException {
    private List<ValidationResult> validationResults;

    public ValidationException(String message, List<ValidationResult> details) {
        super(message);
        this.validationResults = details;
    }

    public List<ValidationResult> getValidationResults() {
        return validationResults;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ValidationException that = (ValidationException) o;
        return Objects.equals(validationResults, that.validationResults);
    }

    @Override
    public int hashCode() {
        return Objects.hash(validationResults);
    }

    @Override
    public String toString() {
        return "ValidationException{" +
                "validationResults=" + validationResults +
                "message=" + this.getMessage() +
                '}';
    }
}
