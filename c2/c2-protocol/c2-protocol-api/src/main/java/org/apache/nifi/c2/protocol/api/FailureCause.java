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

package org.apache.nifi.c2.protocol.api;

import java.util.List;
import java.util.Objects;
import org.apache.nifi.components.ValidationResult;

public class FailureCause {
    private List<ValidationResult> validationResults;
    private String exceptionMessage;
    private List<String> causedByMessages;

    public List<ValidationResult> getValidationResults() {
        return validationResults;
    }

    public void setValidationResults(List<ValidationResult> validationResults) {
        this.validationResults = validationResults;
    }

    public String getExceptionMessage() {
        return exceptionMessage;
    }

    public void setExceptionMessage(String exceptionMessage) {
        this.exceptionMessage = exceptionMessage;
    }

    public List<String> getCausedByMessages() {
        return causedByMessages;
    }

    public void setCausedByMessages(List<String> causedByMessages) {
        this.causedByMessages = causedByMessages;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FailureCause that = (FailureCause) o;
        return Objects.equals(validationResults, that.validationResults) && Objects.equals(exceptionMessage, that.exceptionMessage) && Objects.equals(causedByMessages, that.causedByMessages);
    }

    @Override
    public int hashCode() {
        return Objects.hash(validationResults, exceptionMessage, causedByMessages);
    }
}
