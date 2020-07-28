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

public class DisabledServiceValidationResult extends ValidationResult {
    private String serviceId;

    public DisabledServiceValidationResult(final String subject, final String serviceId) {
        this(subject, serviceId, "Controller Service with ID " + serviceId + " is disabled");
    }

    public DisabledServiceValidationResult(final String subject, final String serviceId, final String explanation) {
        super(new ValidationResult.Builder()
            .input(serviceId)
            .subject(subject)
            .valid(false)
            .explanation(explanation));

        this.serviceId = serviceId;
    }

    public String getControllerServiceIdentifier() {
        return serviceId;
    }
}
