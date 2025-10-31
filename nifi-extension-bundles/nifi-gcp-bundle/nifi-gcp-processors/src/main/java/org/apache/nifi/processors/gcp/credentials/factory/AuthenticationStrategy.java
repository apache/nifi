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
package org.apache.nifi.processors.gcp.credentials.factory;

import org.apache.nifi.components.DescribedValue;

import java.util.Arrays;
import java.util.Optional;

/**
 * Supported authentication strategies for configuring GCP credentials.
 */
public enum AuthenticationStrategy implements DescribedValue {
    APPLICATION_DEFAULT("Application Default Credentials", "Use Google Application Default Credentials such as the GOOGLE_APPLICATION_CREDENTIALS environment variable or gcloud configuration."),
    SERVICE_ACCOUNT_JSON_FILE("Service Account Credentials (Json File)", "Use a Service Account key stored in a JSON file."),
    SERVICE_ACCOUNT_JSON("Service Account Credentials (Json Value)", "Use a Service Account key provided directly as JSON."),
    WORKLOAD_IDENTITY_FEDERATION("Workload Identity Federation", "Exchange workload identity tokens using Google Identity Pool credentials."),
    COMPUTE_ENGINE("Compute Engine Credentials", "Use the Compute Engine service account available to the NiFi instance.");

    private final String displayName;
    private final String description;

    AuthenticationStrategy(final String displayName, final String description) {
        this.displayName = displayName;
        this.description = description;
    }

    @Override
    public String getValue() {
        return displayName;
    }

    @Override
    public String getDisplayName() {
        return displayName;
    }

    @Override
    public String getDescription() {
        return description;
    }

    public static Optional<AuthenticationStrategy> fromValue(final String value) {
        if (value == null) {
            return Optional.empty();
        }

        return Arrays.stream(values())
                .filter(strategy -> strategy.getValue().equals(value))
                .findFirst();
    }
}
