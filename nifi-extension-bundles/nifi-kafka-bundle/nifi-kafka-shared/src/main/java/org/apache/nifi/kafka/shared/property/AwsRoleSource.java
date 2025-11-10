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
package org.apache.nifi.kafka.shared.property;

import org.apache.nifi.components.DescribedValue;

/**
 * AWS Role Source strategy for AWS MSK IAM credentials selection
 */
public enum AwsRoleSource implements DescribedValue {
    DEFAULT_PROFILE("Default Profile", "Use the default AWS credentials provider chain to locate credentials."),
    SPECIFIED_PROFILE("Specified Profile", "Use the configured AWS Profile Name from the default credentials file."),
    SPECIFIED_ROLE("Specified Role", "Assume a specific AWS Role using the configured Role ARN and Session Name."),
    WEB_IDENTITY_TOKEN("Web Identity Provider", "Obtain AWS MSK IAM credentials using STS AssumeRoleWithWebIdentity and a configured OAuth2/OIDC token provider.");

    private final String displayName;
    private final String description;

    AwsRoleSource(final String displayName, final String description) {
        this.displayName = displayName;
        this.description = description;
    }

    @Override
    public String getValue() {
        return name();
    }

    @Override
    public String getDisplayName() {
        return displayName;
    }

    @Override
    public String getDescription() {
        return description;
    }
}
