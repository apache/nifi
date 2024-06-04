/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.nifi.github;

import org.apache.nifi.components.DescribedValue;

/**
 * Enumeration of authentication types for the GitHub client.
 */
public enum GitHubAuthenticationType implements DescribedValue {

    NONE("None", "Authentication disabled"),

    PERSONAL_ACCESS_TOKEN("Personal Access Token", "User-based Personal Access Token"),

    APP_INSTALLATION("App Installation", "App-based Installation Token provisioning from a Private Key");

    private final String displayName;

    private final String description;

    GitHubAuthenticationType(final String displayName, final String description) {
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
