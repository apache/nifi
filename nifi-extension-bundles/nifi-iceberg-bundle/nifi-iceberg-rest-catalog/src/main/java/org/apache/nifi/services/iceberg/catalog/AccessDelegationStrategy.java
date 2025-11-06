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
package org.apache.nifi.services.iceberg.catalog;

import org.apache.nifi.components.DescribedValue;

/**
 * Enumeration of supported strategies for REST Catalog X-Iceberg-Access-Delegation request header
 */
enum AccessDelegationStrategy implements DescribedValue {
    DISABLED("disabled", "Disabled", "Disables sending the X-Iceberg-Access-Delegation request header"),

    VENDED_CREDENTIALS("vended-credentials", "Vended Credentials", "Request vended credentials from REST Catalog");

    private final String value;

    private final String displayName;

    private final String description;

    AccessDelegationStrategy(final String value, final String displayName, final String description) {
        this.value = value;
        this.displayName = displayName;
        this.description = description;
    }

    @Override
    public String getValue() {
        return value;
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
