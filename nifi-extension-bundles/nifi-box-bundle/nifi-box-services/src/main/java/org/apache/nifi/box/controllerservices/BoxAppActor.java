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
package org.apache.nifi.box.controllerservices;

import org.apache.nifi.components.DescribedValue;

/**
 * An enumeration of the possible actors a Box App will act on behalf of.
 */
public enum BoxAppActor implements DescribedValue {
    SERVICE_ACCOUNT("service-account", "Service Account", "Make Box API calls with a service account associated with the app"),
    IMPERSONATED_USER("impersonated-user", "Impersonated User", "Make Box API call on behalf of the specified Box user with as-user header");

    private final String value;
    private final String displayName;
    private final String description;

    BoxAppActor(String value, String displayName, String description) {
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
