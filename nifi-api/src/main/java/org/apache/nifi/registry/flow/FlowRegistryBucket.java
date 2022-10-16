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
package org.apache.nifi.registry.flow;

public class FlowRegistryBucket {
    private String identifier;
    private String name;
    private String description;
    private long createdTimestamp;
    private FlowRegistryPermissions permissions;

    public String getIdentifier() {
        return identifier;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public long getCreatedTimestamp() {
        return createdTimestamp;
    }

    public FlowRegistryPermissions getPermissions() {
        return permissions;
    }

    public void setIdentifier(final String identifier) {
        this.identifier = identifier;
    }

    public void setName(final String name) {
        this.name = name;
    }

    public void setDescription(final String description) {
        this.description = description;
    }

    public void setCreatedTimestamp(final long createdTimestamp) {
        this.createdTimestamp = createdTimestamp;
    }

    public void setPermissions(final FlowRegistryPermissions permissions) {
        this.permissions = permissions;
    }

}
