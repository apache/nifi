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

package org.apache.nifi.components.connector.secrets;

import org.apache.nifi.components.connector.Secret;

import java.util.Objects;

public class StandardSecret implements Secret {
    private final String providerName;
    private final String groupName;
    private final String name;
    private final String description;
    private final String value;

    private StandardSecret(final Builder builder) {
        this.providerName = builder.providerName;
        this.groupName = builder.groupName;
        this.name = builder.name;
        this.description = builder.description;
        this.value = builder.value;
    }

    @Override
    public String getProviderName() {
        return providerName;
    }

    @Override
    public String getGroupName() {
        return groupName;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "StandardSecret[providerName=%s, groupName=%s, name=%s, description=%s]".formatted(
             providerName, groupName, name, description);
    }

    @Override
    public int hashCode() {
        return Objects.hash(providerName, groupName, name, description);
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final StandardSecret other = (StandardSecret) obj;
        return Objects.equals(this.providerName, other.providerName)
            && Objects.equals(this.groupName, other.groupName)
            &&  Objects.equals(this.name, other.name)
            &&  Objects.equals(this.description, other.description);
    }

    public static class Builder {
        private String providerName;
        private String groupName;
        private String name;
        private String description;
        private String value;

        public Builder providerName(String providerName) {
            this.providerName = providerName;
            return this;
        }

        public Builder groupName(String groupName) {
            this.groupName = groupName;
            return this;
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder description(String description) {
            this.description = description;
            return this;
        }

        public Builder value(String value) {
            this.value = value;
            return this;
        }

        public StandardSecret build() {
            return new StandardSecret(this);
        }
    }

}
