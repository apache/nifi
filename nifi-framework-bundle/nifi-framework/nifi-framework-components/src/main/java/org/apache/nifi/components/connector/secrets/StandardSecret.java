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

import org.apache.nifi.authorization.Resource;
import org.apache.nifi.authorization.resource.Authorizable;

import java.util.Objects;

public class StandardSecret implements AuthorizableSecret {
    private final String providerId;
    private final String providerName;
    private final String groupName;
    private final String name;
    private final String fullyQualifiedName;
    private final String description;
    private final String value;
    private final Authorizable authorizable;

    private StandardSecret(final Builder builder) {
        this.providerId = builder.providerId;
        this.providerName = builder.providerName;
        this.groupName = builder.groupName;
        this.name = builder.name;
        this.description = builder.description;
        this.value = builder.value;
        this.authorizable = builder.authorizable;
        this.fullyQualifiedName = builder.fullyQualifiedName == null ? groupName + "." + name : builder.fullyQualifiedName;
    }

    @Override
    public String getProviderId() {
        return providerId;
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
    public String getFullyQualifiedName() {
        return fullyQualifiedName;
    }

    @Override
    public String toString() {
        return "StandardSecret[providerName=%s, groupName=%s, name=%s]".formatted(providerName, groupName, name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(providerName, groupName, name);
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
            &&  Objects.equals(this.name, other.name);
    }

    @Override
    public Authorizable getParentAuthorizable() {
        return authorizable.getParentAuthorizable();
    }

    @Override
    public Resource getResource() {
        return authorizable.getResource();
    }

    public static class Builder {
        private String providerId;
        private String providerName;
        private String groupName;
        private String name;
        private String fullyQualifiedName;
        private String description;
        private String value;
        private Authorizable authorizable;

        public Builder providerId(final String providerId) {
            this.providerId = providerId;
            return this;
        }

        public Builder providerName(final String providerName) {
            this.providerName = providerName;
            return this;
        }

        public Builder groupName(final String groupName) {
            this.groupName = groupName;
            return this;
        }

        public Builder name(final String name) {
            this.name = name;
            return this;
        }

        public Builder description(final String description) {
            this.description = description;
            return this;
        }

        public Builder value(final String value) {
            this.value = value;
            return this;
        }

        public Builder authorizable(final Authorizable authorizable) {
            this.authorizable = authorizable;
            return this;
        }

        public Builder fullyQualifiedName(final String fullyQualifiedName) {
            this.fullyQualifiedName = fullyQualifiedName;
            return this;
        }

        public StandardSecret build() {
            if (groupName == null) {
                throw new IllegalStateException("Group name is required");
            }
            if (authorizable == null) {
                throw new IllegalStateException("Authorizable is required");
            }
            if (name == null || name.isEmpty()) {
                throw new IllegalStateException("Secret name must be provided");
            }
            if (fullyQualifiedName == null || fullyQualifiedName.isEmpty()) {
                throw new IllegalStateException("Secret fully qualified name must be provided");
            }
            if ((providerId == null || providerId.isEmpty()) && (providerName == null || providerName.isEmpty())) {
                throw new IllegalStateException("Either Secret provider ID or provider name must be provided");
            }

            return new StandardSecret(this);
        }
    }

}
