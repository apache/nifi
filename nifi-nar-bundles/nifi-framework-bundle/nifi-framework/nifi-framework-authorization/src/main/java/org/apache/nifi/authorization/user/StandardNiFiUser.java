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
package org.apache.nifi.authorization.user;

import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 * An implementation of NiFiUser.
 */
public class StandardNiFiUser implements NiFiUser {

    public static final String ANONYMOUS_IDENTITY = "anonymous";
    public static final StandardNiFiUser ANONYMOUS = new Builder().identity(ANONYMOUS_IDENTITY).anonymous(true).build();

    private final String identity;
    private final Set<String> groups;
    private final NiFiUser chain;
    private final String clientAddress;
    private final boolean isAnonymous;

    private StandardNiFiUser(final Builder builder) {
        this.identity = builder.identity;
        this.groups = builder.groups == null ? null : Collections.unmodifiableSet(builder.groups);
        this.chain = builder.chain;
        this.clientAddress = builder.clientAddress;
        this.isAnonymous = builder.isAnonymous;
    }

    /**
     * This static builder allows the chain and clientAddress to be populated without allowing calling code to provide a non-anonymous identity of the anonymous user.
     *
     * @param chain the proxied entities in {@see NiFiUser} form
     * @param clientAddress the address the request originated from
     * @return an anonymous user instance with the identity "anonymous"
     */
    public static StandardNiFiUser populateAnonymousUser(NiFiUser chain, String clientAddress) {
        return new Builder().identity(ANONYMOUS_IDENTITY).chain(chain).clientAddress(clientAddress).anonymous(true).build();
    }

    @Override
    public String getIdentity() {
        return identity;
    }

    @Override
    public Set<String> getGroups() {
        return groups;
    }

    @Override
    public NiFiUser getChain() {
        return chain;
    }

    @Override
    public boolean isAnonymous() {
        return isAnonymous;
    }

    @Override
    public String getClientAddress() {
        return clientAddress;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (!(obj instanceof NiFiUser)) {
            return false;
        }

        final NiFiUser other = (NiFiUser) obj;
        return Objects.equals(this.identity, other.getIdentity());
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 53 * hash + Objects.hashCode(this.identity);
        return hash;
    }

    @Override
    public String toString() {
        final String formattedGroups;
        if (groups == null) {
            formattedGroups = "none";
        } else {
            formattedGroups = StringUtils.join(groups, ", ");
        }

        return String.format("identity[%s], groups[%s]", getIdentity(), formattedGroups);
    }

    /**
     * Builder for a StandardNiFiUser
     */
    public static class Builder {

        private String identity;
        private Set<String> groups;
        private NiFiUser chain;
        private String clientAddress;
        private boolean isAnonymous = false;

        /**
         * Sets the identity.
         *
         * @param identity the identity string for the user (i.e. "Andy" or "CN=alopresto, OU=Apache NiFi")
         * @return the builder
         */
        public Builder identity(final String identity) {
            this.identity = identity;
            return this;
        }

        /**
         * Sets the groups.
         *
         * @param groups the user groups
         * @return the builder
         */
        public Builder groups(final Set<String> groups) {
            this.groups = groups;
            return this;
        }

        /**
         * Sets the chain.
         *
         * @param chain the proxy chain that leads to this users
         * @return the builder
         */
        public Builder chain(final NiFiUser chain) {
            this.chain = chain;
            return this;
        }

        /**
         * Sets the client address.
         *
         * @param clientAddress the source address of the request
         * @return the builder
         */
        public Builder clientAddress(final String clientAddress) {
            this.clientAddress = clientAddress;
            return this;
        }

        /**
         * Sets whether this user is the canonical "anonymous" user
         *
         * @param isAnonymous true to represent the canonical "anonymous" user
         * @return the builder
         */
        private Builder anonymous(final boolean isAnonymous) {
            this.isAnonymous = isAnonymous;
            return this;
        }

        /**
         * @return builds a StandardNiFiUser from the current state of the builder
         */
        public StandardNiFiUser build() {
            return new StandardNiFiUser(this);
        }
    }
}
