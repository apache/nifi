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
package org.apache.nifi.authorization;

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * A user to create authorization policies for.
 */
public class User {

    private final String identifier;

    private final String identity;

    private final Set<String> groups;

    /**
     * Constructs a new user with the given identifier and identity, and no groups.
     *
     * @param identifier the unique identifier of the user
     * @param identity the identity string of the user
     */
    public User(final String identifier, final String identity) {
        this(identifier, identity, null);
    }

    /**
     * Constructs a new user with the given identifier, identity, and groups.
     *
     * @param identifier the unique identifier of the user
     * @param identity the identity string of the user
     * @param groups the ids of the groups the user belongs to
     */
    public User(final String identifier, final String identity, final Set<String> groups) {
        if (identifier == null || identifier.trim().isEmpty()) {
            throw new IllegalArgumentException("Identifier can not be null or empty");
        }

        if (identity == null || identity.trim().isEmpty()) {
            throw new IllegalArgumentException("Identity can not be null or empty");
        }

        this.identifier = identifier;
        this.identity = identity;
        this.groups = (groups == null ? Collections.unmodifiableSet(new HashSet<>()) : Collections.unmodifiableSet(groups));
    }

    /**
     * @return the identifier of the user
     */
    public String getIdentifier() {
        return identifier;
    }

    /**
     * @return the identity string of the user
     */
    public String getIdentity() {
        return identity;
    }

    /**
     * @return the group ids of the user
     */
    public Set<String> getGroups() {
        return groups;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }

        final User other = (User) obj;
        if (!Objects.equals(this.identifier, other.identifier)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 53 * hash + Objects.hashCode(this.identifier);
        return hash;
    }

    @Override
    public String toString() {
        return String.format("identifier[%s], identity[%s]", getIdentifier(), getIdentity(), ", ");
    }

}
