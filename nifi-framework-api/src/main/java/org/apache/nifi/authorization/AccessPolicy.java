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

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * Defines a policy for a set of userIdentifiers to perform a set of actions on a given resource.
 */
public class AccessPolicy {

    private final String identifier;

    private final String resource;

    private final Set<String> users;

    private final Set<String> groups;

    private final RequestAction action;

    private AccessPolicy(final Builder builder) {
        this.identifier = builder.identifier;
        this.resource = builder.resource;
        this.action = builder.action;
        this.users = Collections.unmodifiableSet(new HashSet<>(builder.users));
        this.groups = Collections.unmodifiableSet(new HashSet<>(builder.groups));

        if (this.identifier == null || this.identifier.trim().isEmpty()) {
            throw new IllegalArgumentException("Identifier can not be null or empty");
        }

        if (this.resource == null) {
            throw new IllegalArgumentException("Resource can not be null");
        }

        if (this.action == null) {
            throw new IllegalArgumentException("Action can not be null");
        }
    }

    /**
     * @return the identifier for this policy
     */
    public String getIdentifier() {
        return identifier;
    }

    /**
     * @return the resource for this policy
     */
    public String getResource() {
        return resource;
    }

    /**
     * @return an unmodifiable set of user ids for this policy
     */
    public Set<String> getUsers() {
        return users;
    }

    /**
     * @return an unmodifiable set of group ids for this policy
     */
    public Set<String> getGroups() {
        return groups;
    }

    /**
     * @return the action for this policy
     */
    public RequestAction getAction() {
        return action;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }

        final AccessPolicy other = (AccessPolicy) obj;
        return Objects.equals(this.identifier, other.identifier);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(this.identifier);
    }

    @Override
    public String toString() {
        return String.format("identifier[%s], resource[%s], users[%s], groups[%s], action[%s]",
                getIdentifier(), getResource(), getUsers(), getGroups(), getAction());
    }

    /**
     * Builder for Access Policies.
     */
    public static class Builder {

        private String identifier;
        private String resource;
        private RequestAction action;
        private Set<String> users = new HashSet<>();
        private Set<String> groups = new HashSet<>();
        private final boolean fromPolicy;

        /**
         * Default constructor for building a new AccessPolicy.
         */
        public Builder() {
            this.fromPolicy = false;
        }

        /**
         * Initializes the builder with the state of the provided policy. When using this constructor
         * the identifier field of the builder can not be changed and will result in an IllegalStateException
         * if attempting to do so.
         *
         * @param other the existing access policy to initialize from
         */
        public Builder(final AccessPolicy other) {
            if (other == null) {
                throw new IllegalArgumentException("Can not initialize builder with a null access policy");
            }

            this.identifier = other.getIdentifier();
            this.resource = other.getResource();
            this.action = other.getAction();
            this.users.clear();
            this.users.addAll(other.getUsers());
            this.groups.clear();
            this.groups.addAll(other.getGroups());
            this.fromPolicy = true;
        }

        /**
         * Sets the identifier of the builder.
         *
         * @param identifier the identifier to set
         * @return the builder
         * @throws IllegalStateException if this method is called when this builder was constructed from an existing Policy
         */
        public Builder identifier(final String identifier) {
            if (fromPolicy) {
                throw new IllegalStateException(
                        "Identifier can not be changed when initialized from an existing policy");
            }

            this.identifier = identifier;
            return this;
        }

        /**
         * Sets the identifier of the builder to a random UUID.
         *
         * @return the builder
         * @throws IllegalStateException if this method is called when this builder was constructed from an existing Policy
         */
        public Builder identifierGenerateRandom() {
            if (fromPolicy) {
                throw new IllegalStateException(
                        "Identifier can not be changed when initialized from an existing policy");
            }

            this.identifier = UUID.randomUUID().toString();
            return this;
        }

        /**
         * Sets the identifier of the builder with a UUID generated from the specified seed string.
         *
         * @return the builder
         * @throws IllegalStateException if this method is called when this builder was constructed from an existing Policy
         */
        public Builder identifierGenerateFromSeed(final String seed) {
            if (fromPolicy) {
                throw new IllegalStateException(
                        "Identifier can not be changed when initialized from an existing policy");
            }
            if (seed == null) {
                throw new IllegalArgumentException("Cannot seed the policy identifier with a null value.");
            }

            this.identifier = UUID.nameUUIDFromBytes(seed.getBytes(StandardCharsets.UTF_8)).toString();
            return this;
        }

        /**
         * Sets the resource of the builder.
         *
         * @param resource the resource to set
         * @return the builder
         */
        public Builder resource(final String resource) {
            this.resource = resource;
            return this;
        }

        /**
         * Adds all the users from the provided set to the builder's set of users.
         *
         * @param users the users to add
         * @return the builder
         */
        public Builder addUsers(final Set<String> users) {
            if (users != null) {
                this.users.addAll(users);
            }
            return this;
        }

        /**
         * Adds the given user to the builder's set of users.
         *
         * @param user the user to add
         * @return the builder
         */
        public Builder addUser(final String user) {
            if (user != null) {
                this.users.add(user);
            }
            return this;
        }

        /**
         * Removes all users in the provided set from the builder's set of users.
         *
         * @param users the users to remove
         * @return the builder
         */
        public Builder removeUsers(final Set<String> users) {
            if (users != null) {
                this.users.removeAll(users);
            }
            return this;
        }

        /**
         * Removes the provided user from the builder's set of users.
         *
         * @param user the user to remove
         * @return the builder
         */
        public Builder removeUser(final String user) {
            if (user != null) {
                this.users.remove(user);
            }
            return this;
        }

        /**
         * Clears the builder's set of users so that it is non-null and size == 0.
         *
         * @return the builder
         */
        public Builder clearUsers() {
            this.users.clear();
            return this;
        }

        /**
         * Adds all the groups from the provided set to the builder's set of groups.
         *
         * @param groups the groups to add
         * @return the builder
         */
        public Builder addGroups(final Set<String> groups) {
            if (groups != null) {
                this.groups.addAll(groups);
            }
            return this;
        }

        /**
         * Adds the given group to the builder's set of groups.
         *
         * @param group the group to add
         * @return the builder
         */
        public Builder addGroup(final String group) {
            if (group != null) {
                this.groups.add(group);
            }
            return this;
        }

        /**
         * Removes all groups in the provided set from the builder's set of groups.
         *
         * @param groups the groups to remove
         * @return the builder
         */
        public Builder removeGroups(final Set<String> groups) {
            if (groups != null) {
                this.groups.removeAll(groups);
            }
            return this;
        }

        /**
         * Removes the provided groups from the builder's set of groups.
         *
         * @param group the group to remove
         * @return the builder
         */
        public Builder removeGroup(final String group) {
            if (group != null) {
                this.groups.remove(group);
            }
            return this;
        }

        /**
         * Clears the builder's set of groups so that it is non-null and size == 0.
         *
         * @return the builder
         */
        public Builder clearGroups() {
            this.groups.clear();
            return this;
        }

        /**
         * Sets the action for this builder.
         *
         * @param action the action to set
         * @return the builder
         */
        public Builder action(final RequestAction action) {
            this.action = action;
            return this;
        }

        /**
         * @return a new AccessPolicy constructed from the state of the builder
         */
        public AccessPolicy build() {
            return new AccessPolicy(this);
        }
    }

}
