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
 * A group that users can belong to.
 */
public class Group {

    private final String identifier;

    private final String name;

    private final Set<String> users;

    private Group(final GroupBuilder builder) {
        this.identifier = builder.identifier;
        this.name = builder.name;
        this.users = Collections.unmodifiableSet(new HashSet<>(builder.users));

        if (this.identifier == null || this.identifier.trim().isEmpty()) {
            throw new IllegalArgumentException("Identifier can not be null or empty");
        }

        if (this.name == null || this.name.trim().isEmpty()) {
            throw new IllegalArgumentException("Name can not be null or empty");
        }
    }

    /**
     * @return the identifier of the group
     */
    public String getIdentifier() {
        return identifier;
    }

    /**
     * @return the name of the group
     */
    public String getName() {
        return name;
    }

    /**
     * @return an unmodifiable set of user identifiers that belong to this group
     */
    public Set<String> getUsers() {
        return users;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }

        final Group other = (Group) obj;
        return Objects.equals(this.identifier, other.identifier);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(this.identifier);
    }

    @Override
    public String toString() {
        return String.format("identifier[%s], name[%s]", getIdentifier(), getName());
    }


    /**
     * Builder for creating Groups.
     */
    public static class GroupBuilder {

        private String identifier;
        private String name;
        private Set<String> users = new HashSet<>();
        private final boolean fromGroup;

        public GroupBuilder() {
            this.fromGroup = false;
        }

        /**
         * Initializes the builder with the state of the provided group. When using this constructor
         * the identifier field of the builder can not be changed and will result in an IllegalStateException
         * if attempting to do so.
         *
         * @param other the existing access policy to initialize from
         */
        public GroupBuilder(final Group other) {
            if (other == null) {
                throw new IllegalArgumentException("Provided group can not be null");
            }

            this.identifier = other.getIdentifier();
            this.name = other.getName();
            this.users.clear();
            this.users.addAll(other.getUsers());
            this.fromGroup = true;
        }

        /**
         * Sets the identifier of the builder.
         *
         * @param identifier the identifier
         * @return the builder
         * @throws IllegalStateException if this method is called when this builder was constructed from an existing Group
         */
        public GroupBuilder identifier(final String identifier) {
            if (fromGroup) {
                throw new IllegalStateException(
                        "Identifier can not be changed when initialized from an existing group");
            }

            this.identifier = identifier;
            return this;
        }

        /**
         * Sets the name of the builder.
         *
         * @param name the name
         * @return the builder
         */
        public GroupBuilder name(final String name) {
            this.name = name;
            return this;
        }

        /**
         * Adds all users from the provided set to the builder's set of users.
         *
         * @param users a set of users to add
         * @return the builder
         */
        public GroupBuilder addUsers(final Set<String> users) {
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
        public GroupBuilder addUser(final String user) {
            if (user != null) {
                this.users.add(user);
            }
            return this;
        }

        /**
         * Removes the given user from the builder's set of users.
         *
         * @param user the user to remove
         * @return the builder
         */
        public GroupBuilder removeUser(final String user) {
            if (user != null) {
                this.users.remove(user);
            }
            return this;
        }

        /**
         * Removes all users from the provided set from the builder's set of users.
         *
         * @param users the users to remove
         * @return the builder
         */
        public GroupBuilder removeUsers(final Set<String> users) {
            if (users != null) {
                this.users.removeAll(users);
            }
            return this;
        }

        /**
         * Clears the builder's set of users so that users is non-null with size 0.
         *
         * @return the builder
         */
        public GroupBuilder clearUsers() {
            this.users.clear();
            return this;
        }

        /**
         * @return a new Group constructed from the state of the builder
         */
        public Group build() {
            return new Group(this);
        }

    }

}
