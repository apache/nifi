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
import java.util.Objects;
import java.util.UUID;

/**
 * A user to create authorization policies for.
 */
public class User {

    private final String identifier;

    private final String identity;

    private User(final Builder builder) {
        this.identifier = builder.identifier;
        this.identity = builder.identity;

        if (identifier == null || identifier.trim().isEmpty()) {
            throw new IllegalArgumentException("Identifier can not be null or empty");
        }

        if (identity == null || identity.trim().isEmpty()) {
            throw new IllegalArgumentException("Identity can not be null or empty");
        }

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

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }

        final User other = (User) obj;
        return Objects.equals(this.identifier, other.identifier);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(this.identifier);
    }

    @Override
    public String toString() {
        return String.format("identifier[%s], identity[%s]", getIdentifier(), getIdentity());
    }

    /**
     * Builder for Users.
     */
    public static class Builder {

        private String identifier;
        private String identity;
        private final boolean fromUser;

        /**
         * Default constructor for building a new User.
         */
        public Builder() {
            this.fromUser = false;
        }

        /**
         * Initializes the builder with the state of the provided user. When using this constructor
         * the identifier field of the builder can not be changed and will result in an IllegalStateException
         * if attempting to do so.
         *
         * @param other the existing user to initialize from
         */
        public Builder(final User other) {
            if (other == null) {
                throw new IllegalArgumentException("Provided user can not be null");
            }

            this.identifier = other.getIdentifier();
            this.identity = other.getIdentity();
            this.fromUser = true;
        }

        /**
         * Sets the identifier of the builder.
         *
         * @param identifier the identifier to set
         * @return the builder
         * @throws IllegalStateException if this method is called when this builder was constructed from an existing User
         */
        public Builder identifier(final String identifier) {
            if (fromUser) {
                throw new IllegalStateException(
                        "Identifier can not be changed when initialized from an existing user");
            }

            this.identifier = identifier;
            return this;
        }

        /**
         * Sets the identifier of the builder to a random UUID.
         *
         * @return the builder
         * @throws IllegalStateException if this method is called when this builder was constructed from an existing User
         */
        public Builder identifierGenerateRandom() {
            if (fromUser) {
                throw new IllegalStateException(
                        "Identifier can not be changed when initialized from an existing user");
            }

            this.identifier = UUID.randomUUID().toString();
            return this;
        }

        /**
         * Sets the identifier of the builder with a UUID generated from the specified seed string.
         *
         * @return the builder
         * @throws IllegalStateException if this method is called when this builder was constructed from an existing User
         */
        public Builder identifierGenerateFromSeed(final String seed) {
            if (fromUser) {
                throw new IllegalStateException(
                        "Identifier can not be changed when initialized from an existing user");
            }
            if (seed == null) {
                throw new IllegalArgumentException("Cannot seed the user identifier with a null value.");
            }

            this.identifier = UUID.nameUUIDFromBytes(seed.getBytes(StandardCharsets.UTF_8)).toString();
            return this;
        }

        /**
         * Sets the identity of the builder.
         *
         * @param identity the identity to set
         * @return the builder
         */
        public Builder identity(final String identity) {
            this.identity = identity;
            return this;
        }

        /**
         * @return a new User constructed from the state of the builder
         */
        public User build() {
            return new User(this);
        }

    }

}
