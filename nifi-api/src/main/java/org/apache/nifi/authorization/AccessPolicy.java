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
 * Defines a policy for a set of entities to perform a set of actions on a given resource.
 */
public class AccessPolicy {

    private final String identifier;

    private final Resource resource;

    private final Set<String> entities;

    private final Set<RequestAction> actions;

    private AccessPolicy(final AccessPolicyBuilder builder) {
        this.identifier = builder.identifier;
        this.resource = builder.resource;
        this.entities = Collections.unmodifiableSet(new HashSet<>(builder.entities));
        this.actions = Collections.unmodifiableSet(new HashSet<>(builder.actions));

        if (this.identifier == null || this.identifier.trim().isEmpty()) {
            throw new IllegalArgumentException("Identifier can not be null or empty");
        }

        if (this.resource == null) {
            throw new IllegalArgumentException("Resource can not be null");
        }

        if (this.entities == null || this.entities.isEmpty()) {
            throw new IllegalArgumentException("Entities can not be null or empty");
        }

        if (this.actions == null || this.actions.isEmpty()) {
            throw new IllegalArgumentException("Actions can not be null or empty");
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
    public Resource getResource() {
        return resource;
    }

    /**
     * @return an unmodifiable set of entity ids for this policy
     */
    public Set<String> getEntities() {
        return entities;
    }

    /**
     * @return an unmodifiable set of actions for this policy
     */
    public Set<RequestAction> getActions() {
        return actions;
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
        return String.format("identifier[%s], resource[%s], entityId[%s], action[%s]",
                getIdentifier(), getResource().getIdentifier(), getEntities(), getActions());
    }

    /**
     * Builder for Access Policies.
     */
    public static class AccessPolicyBuilder {

        private String identifier;
        private Resource resource;
        private Set<String> entities = new HashSet<>();
        private Set<RequestAction> actions = new HashSet<>();
        private final boolean fromPolicy;

        /**
         * Default constructor for building a new AccessPolicy.
         */
        public AccessPolicyBuilder() {
            this.fromPolicy = false;
        }

        /**
         * Initializes the builder with the state of the provided policy. When using this constructor
         * the identifier field of the builder can not be changed and will result in an IllegalStateException
         * if attempting to do so.
         *
         * @param other the existing access policy to initialize from
         */
        public AccessPolicyBuilder(final AccessPolicy other) {
            if (other == null) {
                throw new IllegalArgumentException("Can not initialize builder with a null access policy");
            }

            this.identifier = other.getIdentifier();
            this.resource = other.getResource();
            this.entities.clear();
            this.entities.addAll(other.getEntities());
            this.actions.clear();
            this.actions.addAll(other.getActions());
            this.fromPolicy = true;
        }

        /**
         * Sets the identifier of the builder.
         *
         * @param identifier the identifier to set
         * @return the builder
         * @throws IllegalStateException if this method is called when this builder was constructed from an existing Policy
         */
        public AccessPolicyBuilder identifier(final String identifier) {
            if (fromPolicy) {
                throw new IllegalStateException(
                        "Identifier can not be changed when initialized from an existing policy");
            }

            this.identifier = identifier;
            return this;
        }

        /**
         * Sets the resource of the builder.
         *
         * @param resource the resource to set
         * @return the builder
         */
        public AccessPolicyBuilder resource(final Resource resource) {
            this.resource = resource;
            return this;
        }

        /**
         * Adds all the entities from the provided set to the builder's set of entities.
         *
         * @param entities the entities to add
         * @return the builder
         */
        public AccessPolicyBuilder addEntities(final Set<String> entities) {
            if (entities != null) {
                this.entities.addAll(entities);
            }
            return this;
        }

        /**
         * Adds the given entity to the builder's set of entities.
         *
         * @param entity the entity to add
         * @return the builder
         */
        public AccessPolicyBuilder addEntity(final String entity) {
            if (entity != null) {
                this.entities.add(entity);
            }
            return this;
        }

        /**
         * Removes all entities in the provided set from the builder's set of entities.
         *
         * @param entities the entities to remove
         * @return the builder
         */
        public AccessPolicyBuilder removeEntities(final Set<String> entities) {
            if (entities != null) {
                this.entities.removeAll(entities);
            }
            return this;
        }

        /**
         * Removes the provided entity from the builder's set of entities.
         *
         * @param entity the entity to remove
         * @return the builder
         */
        public AccessPolicyBuilder removeEntity(final String entity) {
            if (entity != null) {
                this.entities.remove(entity);
            }
            return this;
        }

        /**
         * Clears the builder's set of entities so that it is non-null and size == 0.
         *
         * @return the builder
         */
        public AccessPolicyBuilder clearEntities() {
            this.entities.clear();
            return this;
        }

        /**
         * Adds the provided action to the builder's set of actions.
         *
         * @param action the action to add
         * @return the builder
         */
        public AccessPolicyBuilder addAction(final RequestAction action) {
            this.actions.add(action);
            return this;
        }

        /**
         * Removes the provided action from the builder's set of actions.
         *
         * @param action the action to remove
         * @return the builder
         */
        public AccessPolicyBuilder removeAction(final RequestAction action) {
            this.actions.remove(action);
            return this;
        }

        /**
         * Clears the builder's set of actions so that it is non-null and size == 0.
         *
         * @return the builder
         */
        public AccessPolicyBuilder clearActions() {
            this.actions.clear();
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
