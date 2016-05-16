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

    /**
     * Constructs a new policy with the given resource, entities, and actions.
     *
     * @param identifier the identifier of the policy
     * @param resource the resource for the policy
     * @param entities the entity ids for the policy (i.e. user or group ids)
     * @param actions the actions for the policy
     */
    public AccessPolicy(final String identifier, final Resource resource, final Set<String> entities, final Set<RequestAction> actions) {
        if (identifier == null || identifier.trim().isEmpty()) {
            throw new IllegalArgumentException("Identifier can not be null or empty");
        }

        if (resource == null) {
            throw new IllegalArgumentException("Resource can not be null");
        }

        if (entities == null || entities.isEmpty()) {
            throw new IllegalArgumentException("Entities can not be null or empty");
        }

        if (actions == null || actions.isEmpty()) {
            throw new IllegalArgumentException("Actions can not be null or empty");
        }

        this.identifier = identifier;
        this.resource = resource;
        this.entities = Collections.unmodifiableSet(entities);
        this.actions = Collections.unmodifiableSet(actions);
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
     * @return the set of entity ids for this policy
     */
    public Set<String> getEntities() {
        return entities;
    }

    /**
     * @return the set of actions for this policy
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

        return this.identifier.equals(other.getIdentifier())
                && this.resource.getIdentifier().equals(other.getResource().getIdentifier())
                && this.entities.equals(other.entities)
                && this.actions.equals(other.actions);
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 53 * hash + Objects.hash(this.identifier, this.resource, this.entities, this.actions);
        return hash;
    }

    @Override
    public String toString() {
        return String.format("identifier[%s], resource[%s], entityId[%s], action[%s]",
                getIdentifier(), getResource().getIdentifier(), getEntities(), getActions(), ", ");
    }

}
