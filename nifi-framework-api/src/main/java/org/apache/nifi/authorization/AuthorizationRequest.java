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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Represents an authorization request for a given user/entity performing an action against a resource within some userContext.
 */
public class AuthorizationRequest {

    public static final String DEFAULT_EXPLANATION = "Unable to perform the desired action.";

    private final Resource resource;
    private final Resource requestedResource;
    private final String identity;
    private final Set<String> groups;
    private final RequestAction action;
    private final boolean isAccessAttempt;
    private final boolean isAnonymous;
    private final Map<String, String> userContext;
    private final Map<String, String> resourceContext;
    private final Supplier<String> explanationSupplier;

    private AuthorizationRequest(final Builder builder) {
        Objects.requireNonNull(builder.resource, "The resource is required when creating an authorization request");
        Objects.requireNonNull(builder.action, "The action is required when creating an authorization request");
        Objects.requireNonNull(builder.isAccessAttempt, "Whether this request is an access attempt is request");
        Objects.requireNonNull(builder.isAnonymous, "Whether this request is being performed by an anonymous user is required");

        this.resource = builder.resource;
        this.identity = builder.identity;
        this.groups = builder.groups == null ? null : Collections.unmodifiableSet(builder.groups);
        this.action = builder.action;
        this.isAccessAttempt = builder.isAccessAttempt;
        this.isAnonymous = builder.isAnonymous;
        this.userContext = builder.userContext == null ? null : Collections.unmodifiableMap(builder.userContext);
        this.resourceContext = builder.resourceContext == null ? null : Collections.unmodifiableMap(builder.resourceContext);
        this.explanationSupplier = () -> {
            final String explanation = builder.explanationSupplier.get();

            // ensure the specified supplier returns non null
            if (explanation == null) {
                return DEFAULT_EXPLANATION;
            } else {
                return explanation;
            }
        };

        if (builder.requestedResource == null) {
            this.requestedResource = builder.resource;
        } else {
            this.requestedResource = builder.requestedResource;
        }
    }

    /**
     * The Resource being authorized. Not null.
     *
     * @return The resource
     */
    public Resource getResource() {
        return resource;
    }

    /**
     * The original Resource being requested. In cases with inherited policies, this will be a ancestor resource of
     * of the current resource. The initial request, and cases without inheritance, the requested resource will be
     * the same as the current resource.
     *
     * @return The requested resource
     */
    public Resource getRequestedResource() {
        return requestedResource;
    }

    /**
     * The identity accessing the Resource. May be null if the user could not authenticate.
     *
     * @return The identity
     */
    public String getIdentity() {
        return identity;
    }

    /**
     * The groups the user making this request belongs to. May be null if this NiFi is not configured to load user
     * groups or empty if the user has no groups
     *
     * @return The groups
     */
    public Set<String> getGroups() {
        return groups;
    }

    /**
     * Whether this is a direct access attempt of the Resource if if it's being checked as part of another response.
     *
     * @return if this is a direct access attempt
     */
    public boolean isAccessAttempt() {
        return isAccessAttempt;
    }

    /**
     * Whether the entity accessing is anonymous.
     *
     * @return whether the entity is anonymous
     */
    public boolean isAnonymous() {
        return isAnonymous;
    }

    /**
     * The action being taken against the Resource. Not null.
     *
     * @return The action
     */
    public RequestAction getAction() {
        return action;
    }

    /**
     * The userContext of the user request to make additional access decisions. May be null.
     *
     * @return  The userContext of the user request
     */
    public Map<String, String> getUserContext() {
        return userContext;
    }

    /**
     * The event attributes to make additional access decisions for provenance events. May be null.
     *
     * @return  The event attributes
     */
    public Map<String, String> getResourceContext() {
        return resourceContext;
    }

    /**
     * A supplier for the explanation if access is denied. Non null.
     *
     * @return The explanation supplier if access is denied
     */
    public Supplier<String> getExplanationSupplier() {
        return explanationSupplier;
    }

    /**
     * AuthorizationRequest builder.
     */
    public static final class Builder {

        private Resource resource;
        private Resource requestedResource;
        private String identity;
        private Set<String> groups;
        private Boolean isAnonymous;
        private Boolean isAccessAttempt;
        private RequestAction action;
        private Map<String, String> userContext;
        private Map<String, String> resourceContext;
        private Supplier<String> explanationSupplier = () -> DEFAULT_EXPLANATION;

        public Builder resource(final Resource resource) {
            this.resource = resource;
            return this;
        }

        public Builder requestedResource(final Resource requestedResource) {
            this.requestedResource = requestedResource;
            return this;
        }

        public Builder identity(final String identity) {
            this.identity = identity;
            return this;
        }

        public Builder groups(final Set<String> groups) {
            this.groups = groups;
            return this;
        }

        public Builder anonymous(final Boolean isAnonymous) {
            this.isAnonymous = isAnonymous;
            return this;
        }

        public Builder accessAttempt(final Boolean isAccessAttempt) {
            this.isAccessAttempt = isAccessAttempt;
            return this;
        }

        public Builder action(final RequestAction action) {
            this.action = action;
            return this;
        }

        public Builder userContext(final Map<String, String> userContext) {
            if (userContext != null) {
                this.userContext = new HashMap<>(userContext);
            }
            return this;
        }

        public Builder resourceContext(final Map<String, String> resourceContext) {
            if (resourceContext != null) {
                this.resourceContext = new HashMap<>(resourceContext);
            }
            return this;
        }

        public Builder explanationSupplier(final Supplier<String> explanationSupplier) {
            if (explanationSupplier != null) {
                this.explanationSupplier = explanationSupplier;
            }
            return this;
        }

        public AuthorizationRequest build() {
            return new AuthorizationRequest(this);
        }
    }
}
