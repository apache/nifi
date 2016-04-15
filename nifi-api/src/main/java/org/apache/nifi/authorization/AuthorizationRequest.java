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

/**
 * Represents an authorization request for a given user/entity performing an action against a resource within some context.
 */
public class AuthorizationRequest {

    private final Resource resource;
    private final String identity;
    private final RequestAction action;
    private final boolean isAccessAttempt;
    private final boolean isAnonymous;
    private final Map<String, String> context;
    private final Map<String, String> eventAttributes;

    private AuthorizationRequest(final Builder builder) {
        Objects.requireNonNull(builder.resource, "The resource is required when creating an authorization request");
        Objects.requireNonNull(builder.action, "The action is required when creating an authorization request");
        Objects.requireNonNull(builder.isAccessAttempt, "Whether this request is an access attempt is request");
        Objects.requireNonNull(builder.isAnonymous, "Whether this request is being performed by an anonymous user is required");

        this.resource = builder.resource;
        this.identity = builder.identity;
        this.action = builder.action;
        this.isAccessAttempt = builder.isAccessAttempt;
        this.isAnonymous = builder.isAnonymous;
        this.context = builder.context == null ? null : Collections.unmodifiableMap(builder.context);
        this.eventAttributes = builder.context == null ? null : Collections.unmodifiableMap(builder.eventAttributes);
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
     * The identity accessing the Resource. May be null if the user could not authenticate.
     *
     * @return The identity
     */
    public String getIdentity() {
        return identity;
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
     * The context of the user request to make additional access decisions. May be null.
     *
     * @return  The context of the user request
     */
    public Map<String, String> getContext() {
        return context;
    }

    /**
     * The event attributes to make additional access decisions for provenance events. May be null.
     *
     * @return  The event attributes
     */
    public Map<String, String> getEventAttributes() {
        return eventAttributes;
    }

    /**
     * AuthorizationRequest builder.
     */
    public static final class Builder {

        private Resource resource;
        private String identity;
        private Boolean isAnonymous;
        private Boolean isAccessAttempt;
        private RequestAction action;
        private Map<String, String> context;
        private Map<String, String> eventAttributes;

        public Builder resource(final Resource resource) {
            this.resource = resource;
            return this;
        }

        public Builder identity(final String identity) {
            this.identity = identity;
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

        public Builder context(final Map<String, String> context) {
            this.context = new HashMap<>(context);
            return this;
        }

        public Builder eventAttributes(final Map<String, String> eventAttributes) {
            this.eventAttributes = new HashMap<>(eventAttributes);
            return this;
        }

        public AuthorizationRequest build() {
            return new AuthorizationRequest(this);
        }
    }
}
