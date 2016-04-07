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
    private final Map<String, String> context;
    private final Map<String, String> eventAttributes;

    private AuthorizationRequest(final Builder builder) {
        Objects.requireNonNull(builder.resource, "The resource is required when creating an authorization request");
        Objects.requireNonNull(builder.identity, "The identity of the user is required when creating an authorization request");
        Objects.requireNonNull(builder.action, "The action is required when creating an authorization request");

        this.resource = builder.resource;
        this.identity = builder.identity;
        this.action = builder.action;
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
     * The identity accessing the Resource. Not null.
     *
     * @return The identity
     */
    public String getIdentity() {
        return identity;
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
