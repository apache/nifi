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
package org.apache.nifi.web.connector.authorization;

import org.apache.nifi.components.connector.components.ControllerServiceLifecycle;
import org.apache.nifi.components.connector.components.ControllerServiceState;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * A wrapper around {@link ControllerServiceLifecycle} that enforces authorization before delegating
 * to the underlying implementation.
 */
public class AuthorizingControllerServiceLifecycle implements ControllerServiceLifecycle {

    private final ControllerServiceLifecycle delegate;
    private final ConnectorAuthorizationContext authContext;

    public AuthorizingControllerServiceLifecycle(final ControllerServiceLifecycle delegate, final ConnectorAuthorizationContext authContext) {
        this.delegate = delegate;
        this.authContext = authContext;
    }

    @Override
    public ControllerServiceState getState() {
        authContext.authorizeRead();
        return delegate.getState();
    }

    @Override
    public CompletableFuture<Void> enable() {
        authContext.authorizeWrite();
        return delegate.enable();
    }

    @Override
    public CompletableFuture<Void> enable(final Map<String, String> propertyValueOverrides) {
        authContext.authorizeWrite();
        return delegate.enable(propertyValueOverrides);
    }

    @Override
    public CompletableFuture<Void> disable() {
        authContext.authorizeWrite();
        return delegate.disable();
    }
}

