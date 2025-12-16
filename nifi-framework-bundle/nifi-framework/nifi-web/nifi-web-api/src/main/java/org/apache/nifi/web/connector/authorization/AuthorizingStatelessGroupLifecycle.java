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

import org.apache.nifi.components.connector.components.StatelessGroupLifecycle;

import java.util.concurrent.CompletableFuture;

/**
 * A wrapper around {@link StatelessGroupLifecycle} that enforces authorization before delegating
 * to the underlying implementation. All lifecycle operations require WRITE authorization.
 */
public class AuthorizingStatelessGroupLifecycle implements StatelessGroupLifecycle {

    private final StatelessGroupLifecycle delegate;
    private final ConnectorAuthorizationContext authContext;

    public AuthorizingStatelessGroupLifecycle(final StatelessGroupLifecycle delegate, final ConnectorAuthorizationContext authContext) {
        this.delegate = delegate;
        this.authContext = authContext;
    }

    @Override
    public CompletableFuture<Void> start() {
        authContext.authorizeWrite();
        return delegate.start();
    }

    @Override
    public CompletableFuture<Void> stop() {
        authContext.authorizeWrite();
        return delegate.stop();
    }

    @Override
    public CompletableFuture<Void> terminate() {
        authContext.authorizeWrite();
        return delegate.terminate();
    }
}

