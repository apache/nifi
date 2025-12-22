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

import org.apache.nifi.components.connector.components.ProcessorLifecycle;
import org.apache.nifi.components.connector.components.ProcessorState;

import java.util.concurrent.CompletableFuture;

/**
 * A wrapper around {@link ProcessorLifecycle} that enforces authorization before delegating
 * to the underlying implementation.
 */
public class AuthorizingProcessorLifecycle implements ProcessorLifecycle {

    private final ProcessorLifecycle delegate;
    private final ConnectorAuthorizationContext authContext;

    public AuthorizingProcessorLifecycle(final ProcessorLifecycle delegate, final ConnectorAuthorizationContext authContext) {
        this.delegate = delegate;
        this.authContext = authContext;
    }

    @Override
    public ProcessorState getState() {
        authContext.authorizeRead();
        return delegate.getState();
    }

    @Override
    public int getActiveThreadCount() {
        authContext.authorizeRead();
        return delegate.getActiveThreadCount();
    }

    @Override
    public void terminate() {
        authContext.authorizeWrite();
        delegate.terminate();
    }

    @Override
    public CompletableFuture<Void> stop() {
        authContext.authorizeWrite();
        return delegate.stop();
    }

    @Override
    public CompletableFuture<Void> start() {
        authContext.authorizeWrite();
        return delegate.start();
    }

    @Override
    public void disable() {
        authContext.authorizeWrite();
        delegate.disable();
    }

    @Override
    public void enable() {
        authContext.authorizeWrite();
        delegate.enable();
    }
}

