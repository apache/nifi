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

package org.apache.nifi.components.connector;

/**
 * Thread-local holder for {@link ConnectorRequestContext}. This class resides in
 * {@code nifi-framework-api} so that it is on the shared classloader, accessible
 * from both the NiFi web layer (which sets the context) and NAR-loaded
 * {@link ConnectorConfigurationProvider} implementations (which read it).
 *
 * <p>The context is set by the framework's request filter before connector operations
 * and cleared after the request completes. Provider implementations should access the
 * context via {@link #getContext()} and must not assume it is always present -- it will
 * be {@code null} for operations not triggered by an HTTP request (e.g., background tasks).</p>
 */
public final class ConnectorRequestContextHolder {

    private static final ThreadLocal<ConnectorRequestContext> CONTEXT = new ThreadLocal<>();

    private ConnectorRequestContextHolder() {
    }

    /**
     * Returns the {@link ConnectorRequestContext} for the current thread, or {@code null}
     * if no context has been set (e.g., for background or non-HTTP-request operations).
     *
     * @return the current request context, or {@code null}
     */
    public static ConnectorRequestContext getContext() {
        return CONTEXT.get();
    }

    /**
     * Sets the {@link ConnectorRequestContext} for the current thread.
     *
     * @param context the request context to set
     */
    public static void setContext(final ConnectorRequestContext context) {
        CONTEXT.set(context);
    }

    /**
     * Clears the {@link ConnectorRequestContext} from the current thread.
     * This must be called after request processing completes to prevent memory leaks.
     */
    public static void clearContext() {
        CONTEXT.remove();
    }
}
