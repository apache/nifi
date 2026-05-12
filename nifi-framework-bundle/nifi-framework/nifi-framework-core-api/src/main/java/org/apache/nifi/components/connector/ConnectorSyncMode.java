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
 * Indicates whether retrieval of {@link ConnectorNode} instances from the
 * {@link ConnectorRepository} should consult the configured
 * {@link ConnectorConfigurationProvider} to refresh the local state from the
 * external store, or whether the in-memory copy should be returned as-is.
 *
 * <p>Choosing {@link #SYNC_WITH_PROVIDER} causes the repository to call into
 * the configuration provider (which may perform network I/O, secret lookups,
 * and other potentially expensive or failure-prone operations) before returning
 * the connector. This is appropriate for user-facing reads that must reflect
 * the latest external state, such as REST API responses. Callers operating on
 * critical paths that must not block on or fail because of the external store
 * (for example, periodic flow serialization) should use {@link #LOCAL_ONLY}
 * instead.</p>
 */
public enum ConnectorSyncMode {

    /**
     * Consult the {@link ConnectorConfigurationProvider} (when configured) to
     * refresh the local connector state from the external store before
     * returning. This may perform network I/O and may throw if the external
     * store is unavailable.
     */
    SYNC_WITH_PROVIDER,

    /**
     * Return the in-memory connector state without consulting the
     * {@link ConnectorConfigurationProvider}. No external I/O is performed.
     */
    LOCAL_ONLY
}
