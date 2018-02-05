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
package org.apache.nifi.toolkit.cli.impl.client.nifi;

import java.io.Closeable;

/**
 * Main interface for interacting with a NiFi instance.
 */
public interface NiFiClient extends Closeable {

    // ----- ControllerClient -----

    /**
     * @return a ControllerClient
     */
    ControllerClient getControllerClient();

    /**
     * Obtains a ControllerClient for the given proxied entities. Each operation made from this client
     * will add the appropriate X-ProxiedEntitiesChain header to each request.
     *
     * @param proxiedEntity one or more identities to proxy
     * @return a ControllerClient
     */
    ControllerClient getControllerClientForProxiedEntities(String ... proxiedEntity);

    /**
     * Obtains a ControllerClient that will submit the given token in the Authorization Bearer header
     * with each request.
     *
     * @param token a token to authentication with
     * @return a ControllerClient
     */
    ControllerClient getControllerClientForToken(String token);

    // ----- FlowClient -----

    FlowClient getFlowClient();

    FlowClient getFlowClientForProxiedEntities(String ... proxiedEntity);

    FlowClient getFlowClientForToken(String token);

    // ----- ProcessGroupClient -----

    ProcessGroupClient getProcessGroupClient();

    ProcessGroupClient getProcessGroupClientForProxiedEntities(String ... proxiedEntity);

    ProcessGroupClient getProcessGroupClientForToken(String token);

    // ----- VersionsClient -----

    VersionsClient getVersionsClient();

    VersionsClient getVersionsClientForProxiedEntities(String ... proxiedEntity);

    VersionsClient getVersionsClientForToken(String token);

    /**
     * The builder interface that implementations should provide for obtaining the client.
     */
    interface Builder {

        NiFiClient.Builder config(NiFiClientConfig clientConfig);

        NiFiClientConfig getConfig();

        NiFiClient build();

    }

}
