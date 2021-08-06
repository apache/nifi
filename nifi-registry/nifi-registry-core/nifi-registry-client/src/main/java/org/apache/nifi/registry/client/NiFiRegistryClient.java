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
package org.apache.nifi.registry.client;

import java.io.Closeable;

/**
 * A client for interacting with the REST API of a NiFi registry instance.
 */
public interface NiFiRegistryClient extends Closeable {

    /**
     * @return the client for interacting with buckets
     */
    BucketClient getBucketClient();

    /**
     * @deprecated use getBucketClient(RequestConfig requestConfig)
     *
     * @return the client for interacting with buckets on behalf of the given proxied entities
     */
    BucketClient getBucketClient(String ... proxiedEntity);

    /**
     * @return the client for interacting with buckets using the given request config
     */
    BucketClient getBucketClient(RequestConfig requestConfig);

    //-------------------------------------------------------------------------------------------

    /**
     * @return the client for interacting with flows
     */
    FlowClient getFlowClient();

    /**
     * @deprecated use getFlowClient(RequestConfig requestConfig)
     *
     * @return the client for interacting with flows on behalf of the given proxied entities
     */
    FlowClient getFlowClient(String ... proxiedEntity);

    /**
     * @return the client for interacting with flows using the given request config
     */
    FlowClient getFlowClient(RequestConfig requestConfig);

    //-------------------------------------------------------------------------------------------

    /**
     * @return the client for interacting with flows/snapshots
     */
    FlowSnapshotClient getFlowSnapshotClient();

    /**
     * @deprecated use getFlowSnapshotClient(RequestConfig requestConfig)
     *
     * @return the client for interacting with flows/snapshots on behalf of the given proxied entities
     */
    FlowSnapshotClient getFlowSnapshotClient(String ... proxiedEntity);

    /**
     * @return the client for interacting with flows/snapshots using the given request config
     */
    FlowSnapshotClient getFlowSnapshotClient(RequestConfig requestConfig);

    //-------------------------------------------------------------------------------------------

    /**
     * @return the client for interacting with bucket items
     */
    ItemsClient getItemsClient();

    /**
     * @deprecated use getItemsClient(RequestConfig requestConfig)
     *
     * @return the client for interacting with bucket items on behalf of the given proxied entities
     */
    ItemsClient getItemsClient(String ... proxiedEntity);

    /**
     * @return the client for interacting with bucket items using the given request config
     */
    ItemsClient getItemsClient(RequestConfig requestConfig);

    //-------------------------------------------------------------------------------------------

    /**
     * @return the client for obtaining information about the current user
     */
    UserClient getUserClient();

    /**
     * @deprecated use getUserClient(RequestConfig requestConfig)
     *
     * @return the client for obtaining information about the current user based on the given proxied entities
     */
    UserClient getUserClient(String ... proxiedEntity);

    /**
     * @return the client for obtaining information about the current user based on the request config
     */
    UserClient getUserClient(RequestConfig requestConfig);

    //-------------------------------------------------------------------------------------------

    /**
     * @return the client for interacting with extension bundles
     */
    BundleClient getBundleClient();

    /**
     * @deprecated use getBundleClient(RequestConfig requestConfig)
     *
     * @return the client for interacting with extension bundles on behalf of the given proxied entities
     */
    BundleClient getBundleClient(String ... proxiedEntity);

    /**
     * @return the client for interacting with extension bundles using the given request config
     */
    BundleClient getBundleClient(RequestConfig requestConfig);

    //-------------------------------------------------------------------------------------------

    /**
     * @return the client for interacting with extension bundle versions
     */
    BundleVersionClient getBundleVersionClient();

    /**
     * @deprecated use getBundleVersionClient(RequestConfig requestConfig)
     *
     * @return the client for interacting with extension bundle versions on behalf of the given proxied entities
     */
    BundleVersionClient getBundleVersionClient(String ... proxiedEntity);

    /**
     * @return the client for interacting with extension bundle versions using the given request config
     */
    BundleVersionClient getBundleVersionClient(RequestConfig requestConfig);

    //-------------------------------------------------------------------------------------------

    /**
     * @return the client for interacting with the extension repository
     */
    ExtensionRepoClient getExtensionRepoClient();

    /**
     * @deprecated use getExtensionRepoClient(RequestConfig requestConfig)
     *
     * @return the client for interacting with the extension repository on behalf of the given proxied entities
     */
    ExtensionRepoClient getExtensionRepoClient(String ... proxiedEntity);

    /**
     * @return the client for interacting with the extension repository using the given request config
     */
    ExtensionRepoClient getExtensionRepoClient(RequestConfig requestConfig);

    //-------------------------------------------------------------------------------------------

    /**
     * @return the client for interacting with extensions
     */
    ExtensionClient getExtensionClient();

    /**
     * @deprecated use getExtensionClient(RequestConfig requestConfig)
     *
     * @return the client for interacting with extensions on behalf of the given proxied entities
     */
    ExtensionClient getExtensionClient(String ... proxiedEntity);

    /**
     * @return the client for interacting with extensions using the given request config
     */
    ExtensionClient getExtensionClient(RequestConfig requestConfig);

    //-------------------------------------------------------------------------------------------

    /**
     * Returns client for interacting with tenants.
     *
     * @return the client for interacting with tenants
     */
    TenantsClient getTenantsClient();

    /**
     * Returns client for interacting with tenants.
     *
     * @deprecated use getTenantsClient(RequestConfig requestConfig)
     *
     * @param proxiedEntity The given proxied entities.
     *
     * @return the client for interacting with tenants on behalf of the given proxied entities.
     */
    TenantsClient getTenantsClient(String ... proxiedEntity);

    /**
     * @return the client for interacting with tenants using the given request config
     */
    TenantsClient getTenantsClient(RequestConfig requestConfig);

    //-------------------------------------------------------------------------------------------

    /**
     * Returns client for interacting with access policies.
     *
     * @return the client for interacting with access policies
     */
    PoliciesClient getPoliciesClient();

    /**
     * Returns client for interacting with access policies.
     *
     * @deprecated use getPoliciesClient(RequestConfig requestConfig)
     *
     * @param proxiedEntity The given proxied entities.
     *
     * @return the client for interacting with access policies on behalf of the given proxied entities.
     */
    PoliciesClient getPoliciesClient(String ... proxiedEntity);

    /**
     * @return the client for interacting with access policies using the given request config
     */
    PoliciesClient getPoliciesClient(RequestConfig requestConfig);

    //-------------------------------------------------------------------------------------------

    /**
     * @return the client for obtaining access tokens
     */
    AccessClient getAccessClient();

    //-------------------------------------------------------------------------------------------

    /**
     * The builder interface that implementations should provide for obtaining the client.
     */
    interface Builder {

        Builder config(NiFiRegistryClientConfig clientConfig);

        NiFiRegistryClientConfig getConfig();

        NiFiRegistryClient build();

    }

}
