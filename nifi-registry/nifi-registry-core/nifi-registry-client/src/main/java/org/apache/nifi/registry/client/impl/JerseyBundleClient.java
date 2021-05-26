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
package org.apache.nifi.registry.client.impl;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.registry.client.BundleClient;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.client.RequestConfig;
import org.apache.nifi.registry.extension.bundle.Bundle;
import org.apache.nifi.registry.extension.bundle.BundleFilterParams;

import javax.ws.rs.client.WebTarget;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Jersey implementation of BundleClient.
 */
public class JerseyBundleClient extends AbstractJerseyClient implements BundleClient {

    private final WebTarget bucketExtensionBundlesTarget;
    private final WebTarget extensionBundlesTarget;

    public JerseyBundleClient(final WebTarget baseTarget) {
        this(baseTarget, null);
    }

    public JerseyBundleClient(final WebTarget baseTarget, final RequestConfig requestConfig) {
        super(requestConfig);
        this.bucketExtensionBundlesTarget = baseTarget.path("buckets/{bucketId}/bundles");
        this.extensionBundlesTarget = baseTarget.path("bundles");
    }

    @Override
    public List<Bundle> getAll() throws IOException, NiFiRegistryException {
        return getAll(null);
    }

    @Override
    public List<Bundle> getAll(final BundleFilterParams filterParams) throws IOException, NiFiRegistryException {
        return executeAction("Error getting extension bundles", () -> {
            WebTarget target = extensionBundlesTarget;

            if (filterParams != null) {
                if (!StringUtils.isBlank(filterParams.getBucketName())) {
                    target = target.queryParam("bucketName", filterParams.getBucketName());
                }
                if (!StringUtils.isBlank(filterParams.getGroupId())) {
                    target = target.queryParam("groupId", filterParams.getGroupId());
                }
                if (!StringUtils.isBlank(filterParams.getArtifactId())) {
                    target = target.queryParam("artifactId", filterParams.getArtifactId());
                }
            }

            final Bundle[] bundles = getRequestBuilder(target).get(Bundle[].class);
            return  bundles == null ? Collections.emptyList() : Arrays.asList(bundles);
        });
    }

    @Override
    public List<Bundle> getByBucket(final String bucketId) throws IOException, NiFiRegistryException {
        if (StringUtils.isBlank(bucketId)) {
            throw new IllegalArgumentException("Bucket id cannot be null or blank");
        }

        return executeAction("Error getting extension bundles for bucket", () -> {
            WebTarget target = bucketExtensionBundlesTarget.resolveTemplate("bucketId", bucketId);

            final Bundle[] bundles = getRequestBuilder(target).get(Bundle[].class);
            return  bundles == null ? Collections.emptyList() : Arrays.asList(bundles);
        });
    }

    @Override
    public Bundle get(final String bundleId) throws IOException, NiFiRegistryException {
        if (StringUtils.isBlank(bundleId)) {
            throw new IllegalArgumentException("Bundle id cannot be null or blank");
        }

        return executeAction("Error getting extension bundle", () -> {
            WebTarget target = extensionBundlesTarget
                    .path("{bundleId}")
                    .resolveTemplate("bundleId", bundleId);

            return getRequestBuilder(target).get(Bundle.class);
        });
    }

    @Override
    public Bundle delete(final String bundleId) throws IOException, NiFiRegistryException {
        if (StringUtils.isBlank(bundleId)) {
            throw new IllegalArgumentException("Bundle id cannot be null or blank");
        }

        return executeAction("Error deleting extension bundle", () -> {
            WebTarget target = extensionBundlesTarget
                    .path("{bundleId}")
                    .resolveTemplate("bundleId", bundleId);

            return getRequestBuilder(target).delete(Bundle.class);
        });
    }

}
