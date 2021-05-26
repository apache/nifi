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
import org.apache.nifi.registry.client.ExtensionClient;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.client.RequestConfig;
import org.apache.nifi.registry.extension.bundle.BundleType;
import org.apache.nifi.registry.extension.component.ExtensionFilterParams;
import org.apache.nifi.registry.extension.component.ExtensionMetadataContainer;
import org.apache.nifi.registry.extension.component.TagCount;
import org.apache.nifi.registry.extension.component.manifest.ExtensionType;
import org.apache.nifi.registry.extension.component.manifest.ProvidedServiceAPI;

import javax.ws.rs.client.WebTarget;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class JerseyExtensionClient extends AbstractJerseyClient implements ExtensionClient {

    private final WebTarget extensionsTarget;

    public JerseyExtensionClient(final WebTarget baseTarget) {
        this(baseTarget, null);
    }

    public JerseyExtensionClient(final WebTarget baseTarget, final RequestConfig requestConfig) {
        super(requestConfig);
        this.extensionsTarget = baseTarget.path("extensions");
    }

    @Override
    public ExtensionMetadataContainer findExtensions(final ExtensionFilterParams filterParams)
            throws IOException, NiFiRegistryException {

        return executeAction("Error retrieving extensions", () -> {
            WebTarget target = extensionsTarget;

            if (filterParams != null) {
                final BundleType bundleType = filterParams.getBundleType();
                if (bundleType != null) {
                    target = target.queryParam("bundleType", bundleType.toString());
                }

                final ExtensionType extensionType = filterParams.getExtensionType();
                if (extensionType != null) {
                    target = target.queryParam("extensionType", extensionType.toString());
                }

                final Set<String> tags = filterParams.getTags();
                if (tags != null) {
                    for (final String tag : tags) {
                        target = target.queryParam("tag", tag);
                    }
                }
            }

            return getRequestBuilder(target).get(ExtensionMetadataContainer.class);
        });
    }

    @Override
    public ExtensionMetadataContainer findExtensions(final ProvidedServiceAPI serviceAPI) throws IOException, NiFiRegistryException {
        if (serviceAPI == null
                || StringUtils.isBlank(serviceAPI.getClassName())
                || StringUtils.isBlank(serviceAPI.getGroupId())
                || StringUtils.isBlank(serviceAPI.getArtifactId())
                || StringUtils.isBlank(serviceAPI.getVersion())) {
            throw new IllegalArgumentException("Provided service API must be specified with a class, group, artifact, and version");
        }

        return executeAction("Error retrieving extensions", () -> {
            WebTarget target = extensionsTarget.path("provided-service-api");
            target = target.queryParam("className", serviceAPI.getClassName());
            target = target.queryParam("groupId", serviceAPI.getGroupId());
            target = target.queryParam("artifactId", serviceAPI.getArtifactId());
            target = target.queryParam("version", serviceAPI.getVersion());

            return getRequestBuilder(target).get(ExtensionMetadataContainer.class);
        });
    }

    @Override
    public List<TagCount> getTagCounts() throws IOException, NiFiRegistryException {
        return executeAction("Error retrieving tag counts", () -> {
           final WebTarget target = extensionsTarget.path("tags");

           final TagCount[] tagCounts = getRequestBuilder(target).get(TagCount[].class);
           return tagCounts == null ? Collections.emptyList() : Arrays.asList(tagCounts);
        });
    }

}
