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
import org.apache.nifi.registry.client.BundleVersionClient;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.client.RequestConfig;
import org.apache.nifi.registry.extension.bundle.BundleType;
import org.apache.nifi.registry.extension.bundle.BundleVersion;
import org.apache.nifi.registry.extension.bundle.BundleVersionFilterParams;
import org.apache.nifi.registry.extension.bundle.BundleVersionMetadata;
import org.apache.nifi.registry.extension.component.ExtensionMetadata;
import org.apache.nifi.registry.extension.component.manifest.Extension;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.file.FileDataBodyPart;
import org.glassfish.jersey.media.multipart.file.StreamDataBodyPart;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Jersey implementation of BundleVersionClient.
 */
public class JerseyBundleVersionClient extends AbstractJerseyClient implements BundleVersionClient {

    private final WebTarget bucketExtensionBundlesTarget;
    private final WebTarget extensionBundlesTarget;

    public JerseyBundleVersionClient(final WebTarget baseTarget) {
        this(baseTarget, null);
    }

    public JerseyBundleVersionClient(final WebTarget baseTarget, final RequestConfig requestConfig) {
        super(requestConfig);
        this.bucketExtensionBundlesTarget = baseTarget.path("buckets/{bucketId}/bundles");
        this.extensionBundlesTarget = baseTarget.path("bundles");
    }

    @Override
    public BundleVersion create(final String bucketId, final BundleType bundleType, final InputStream bundleContentStream)
            throws IOException, NiFiRegistryException {
        return create(bucketId, bundleType, bundleContentStream, null);
    }

        @Override
    public BundleVersion create(final String bucketId, final BundleType bundleType, final InputStream bundleContentStream, final String sha256)
            throws IOException, NiFiRegistryException {

        if (StringUtils.isBlank(bucketId)) {
            throw new IllegalArgumentException("Bucket id cannot be null or blank");
        }

        if (bundleType == null) {
            throw new IllegalArgumentException("Bundle type cannot be null");
        }

        if (bundleContentStream == null) {
            throw new IllegalArgumentException("Bundle content cannot be null");
        }

        return executeAction("Error creating extension bundle version", () -> {
            final WebTarget target = bucketExtensionBundlesTarget
                    .path("{bundleType}")
                    .resolveTemplate("bucketId", bucketId)
                    .resolveTemplate("bundleType", bundleType.toString());

            final StreamDataBodyPart streamBodyPart = new StreamDataBodyPart("file", bundleContentStream);

            final FormDataMultiPart multipart = new FormDataMultiPart();
            multipart.bodyPart(streamBodyPart);

            if (!StringUtils.isBlank(sha256)) {
                multipart.field("sha256", sha256);
            }

            return getRequestBuilder(target)
                    .post(
                            Entity.entity(multipart, multipart.getMediaType()),
                            BundleVersion.class
                    );
        });
    }

    @Override
    public BundleVersion create(final String bucketId, final BundleType bundleType, final File bundleFile)
            throws IOException, NiFiRegistryException {
        return create(bucketId, bundleType, bundleFile, null);
    }

    @Override
    public BundleVersion create(final String bucketId, final BundleType bundleType, final File bundleFile, final String sha256)
            throws IOException, NiFiRegistryException {

        if (StringUtils.isBlank(bucketId)) {
            throw new IllegalArgumentException("Bucket id cannot be null or blank");
        }

        if (bundleType == null) {
            throw new IllegalArgumentException("Bundle type cannot be null");
        }

        if (bundleFile == null) {
            throw new IllegalArgumentException("Bundle file cannot be null");
        }

        return executeAction("Error creating extension bundle version", () -> {
            final WebTarget target = bucketExtensionBundlesTarget
                    .path("{bundleType}")
                    .resolveTemplate("bucketId", bucketId)
                    .resolveTemplate("bundleType", bundleType.toString());

            final FileDataBodyPart fileBodyPart = new FileDataBodyPart("file", bundleFile, MediaType.APPLICATION_OCTET_STREAM_TYPE);

            final FormDataMultiPart multipart = new FormDataMultiPart();
            multipart.bodyPart(fileBodyPart);

            if (!StringUtils.isBlank(sha256)) {
                multipart.field("sha256", sha256);
            }

            return getRequestBuilder(target)
                    .post(
                            Entity.entity(multipart, multipart.getMediaType()),
                            BundleVersion.class
                    );
        });
    }

    @Override
    public List<BundleVersionMetadata> getBundleVersions(final BundleVersionFilterParams filterParams)
            throws IOException, NiFiRegistryException {

        return executeAction("Error getting extension bundle versions", () -> {
            WebTarget target = extensionBundlesTarget.path("/versions");

            if (filterParams != null) {
                if (!StringUtils.isBlank(filterParams.getGroupId())) {
                    target = target.queryParam("groupId", filterParams.getGroupId());
                }

                if (!StringUtils.isBlank(filterParams.getArtifactId())) {
                    target = target.queryParam("artifactId", filterParams.getArtifactId());
                }

                if (!StringUtils.isBlank(filterParams.getVersion())) {
                    target = target.queryParam("version", filterParams.getVersion());
                }
            }

            final BundleVersionMetadata[] bundleVersions = getRequestBuilder(target).get(BundleVersionMetadata[].class);
            return  bundleVersions == null ? Collections.emptyList() : Arrays.asList(bundleVersions);
        });
    }

    @Override
    public List<BundleVersionMetadata> getBundleVersions(final String bundleId)
            throws IOException, NiFiRegistryException {

        if (StringUtils.isBlank(bundleId)) {
            throw new IllegalArgumentException("Bundle id cannot be null or blank");
        }

        return executeAction("Error getting extension bundle versions", () -> {
            final WebTarget target = extensionBundlesTarget
                    .path("{bundleId}/versions")
                    .resolveTemplate("bundleId", bundleId);

            final BundleVersionMetadata[] bundleVersions = getRequestBuilder(target).get(BundleVersionMetadata[].class);
            return  bundleVersions == null ? Collections.emptyList() : Arrays.asList(bundleVersions);
        });
    }

    @Override
    public BundleVersion getBundleVersion(final String bundleId, final String version)
            throws IOException, NiFiRegistryException {

        if (StringUtils.isBlank(bundleId)) {
            throw new IllegalArgumentException("Bundle id cannot be null or blank");
        }

        if (StringUtils.isBlank(version)) {
            throw new IllegalArgumentException("Version cannot be null or blank");
        }

        return executeAction("Error getting extension bundle version", () -> {
            final WebTarget target = extensionBundlesTarget
                    .path("{bundleId}/versions/{version}")
                    .resolveTemplate("bundleId", bundleId)
                    .resolveTemplate("version", version);

            return getRequestBuilder(target).get(BundleVersion.class);
         });
    }

    @Override
    public List<ExtensionMetadata> getExtensions(final String bundleId, final String version)
            throws IOException, NiFiRegistryException {

        if (StringUtils.isBlank(bundleId)) {
            throw new IllegalArgumentException("Bundle id cannot be null or blank");
        }

        if (StringUtils.isBlank(version)) {
            throw new IllegalArgumentException("Version cannot be null or blank");
        }

        return executeAction("Error getting extension bundle metadata", () -> {
            final WebTarget target = extensionBundlesTarget
                    .path("{bundleId}/versions/{version}/extensions")
                    .resolveTemplate("bundleId", bundleId)
                    .resolveTemplate("version", version);

            final ExtensionMetadata[] extensions = getRequestBuilder(target).get(ExtensionMetadata[].class);
            return  extensions == null ? Collections.emptyList() : Arrays.asList(extensions);
        });
    }

    @Override
    public Extension getExtension(final String bundleId, final String version, final String name) throws IOException, NiFiRegistryException {
        if (StringUtils.isBlank(bundleId)) {
            throw new IllegalArgumentException("Bundle id cannot be null or blank");
        }

        if (StringUtils.isBlank(version)) {
            throw new IllegalArgumentException("Version cannot be null or blank");
        }

        if (StringUtils.isBlank(name)) {
            throw new IllegalArgumentException("Extension name cannot be null or blank");
        }

        return executeAction("Error getting extension", () -> {
            final WebTarget target = extensionBundlesTarget
                    .path("{bundleId}/versions/{version}/extensions/{name}")
                    .resolveTemplate("bundleId", bundleId)
                    .resolveTemplate("version", version)
                    .resolveTemplate("name", name);

            final Extension extension = getRequestBuilder(target).get(Extension.class);
            return  extension;
        });
    }

    @Override
    public InputStream getExtensionDocs(final String bundleId, final String version, final String name) throws IOException, NiFiRegistryException {
        if (StringUtils.isBlank(bundleId)) {
            throw new IllegalArgumentException("Bundle id cannot be null or blank");
        }

        if (StringUtils.isBlank(version)) {
            throw new IllegalArgumentException("Version cannot be null or blank");
        }

        if (StringUtils.isBlank(name)) {
            throw new IllegalArgumentException("Extension name cannot be null or blank");
        }

        return executeAction("Error getting extension", () -> {
            final WebTarget target = extensionBundlesTarget
                    .path("{bundleId}/versions/{version}/extensions/{name}/docs")
                    .resolveTemplate("bundleId", bundleId)
                    .resolveTemplate("version", version)
                    .resolveTemplate("name", name);

            return getRequestBuilder(target)
                    .accept(MediaType.TEXT_HTML)
                    .get()
                    .readEntity(InputStream.class);
        });
    }

    @Override
    public InputStream getBundleVersionContent(final String bundleId, final String version)
            throws IOException, NiFiRegistryException {

        if (StringUtils.isBlank(bundleId)) {
            throw new IllegalArgumentException("Bundle id cannot be null or blank");
        }

        if (StringUtils.isBlank(version)) {
            throw new IllegalArgumentException("Version cannot be null or blank");
        }

        return executeAction("Error getting extension bundle version", () -> {
            final WebTarget target = extensionBundlesTarget
                    .path("{bundleId}/versions/{version}/content")
                    .resolveTemplate("bundleId", bundleId)
                    .resolveTemplate("version", version);

            return getRequestBuilder(target)
                    .accept(MediaType.APPLICATION_OCTET_STREAM_TYPE)
                    .get()
                    .readEntity(InputStream.class);
        });
    }

    @Override
    public File writeBundleVersionContent(final String bundleId, final String version, final File directory)
            throws IOException, NiFiRegistryException {

        if (StringUtils.isBlank(bundleId)) {
            throw new IllegalArgumentException("Bundle id cannot be null or blank");
        }

        if (StringUtils.isBlank(version)) {
            throw new IllegalArgumentException("Version cannot be null or blank");
        }

        if (directory == null || !directory.exists() || !directory.isDirectory()) {
            throw new IllegalArgumentException("Directory must exist and be a valid directory");
        }

        return executeAction("Error getting extension bundle version", () -> {
            final WebTarget target = extensionBundlesTarget
                    .path("{bundleId}/versions/{version}/content")
                    .resolveTemplate("bundleId", bundleId)
                    .resolveTemplate("version", version);

            final Response response = getRequestBuilder(target)
                    .accept(MediaType.APPLICATION_OCTET_STREAM_TYPE)
                    .get();

            return ClientUtils.getExtensionBundleVersionContent(response, directory);
        });
    }

    @Override
    public BundleVersion delete(final String bundleId, final String version) throws IOException, NiFiRegistryException {
        if (StringUtils.isBlank(bundleId)) {
            throw new IllegalArgumentException("Bundle id cannot be null or blank");
        }

        if (StringUtils.isBlank(version)) {
            throw new IllegalArgumentException("Version cannot be null or blank");
        }

        return executeAction("Error deleting extension bundle version", () -> {
            final WebTarget target = extensionBundlesTarget
                    .path("{bundleId}/versions/{version}")
                    .resolveTemplate("bundleId", bundleId)
                    .resolveTemplate("version", version);

            return getRequestBuilder(target).delete(BundleVersion.class);
        });
    }

}
