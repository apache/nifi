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
package org.apache.nifi.registry.service.extension;

import org.apache.nifi.registry.bucket.Bucket;
import org.apache.nifi.registry.extension.bundle.Bundle;
import org.apache.nifi.registry.extension.bundle.BundleFilterParams;
import org.apache.nifi.registry.extension.bundle.BundleType;
import org.apache.nifi.registry.extension.bundle.BundleVersion;
import org.apache.nifi.registry.extension.bundle.BundleVersionFilterParams;
import org.apache.nifi.registry.extension.bundle.BundleVersionMetadata;
import org.apache.nifi.registry.extension.component.manifest.Extension;
import org.apache.nifi.registry.extension.component.ExtensionFilterParams;
import org.apache.nifi.registry.extension.component.ExtensionMetadata;
import org.apache.nifi.registry.extension.component.TagCount;
import org.apache.nifi.registry.extension.component.manifest.ProvidedServiceAPI;
import org.apache.nifi.registry.extension.repo.ExtensionRepoArtifact;
import org.apache.nifi.registry.extension.repo.ExtensionRepoBucket;
import org.apache.nifi.registry.extension.repo.ExtensionRepoGroup;
import org.apache.nifi.registry.extension.repo.ExtensionRepoVersionSummary;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;

public interface ExtensionService {

    // ----- Extension Bundles -----

    /**
     * Creates a version of an extension bundle.
     *
     * The InputStream is expected to contain the binary contents of a bundle in the format specified by bundleType.
     *
     * The metadata will be extracted from the bundle and used to determine if this is a new version of an existing bundle,
     * or it will create a new bundle and this as the first version if one doesn't already exist.
     *
     * @param bucketIdentifier the bucket id
     * @param bundleType the type of bundle
     * @param inputStream the binary content of the bundle
     * @param clientSha256 the SHA-256 hex supplied by the client
     * @return the BundleVersion representing all of the information about the bundle
     * @throws IOException if an error occurs processing the InputStream
     */
    BundleVersion createBundleVersion(String bucketIdentifier, BundleType bundleType,
                                      InputStream inputStream, String clientSha256) throws IOException;

    /**
     * Retrieves the extension bundles in the given buckets.
     *
     * @param bucketIdentifiers the bucket identifiers
     * @param filterParams the optional filter params
     * @return the bundles in the given buckets
     */
    List<Bundle> getBundles(Set<String> bucketIdentifiers, BundleFilterParams filterParams);

    /**
     * Retrieves the extension bundles in the given bucket.
     *
     * @param bucketIdentifier the bucket identifier
     * @return the bundles in the given bucket
     */
    List<Bundle> getBundlesByBucket(String bucketIdentifier);

    /**
     * Retrieve the extension bundle with the given id.
     *
     * @param extensionBundleIdentifier the extension bundle id
     * @return the bundle
     */
    Bundle getBundle(String extensionBundleIdentifier);

    /**
     * Deletes the given extension bundle and all it's versions.
     *
     * @param bundle the extension bundle to delete
     * @return the deleted bundle
     */
    Bundle deleteBundle(Bundle bundle);

    // ----- Extension Bundle Versions -----

    /**
     * Retrieves the extension bundle versions in the given buckets.
     *
     * @param bucketIdentifiers the bucket identifiers
     * @param filterParams the optional filter params
     * @return the set of extension bundle versions
     */
    SortedSet<BundleVersionMetadata> getBundleVersions(Set<String> bucketIdentifiers, BundleVersionFilterParams filterParams);

    /**
     * Retrieves the versions of the given extension bundle.
     *
     * @param extensionBundleIdentifier the extension bundle id
     * @return the sorted set of versions for the given bundle
     */
    SortedSet<BundleVersionMetadata> getBundleVersions(String extensionBundleIdentifier);

    /**
     * Retrieves the full BundleVersion object, including version metadata, bundle metadata, and bucket metadata.
     *
     * @param bundleId the bundle id
     * @param version the version
     * @return the BundleVersion
     */
    BundleVersion getBundleVersion(String bucketId, String bundleId, String version);

    /**
     * Retrieves the full BundleVersion object, including version metadata, bundle metadata, and bucket metadata.
     *
     * @param bucketId the bucket id where the bundle is located
     * @param groupId the group id of the bundle
     * @param artifactId the artifact id of the bundle
     * @param version the version of the bundle
     * @return the extension bundle version
     */
    BundleVersion getBundleVersion(String bucketId, String groupId, String artifactId, String version);

    /**
     * Writes the binary content of the extension bundle version to the given OutputStream.
     *
     * @param bundleVersion the version to write the content for
     * @param out the output stream to write to
     */
    void writeBundleVersionContent(BundleVersion bundleVersion, OutputStream out);

    /**
     * Deletes the given version of the extension bundle.
     *
     * @param bundleVersion the version to delete
     * @return the deleted extension bundle version
     */
    BundleVersion deleteBundleVersion(BundleVersion bundleVersion);

    // ----- Extension Methods ------

    /**
     * Retrieves all extensions in the given buckets, sorted by name.
     *
     * @param bucketIdentifiers the identifiers of the buckets
     * @param filterParams the filter params
     * @return extensions in the given buckets matching the filter params
     */
    SortedSet<ExtensionMetadata> getExtensionMetadata(Set<String> bucketIdentifiers, ExtensionFilterParams filterParams);

    /**
     * Retrieves the extensions in the given buckets that provided the given service API.
     *
     * This would be used when a processor has a property specifying a service API and we want to look up implementations.
     *
     * @param bucketIdentifiers the identifiers of the buckets
     * @param providedServiceAPI the provided service API
     * @return the extensions providing the given service
     */
    SortedSet<ExtensionMetadata> getExtensionMetadata(Set<String> bucketIdentifiers, ProvidedServiceAPI providedServiceAPI);

    /**
     * Retrieves the set of extensions for the given bundle version.
     *
     * @param bundleVersion the bundle version to retrieve extensions for
     * @return the set of extensions for the given bundle version
     */
    SortedSet<ExtensionMetadata> getExtensionMetadata(BundleVersion bundleVersion);

    /**
     * Retrieves the extension with the given name in the given bundle version.
     *
     * @param bundleVersion the bundle version
     * @param name the extension name
     * @return the extension
     */
    Extension getExtension(BundleVersion bundleVersion, String name);

    /**
     * Writes the documentation for the extension with the given name and bundle to the given output stream.
     *
     * @param bundleVersion the bundle version
     * @param name the name of the extension
     * @param outputStream the output stream to write to
     * @throws IOException if an error occurs writing to the output stream
     */
    void writeExtensionDocs(BundleVersion bundleVersion, String name, OutputStream outputStream) throws IOException;

    /**
     * Writes the additional details documentation for the extension with the given name and bundle to the given output stream.
     *
     * @param bundleVersion the bundle version
     * @param name the name of the extension
     * @param outputStream the output stream to write to
     * @throws IOException if an error occurs writing to the output stream
     */
    void writeAdditionalDetailsDocs(BundleVersion bundleVersion, String name, OutputStream outputStream) throws IOException;

    /**
     * @return all know tags
     */
    SortedSet<TagCount> getExtensionTags();

    // ----- Extension Repo Methods -----

    /**
     * Retrieves the extension repo buckets for the given bucket ids.
     *
     * @param bucketIds the bucket ids
     * @return the set of buckets
     */
    SortedSet<ExtensionRepoBucket> getExtensionRepoBuckets(Set<String> bucketIds);

    /**
     * Retrieves the extension repo groups for the given bucket.
     *
     * @param bucket the bucket
     * @return the groups for the bucket
     */
    SortedSet<ExtensionRepoGroup> getExtensionRepoGroups(Bucket bucket);

    /**
     * Retrieves the extension repo artifacts for the given bucket and group.
     *
     * @param bucket the bucket
     * @param groupId the group id
     * @return the artifacts for the bucket and group
     */
    SortedSet<ExtensionRepoArtifact> getExtensionRepoArtifacts(Bucket bucket, String groupId);

    /**
     * Retrieves the extension repo version summaries for the given bucket, group, and artifact.
     *
     * @param bucket the bucket
     * @param groupId the group id
     * @param artifactId the artifact id
     * @return the version summaries for the bucket, group, and artifact
     */
    SortedSet<ExtensionRepoVersionSummary> getExtensionRepoVersions(Bucket bucket, String groupId, String artifactId);

}
