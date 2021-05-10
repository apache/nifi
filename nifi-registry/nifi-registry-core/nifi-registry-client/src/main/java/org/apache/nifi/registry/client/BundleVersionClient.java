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

import org.apache.nifi.registry.extension.bundle.BundleType;
import org.apache.nifi.registry.extension.bundle.BundleVersion;
import org.apache.nifi.registry.extension.bundle.BundleVersionFilterParams;
import org.apache.nifi.registry.extension.bundle.BundleVersionMetadata;
import org.apache.nifi.registry.extension.component.manifest.Extension;
import org.apache.nifi.registry.extension.component.ExtensionMetadata;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * Client for interacting with extension bundle versions.
 */
public interface BundleVersionClient {

    /**
     * Uploads a version of an extension bundle to NiFi Registry where the bundle content comes from an InputStream.
     *
     * @param bucketId the bucket where the extension bundle will leave
     * @param bundleType the type of bundle being uploaded
     * @param bundleContentStream the input stream with the binary content of the bundle
     * @return the BundleVersion entity
     *
     * @throws IOException if an I/O error occurs
     * @throws NiFiRegistryException if an non I/O error occurs
     */
    BundleVersion create(String bucketId, BundleType bundleType, InputStream bundleContentStream)
            throws IOException, NiFiRegistryException;

    /**
     * Uploads a version of an extension bundle to NiFi Registry where the bundle content comes from an InputStream.
     *
     * @param bucketId the bucket where the extension bundle will leave
     * @param bundleType the type of bundle being uploaded
     * @param bundleContentStream the input stream with the binary content of the bundle
     * @param sha256 the optional SHA-256 in hex form
     * @return the BundleVersion entity
     *
     * @throws IOException if an I/O error occurs
     * @throws NiFiRegistryException if an non I/O error occurs
     */
    BundleVersion create(String bucketId, BundleType bundleType, InputStream bundleContentStream, String sha256)
            throws IOException, NiFiRegistryException;

    /**
     * Uploads a version of an extension bundle to NiFi Registry where the bundle content comes from a File.
     *
     * @param bucketId the bucket where the extension bundle will leave
     * @param bundleType the type of bundle being uploaded
     * @param bundleFile the file with the binary content of the bundle
     * @return the BundleVersion entity
     *
     * @throws IOException if an I/O error occurs
     * @throws NiFiRegistryException if an non I/O error occurs
     */
    BundleVersion create(String bucketId, BundleType bundleType, File bundleFile)
            throws IOException, NiFiRegistryException;

    /**
     * Uploads a version of an extension bundle to NiFi Registry where the bundle content comes from a File.
     *
     * @param bucketId the bucket where the extension bundle will leave
     * @param bundleType the type of bundle being uploaded
     * @param bundleFile the file with the binary content of the bundle
     * @param sha256 the optional SHA-256 in hex form
     * @return the BundleVersion entity
     *
     * @throws IOException if an I/O error occurs
     * @throws NiFiRegistryException if an non I/O error occurs
     */
    BundleVersion create(String bucketId, BundleType bundleType, File bundleFile, String sha256)
            throws IOException, NiFiRegistryException;

    /**
     * Retrieves all the extension bundle versions located in buckets the current user is authorized for, and
     * matching any of the provided filter params.
     *
     * @param filterParams the filter params
     * @return the list of bundle version metadata
     *
     * @throws IOException if an I/O error occurs
     * @throws NiFiRegistryException if an non I/O error occurs
     */
    List<BundleVersionMetadata> getBundleVersions(BundleVersionFilterParams filterParams)
            throws IOException, NiFiRegistryException;

    /**
     * Retrieves the metadata about the versions of the given bundle.
     *
     * @param bundleId the bundle id
     * @return the list of version metadata
     *
     * @throws IOException if an I/O error occurs
     * @throws NiFiRegistryException if an non I/O error occurs
     */
    List<BundleVersionMetadata> getBundleVersions(String bundleId) throws IOException, NiFiRegistryException;

    /**
     * Retrieves bundle version entity for the given bundle id and version string.
     *
     * The entity contains all of the information about the version, such as the bucket, bundle, and version metadata.
     *
     * The binary content of the bundle can be obtained by calling {@method getBundleVersionContent}.
     *
     * @param bundleId the bundle id
     * @param version the bundle version
     * @return the BundleVersion entity
     *
     * @throws IOException if an I/O error occurs
     * @throws NiFiRegistryException if an non I/O error occurs
     */
    BundleVersion getBundleVersion(String bundleId, String version) throws IOException, NiFiRegistryException;

    /**
     * Retrieves the metadata about the extensions in the given bundle version.
     *
     * @param bundleId the bundle id
     * @param version the bundle version
     * @return the list of metadata about the extensions
     *
     * @throws IOException if an I/O error occurs
     * @throws NiFiRegistryException if an non I/O error occurs
     */
    List<ExtensionMetadata> getExtensions(String bundleId, String version) throws IOException, NiFiRegistryException;

    /**
     * Retrieves the full extension info for the extension with the given name in the given bundle version.
     *
     * @param bundleId the bundle id
     * @param version the version of the bundle
     * @param name the name of the extension
     * @return the extension info
     *
     * @throws IOException if an I/O error occurs
     * @throws NiFiRegistryException if an non I/O error occurs
     */
    Extension getExtension(String bundleId, String version, String name) throws IOException, NiFiRegistryException;

    /**
     * Obtains an InputStream for the html docs of the given extension.
     *
     * @param bundleId the bundle id
     * @param version the version of the bundle
     * @param name the name of the extensions
     * @return the InputStream for the extension docs
     *
     * @throws IOException if an I/O error occurs
     * @throws NiFiRegistryException if an non I/O error occurs
     */
    InputStream getExtensionDocs(String bundleId, String version, String name) throws IOException, NiFiRegistryException;

    /**
     * Obtains an InputStream for the binary content for the version of the given bundle.
     *
     * @param bundleId the bundle id
     * @param version the version
     * @return the InputStream for the bundle version content
     *
     * @throws IOException if an I/O error occurs
     * @throws NiFiRegistryException if an non I/O error occurs
     */
    InputStream getBundleVersionContent(String bundleId, String version) throws IOException, NiFiRegistryException;

    /**
     * Writes the binary content for the version of the given the bundle to the specified directory.
     *
     * @param bundleId the bundle id
     * @param version the bundle version
     * @param directory the directory to write to
     * @return the File object for the bundle that was written
     *
     * @throws IOException if an I/O error occurs
     * @throws NiFiRegistryException if an non I/O error occurs
     */
    File writeBundleVersionContent(String bundleId, String version, File directory) throws IOException, NiFiRegistryException;

    /**
     * Deletes the given extension bundle version.
     *
     * @param bundleId the bundle id
     * @param version the bundle version
     * @return the deleted bundle versions
     *
     * @throws IOException if an I/O error occurs
     * @throws NiFiRegistryException if an non I/O error occurs
     */
    BundleVersion delete(String bundleId, String version) throws IOException, NiFiRegistryException;

}
