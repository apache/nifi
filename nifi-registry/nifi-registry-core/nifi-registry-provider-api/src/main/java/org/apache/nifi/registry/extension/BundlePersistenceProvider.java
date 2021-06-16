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
package org.apache.nifi.registry.extension;

import org.apache.nifi.registry.provider.Provider;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * Responsible for storing and retrieving the binary content of a version of an extension bundle.
 */
public interface BundlePersistenceProvider extends Provider {

    /**
     * Persists the binary content of a version of an extension bundle.
     *
     * This method should throw a BundlePersistenceException if content already exists for the BundleVersionCoordinate
     * specified in the BundlePersistenceContext.
     *
     * @param context the context about the bundle version being persisted
     * @param contentStream the stream of binary content to persist
     * @throws BundlePersistenceException if an error occurs storing the content, or if content already exists for version coordinate
     */
    void createBundleVersion(BundlePersistenceContext context, InputStream contentStream) throws BundlePersistenceException;

    /**
     * Updates the binary content for a version of an extension bundle.
     *
     * @param context the context about the bundle version being updated
     * @param contentStream the stream of the updated binary content
     * @throws BundlePersistenceException if an error occurs storing the content
     */
    void updateBundleVersion(BundlePersistenceContext context, InputStream contentStream) throws BundlePersistenceException;

    /**
     * Writes the binary content of the bundle specified by the bucket-group-artifact-version to the provided OutputStream.
     *
     * @param versionCoordinate the versionCoordinate of the bundle version
     * @param outputStream the output stream to write the contents to
     * @throws BundlePersistenceException if an error occurs retrieving the content
     */
    void getBundleVersionContent(BundleVersionCoordinate versionCoordinate, OutputStream outputStream) throws BundlePersistenceException;

    /**
     * Deletes the content of the bundle version specified by bucket-group-artifact-version.
     *
     * @param versionCoordinate the versionCoordinate of the bundle version
     * @throws BundlePersistenceException if an error occurs deleting the content
     */
    void deleteBundleVersion(BundleVersionCoordinate versionCoordinate) throws BundlePersistenceException;

    /**
     * Deletes the content for all versions of the bundle specified by group-artifact.
     *
     * @param bundleCoordinate the coordinate of the bundle to delete all versions for
     * @throws BundlePersistenceException if an error occurs deleting the content
     */
    void deleteAllBundleVersions(BundleCoordinate bundleCoordinate) throws BundlePersistenceException;

}
