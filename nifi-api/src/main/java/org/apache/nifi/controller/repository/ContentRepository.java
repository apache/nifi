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
package org.apache.nifi.controller.repository;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Set;

import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.controller.repository.claim.ContentClaimManager;

/**
 * Defines the capabilities of a content repository. Append options are not
 * available on the methods but a merge capability is provided which between
 * that and creating new claims a merge is available.
 *
 * @author none
 */
public interface ContentRepository {

    /**
     * Initializes the Content Repository, providing to it the
     * ContentClaimManager that is to be used for interacting with Content
     * Claims
     *
     * @param claimManager
     * @throws java.io.IOException
     */
    void initialize(ContentClaimManager claimManager) throws IOException;

    /**
     * Shuts down the Content Repository, freeing any resources that may be held.
     * This is called when an administrator shuts down NiFi.
     */
    void shutdown();
    
    /**
     * Returns the names of all Containers that exist for this Content
     * Repository
     *
     * @return
     */
    Set<String> getContainerNames();

    /**
     * Returns the maximum number of bytes that can be stored in the storage
     * mechanism that backs the container with the given name
     *
     * @param containerName
     * @return
     * @throws java.io.IOException
     *
     * @throws IllegalArgumentException if no container exists with the given
     * name
     */
    long getContainerCapacity(String containerName) throws IOException;

    /**
     * Returns the number of bytes available to be used used by the storage
     * mechanism that backs the container with the given name
     *
     * @param containerName
     * @return
     * @throws java.io.IOException
     *
     * @throws IllegalArgumentException if no container exists with the given
     * name
     */
    long getContainerUsableSpace(String containerName) throws IOException;

    /**
     * Creates a new content claim
     *
     * @param lossTolerant indicates whether the content for the new claim is
     * loss tolerant. If true the repository might choose more volatile storage
     * options which could increase performance for a tradeoff with reliability
     * @return newly created claim
     * @throws java.io.IOException
     */
    ContentClaim create(boolean lossTolerant) throws IOException;

    /**
     * Increments the number of claimants for the given claim
     *
     * @param claim
     * @return the number of claimants after incrementing
     */
    int incrementClaimaintCount(ContentClaim claim);

    /**
     * Obtains the current number of claimants for the given claim
     *
     * @param claim
     * @return the number of claimants
     */
    int getClaimantCount(ContentClaim claim);

    /**
     * Reduces the number of claimants for the given claim. Even if the given
     * claim is null or content cannot be found or removed no exception will be
     * thrown.
     *
     * @param claim
     * @return 
     */
    int decrementClaimantCount(ContentClaim claim);

    /**
     * Removes the content indicated by the given claim
     *
     * @param claim
     *
     * @return a boolean indicating whether or not the destruction of the claim
     * was successful
     */
    boolean remove(ContentClaim claim);

    /**
     * Clones the content for the given content claim and returns content claim
     * of the new object
     *
     * @param original
     * @param lossTolerant
     * @return
     * @throws IOException if an IO error occurs. Any content written to the new
     * destination prior to the error will be destroyed
     */
    ContentClaim clone(ContentClaim original, boolean lossTolerant) throws IOException;

    /**
     * Creates a new content item that is the merger in iteration order of all
     * content for the given claims
     *
     * @return the size of the destination
     * @param claims the claims to merge which will be combined in order of
     * collection iteration
     * @param destination the claim to write the merged content to
     * @param header if supplied will be prepended to the output
     * @param footer if supplied will be appended to the output
     * @param demarcator if supplied will be placed in between each merged
     * object
     * @throws IOException
     * @throws IllegalArgumentException if the given destination is included in
     * the given claims
     */
    long merge(Collection<ContentClaim> claims, ContentClaim destination, byte[] header, byte[] footer, byte[] demarcator) throws IOException;

    /**
     * Imports content from the given path creating a new content object and
     * claim within the repository.
     *
     * @return the size of the claim
     * @param content
     * @param claim the claim to write imported content to
     * @throws IOException
     */
    long importFrom(Path content, ContentClaim claim) throws IOException;

    /**
     * Imports content from the given path to the specified claim, appending or
     * replacing the current claim, according to the value of the append
     * argument
     *
     * @return the size of the claim
     * @param content
     * @param claim the claim to write imported content to
     * @param append if true, the content will be appended to the claim; if
     * false, the content will replace the contents of the claim
     * @throws IOException
     */
    long importFrom(Path content, ContentClaim claim, boolean append) throws IOException;

    /**
     * Imports content from the given stream creating a new content object and
     * claim within the repository.
     *
     * @return the size of the claim
     * @param content
     * @param claim the claim to write imported content to
     * @throws IOException
     */
    long importFrom(InputStream content, ContentClaim claim) throws IOException;

    /**
     * Imports content from the given stream, appending or replacing the current
     * claim, according to the value of the appen dargument
     *
     * @param content
     * @param claim
     * @param append
     * @return
     * @throws IOException
     */
    long importFrom(InputStream content, ContentClaim claim, boolean append) throws IOException;

    /**
     * Exports the content of the given claim to the given destination.
     *
     * @return the size of the destination or the claim
     * @param claim
     * @param destination where to export data
     * @param append if true appends to the destination; false overwrites
     * @throws IOException if an IO error occurs. The state of the content for
     * the given destination is unknown and callers should consider whether they
     * should clean up any partially created paths
     */
    long exportTo(ContentClaim claim, Path destination, boolean append) throws IOException;

    /**
     * Exports the content of the given claim to the given destination.
     *
     * @return the size of the destination or the claim
     * @param claim
     * @param destination where to export data
     * @param append if true appends to the destination; false overwrites
     * @param offset the offset at which the claim should start being copied
     * @param length the number of bytes to copy
     * @throws IOException if an IO error occurs. The state of the content for
     * the given destination is unknown and callers should consider whether they
     * should clean up any partially created paths
     */
    long exportTo(ContentClaim claim, Path destination, boolean append, long offset, long length) throws IOException;

    /**
     * Exports the content of the given claim to the given destination.
     *
     * @return the size of the claim
     * @param claim
     * @param destination where to export data
     * @throws IOException if an IO error occurs.
     */
    long exportTo(ContentClaim claim, OutputStream destination) throws IOException;

    /**
     * Exports a subset of the content of the given claim, starting at offset
     * and copying length bytes, to the given destination.
     *
     * @return the number of bytes copied
     * @param claim
     * @param destination where to export data
     * @param offset the offset into the claim at which the copy should begin
     * @param length the number of bytes to copy
     * @throws IOException if an IO error occurs.
     */
    long exportTo(ContentClaim claim, OutputStream destination, long offset, long length) throws IOException;

    /**
     * @param claim
     * @return size in bytes of content for given claim
     * @throws IOException
     */
    long size(ContentClaim claim) throws IOException;

    /**
     * Provides access to the input stream for the given claim
     *
     * @param claim
     * @return InputStream over the content of the given claim
     * @throws IOException
     */
    InputStream read(ContentClaim claim) throws IOException;

    /**
     * Obtains an OutputStream to the content for the given claim.
     *
     * @param claim
     * @return
     * @throws IOException
     */
    OutputStream write(ContentClaim claim) throws IOException;

    /**
     * Purges the contents of the repository, as if the repository were newly
     * created.
     */
    void purge();

    /**
     * Performs any cleanup actions that may need to be taken upon system
     * restart. For example, if content was partially written to the repository
     * before the restart, the repository is given a chance to handle this data
     */
    void cleanup();

    /**
     * Returns a boolean indicating whether or not the content specified by the
     * given claim can be read, regardless of whether the content has been
     * archived or not. If the ContentRepository does not implement archiving
     * capabilities, this method will return <code>false</code>.
     *
     * @param contentClaim the Content Claim to check
     * @return
     *
     * @throws IOException
     */
    boolean isAccessible(ContentClaim contentClaim) throws IOException;
}
