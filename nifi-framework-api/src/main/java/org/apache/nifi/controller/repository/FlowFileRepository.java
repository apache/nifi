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

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.repository.claim.ResourceClaimManager;

/**
 * Implementations must be thread safe
 *
 */
public interface FlowFileRepository extends Closeable {

    /**
     * Initializes the Content Repository, providing to it the
     * ContentClaimManager that is to be used for interacting with Content
     * Claims
     *
     * @param claimManager for handling claims
     * @throws java.io.IOException if unable to initialize repository
     */
    void initialize(ResourceClaimManager claimManager) throws IOException;

    /**
     * @return the maximum number of bytes that can be stored in the underlying
     * storage mechanism
     *
     * @throws IOException if computing capacity fails
     */
    long getStorageCapacity() throws IOException;

    /**
     * @return the number of bytes currently available for use by the underlying
     * storage mechanism
     *
     * @throws IOException if computing usable space fails
     */
    long getUsableStorageSpace() throws IOException;

    /**
     * Returns the name of the FileStore that the repository is stored on, or <code>null</code>
     * if not applicable or unable to determine the file store name
     *
     * @return the name of the FileStore
     */
    String getFileStoreName();

    /**
     * Updates the repository with the given RepositoryRecords.
     *
     * @param records the records to update the repository with
     * @throws java.io.IOException if update fails
     */
    void updateRepository(Collection<RepositoryRecord> records) throws IOException;

    /**
     * Loads all flow files found within the repository, establishes the content
     * claims and their reference count
     *
     * @param queueProvider the provider of FlowFile Queues into which the
     * FlowFiles should be enqueued
     * @param minimumSequenceNumber specifies the minimum value that should be
     * returned by a call to {@link #getNextFlowFileSequence()}
     *
     * @return index of highest flow file identifier
     * @throws IOException if load fails
     */
    long loadFlowFiles(QueueProvider queueProvider, long minimumSequenceNumber) throws IOException;

    /**
     * @return <code>true</code> if the Repository is volatile (i.e., its data
     * is lost upon application restart), <code>false</code> otherwise
     */
    boolean isVolatile();

    /**
     * @return the next ID in sequence for creating <code>FlowFile</code>s.
     */
    long getNextFlowFileSequence();

    /**
     * @return the max ID of all <code>FlowFile</code>s that currently exist in
     * the repository.
     * @throws IOException if computing max identifier fails
     */
    long getMaxFlowFileIdentifier() throws IOException;

    /**
     * Updates the Repository to indicate that the given FlowFileRecords were
     * Swapped Out of memory
     *
     * @param swappedOut the FlowFiles that were swapped out of memory
     * @param flowFileQueue the queue that the FlowFiles belong to
     * @param swapLocation the location to which the FlowFiles were swapped
     *
     * @throws IOException if swap fails
     */
    void swapFlowFilesOut(List<FlowFileRecord> swappedOut, FlowFileQueue flowFileQueue, String swapLocation) throws IOException;

    /**
     * Updates the Repository to indicate that the given FlowFileRecords were
     * Swapped In to memory
     *
     * @param swapLocation the location (e.g., a filename) from which FlowFiles
     * were recovered
     * @param flowFileRecords the records that were swapped in
     * @param flowFileQueue the queue that the FlowFiles belong to
     *
     * @throws IOException if swap fails
     */
    void swapFlowFilesIn(String swapLocation, List<FlowFileRecord> flowFileRecords, FlowFileQueue flowFileQueue) throws IOException;
}
