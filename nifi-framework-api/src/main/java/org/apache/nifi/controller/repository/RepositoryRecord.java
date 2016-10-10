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

import java.util.List;

import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.repository.claim.ContentClaim;

/**
 * Represents an abstraction of a FlowFile that can be used to track changing
 * state of a FlowFile so that transactionality with repositories can exist
 */
public interface RepositoryRecord {

    /**
     * @return FlowFileQueue to which the FlowFile is to be transferred
     */
    FlowFileQueue getDestination();

    /**
     * @return FlowFileQueue from which the record was pulled
     */
    FlowFileQueue getOriginalQueue();

    /**
     * @return type of update that this record encapsulates
     */
    RepositoryRecordType getType();

    /**
     * @return current ContentClaim for the FlowFile
     */
    ContentClaim getCurrentClaim();

    /**
     * @return original ContentClaim for the FlowFile before any changes were made
     */
    ContentClaim getOriginalClaim();

    /**
     * @return byte offset into the Content Claim where this FlowFile's content
     * begins
     */
    long getCurrentClaimOffset();

    /**
     * @return FlowFile being encapsulated by this record
     */
    FlowFileRecord getCurrent();

    /**
     * @return Whether or not the FlowFile's attributes have changed since the FlowFile
     * was pulled from its queue (or created)
     */
    boolean isAttributesChanged();

    /**
     * @return <code>true</code> if the FlowFile is not viable and should be
     * aborted (e.g., the content of the FlowFile cannot be found)
     */
    boolean isMarkedForAbort();

    /**
     * @return If the FlowFile is swapped out of the Java heap space, provides the
     * location of the swap file, or <code>null</code> if the FlowFile is not
     * swapped out
     */
    String getSwapLocation();

    /**
     * @return a List of Content Claims that are "transient," meaning that they existed only for the
     *         life of the Process Session in which they were created and should not be persisted.
     */
    List<ContentClaim> getTransientClaims();
}
