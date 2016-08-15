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
package org.apache.nifi.controller.repository.claim;

import org.apache.nifi.controller.repository.ContentRepository;

/**
 * <p>
 * Represents a resource that can be provided by a {@link ContentRepository}
 * </p>
 *
 * <p>
 * MUST BE THREAD-SAFE!
 * </p>
 */
public interface ResourceClaim extends Comparable<ResourceClaim> {

    /**
     * @return the unique identifier for this claim
     */
    String getId();

    /**
     * @return the container identifier in which this claim is held
     */
    String getContainer();

    /**
     * @return the section within a given container the claim is held
     */
    String getSection();

    /**
     * @return Indicates whether or not the Claim is loss-tolerant. If so, we will
     *         attempt to keep the content but will not sacrifice a great deal of
     *         performance to do so
     */
    boolean isLossTolerant();

    /**
     * @return <code>true</code> if the Resource Claim may still be written to, <code>false</code> if the Resource Claim
     *         will no longer be written to
     */
    boolean isWritable();

    /**
     * Indicates whether or not the Resource Claim is in use. A Resource Claim is said to be in use if either it is
     * writable or at least one Content Claim still refers to the it
     *
     * @return <code>true</code> if the Resource Claim is in use, <code>false</code> otherwise
     */
    boolean isInUse();
}
