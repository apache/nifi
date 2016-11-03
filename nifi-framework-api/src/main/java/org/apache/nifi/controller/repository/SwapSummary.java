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

import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.controller.repository.claim.ResourceClaim;

/**
 * <p>
 * Provides a summary of the information that is stored in a FlowFile swap file.
 * </p>
 */
public interface SwapSummary {
    /**
     * @return a QueueSize that represents the number of FlowFiles are in the swap file and their
     *         aggregate content size
     */
    QueueSize getQueueSize();

    /**
     * @return the largest ID of any of the FlowFiles that are contained in the swap file
     */
    Long getMaxFlowFileId();

    /**
     * Returns a List of all ResourceClaims that are referenced by the FlowFiles in the swap file.
     * This List may well contain the same ResourceClaim many times. This indicates that many FlowFiles
     * reference the same ResourceClaim.
     *
     * @return a List of all ResourceClaims that are referenced by the FlowFiles in the swap file
     */
    List<ResourceClaim> getResourceClaims();
}
