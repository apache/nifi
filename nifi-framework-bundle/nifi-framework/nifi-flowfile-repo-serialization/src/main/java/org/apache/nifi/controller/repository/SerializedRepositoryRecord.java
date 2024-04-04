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

import org.apache.nifi.controller.repository.claim.ContentClaim;

public interface SerializedRepositoryRecord {

    /**
     * @return the ID of the FlowFile Queue that this Record belongs to
     */
    String getQueueIdentifier();

    /**
     * @return type of update that this record encapsulates
     */
    RepositoryRecordType getType();

    /**
     * @return ContentClaim for the FlowFile
     */
    ContentClaim getContentClaim();

    /**
     * @return byte offset into the Content Claim where this FlowFile's content begins
     */
    long getClaimOffset();

    /**
     * @return the swap location for swap in/swap out records
     */
    String getSwapLocation();

    /**
     * @return FlowFile being encapsulated by this record
     */
    FlowFileRecord getFlowFileRecord();

    /**
     * @return whether or not the record is marked for abort
     */
    boolean isMarkedForAbort();

    /**
     * @return whether or not attributes have changed since the record was last persisted
     */
    boolean isAttributesChanged();
}
