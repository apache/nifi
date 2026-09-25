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

import java.io.IOException;

/**
 * Content Repository that supports transferring Content Claims to a backing Content Repository.
 */
public interface TransferableContentClaimRepository extends ContentRepository {

    /**
     * Prepares a Content Claim from the backing Content Repository. The supplied claim is returned when it already references the backing Content Repository.
     *
     * @param claim the claim to prepare
     * @return a Content Claim whose content is stored in the backing Content Repository
     * @throws IOException if the content cannot be written to the backing Content Repository
     */
    ContentClaim prepareBackingClaim(ContentClaim claim) throws IOException;

    /**
     * Completes the transfer of a prepared Content Claim.
     *
     * @param claim the original claim passed to {@link #prepareBackingClaim(ContentClaim)}
     */
    void completeBackingClaimTransfer(ContentClaim claim);
}
