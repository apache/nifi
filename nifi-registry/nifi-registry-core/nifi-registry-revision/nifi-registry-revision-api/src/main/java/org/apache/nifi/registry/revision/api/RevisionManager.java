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
package org.apache.nifi.registry.revision.api;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * <p>
 * A Revision Manager provides the ability to prevent clients of the Web API from stepping on one another.
 * This is done by providing revisions for entities individually.
 * </p>
 *
 * NOTE: This API is considered a framework level API for the NiFi ecosystem and may evolve as
 * the NiFi PMC and committers deem necessary. It is not considered a public extension point.
 */
public interface RevisionManager {

    /**
     * Returns the current Revision for the entity with the given ID. If no Revision yet exists for the
     * entity with the given ID, one will be created with a Version of 0 and no Client ID.
     *
     * @param entityId the ID of the entity
     * @return the current Revision for the entity with the given ID
     */
    Revision getRevision(String entityId);

    /**
     * Performs the given task without allowing the given Revision Claim to expire. Once this method
     * returns or an Exception is thrown (with the Exception of ExpiredRevisionClaimException),
     * the Revision may have been updated for each entity that the RevisionClaim holds a Claim for.
     * If an ExpiredRevisionClaimException is thrown, the Revisions claimed by RevisionClaim
     * will not be updated.
     *
     * @param claim the Revision Claim that is responsible for holding a Claim on the Revisions for each entity that is
     *            to be updated
     * @param task the task that is responsible for updating the entities whose Revisions are claimed by the given
     *            RevisionClaim. The returned Revision set should include a Revision for each Revision that is the
     *            supplied Revision Claim. If there exists any Revision in the provided RevisionClaim that is not part
     *            of the RevisionClaim returned by the task, then the Revision is assumed to have not been modified.
     *
     * @return a RevisionUpdate object that represents the new version of the entity that was updated
     *
     * @throws ExpiredRevisionClaimException if the Revision Claim has expired
     */
    <T> RevisionUpdate<T> updateRevision(RevisionClaim claim, UpdateRevisionTask<T> task);

    /**
     * Performs the given task that is expected to remove a entity from the flow. As a result,
     * the Revision for the entity referenced by the RevisionClaim will be removed.
     *
     * @param claim the Revision Claim that is responsible for holding a Claim on the Revisions for each entity that is
     *            to be removed
     * @param task the task that is responsible for deleting the entities whose Revisions are claimed by the given RevisionClaim
     * @return the value returned from the DeleteRevisionTask
     *
     * @throws ExpiredRevisionClaimException if the Revision Claim has expired
     */
    <T> T deleteRevision(RevisionClaim claim, DeleteRevisionTask<T> task) throws ExpiredRevisionClaimException;

    /**
     * Clears any revisions that are currently held and resets the Revision Manager so that the revisions
     * present are those provided in the given collection
     */
    void reset(Collection<Revision> revisions);

    /**
     * @return a List of all Revisions managed by this Revision Manager
     */
    List<Revision> getAllRevisions();

    /**
     * @return a Map of all Revisions where the key is the entity id
     */
    Map<String,Revision> getRevisionMap();

}