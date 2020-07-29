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

package org.apache.nifi.web.revision;

import java.util.Collection;
import java.util.List;

import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.web.Revision;


/**
 * <p>
 * A Revision Manager provides the ability to prevent clients of the Web API from
 * stepping on one another. This is done by providing revisions
 * for components individually.
 * </p>
 *
 * <p>
 * Clients that will modify a resource must do so using a two-phase commit. First,
 * the client will issue a request that includes an HTTP Header of "X-Validation-Expects".
 * This indicates that the request will not actually be performed but rather that the
 * node should validate that the request could in fact be performed. If all nodes respond
 * with a 202-Accepted response, then the second phase will commence. The second phase
 * will consist of replicating the same request but without the "X-Validation-Expects" header.
 * </p>
 *
 * <p>
 * When the first phase of the two-phase commit is processed, the Revision Manager should
 * be used to retrieve the current revision by calling the {@link #getRevision(String)} method
 * to verify that the client-provided Revisions are current.
 * If the revisions are up-to-date, the request validation may continue.
 * Otherwise, the request should fail and the second phase should not be performed.
 * </p>
 *
 * <p>
 * If the first phase of the above two-phase commit completes and all nodes indicate that the
 * request may continue, this means that all nodes have agreed that the client's Revisions are
 * acceptable.
 * </p>
 */
public interface RevisionManager {

    /**
     * Returns the current Revision for the component with the given ID. If no Revision yet exists for the
     * component with the given ID, one will be created with a Version of 0 and no Client ID.
     *
     * @param componentId the ID of the component
     * @return the current Revision for the component with the given ID
     */
    Revision getRevision(String componentId);

    /**
     * Performs the given task without allowing the given Revision Claim to expire. Once this method
     * returns or an Exception is thrown (with the Exception of ExpiredRevisionClaimException),
     * the Revision may have been updated for each component that the RevisionClaim holds a Claim for.
     * If an ExpiredRevisionClaimException is thrown, the Revisions claimed by RevisionClaim
     * will not be updated.
     *
     * @param claim the Revision Claim that is responsible for holding a Claim on the Revisions for each component that is
     *            to be updated
     * @param modifier the user that is modifying the resource
     * @param task the task that is responsible for updating the components whose Revisions are claimed by the given
     *            RevisionClaim. The returned Revision set should include a Revision for each Revision that is the
     *            supplied Revision Claim. If there exists any Revision in the provided RevisionClaim that is not part
     *            of the RevisionClaim returned by the task, then the Revision is assumed to have not been modified.
     *
     * @return a RevisionUpdate object that represents the new version of the component that was updated
     *
     * @throws ExpiredRevisionClaimException if the Revision Claim has expired
     */
    <T> RevisionUpdate<T> updateRevision(RevisionClaim claim, NiFiUser modifier, UpdateRevisionTask<T> task);

    /**
     * Performs the given task that is expected to remove a component from the flow. As a result,
     * the Revision for the component referenced by the RevisionClaim will be removed.
     *
     * @param claim the Revision Claim that is responsible for holding a Claim on the Revisions for each component that is
     *            to be removed
     * @param user the user that is requesting that the revision be deleted
     * @param task the task that is responsible for deleting the components whose Revisions are claimed by the given RevisionClaim
     * @return the value returned from the DeleteRevisionTask
     *
     * @throws ExpiredRevisionClaimException if the Revision Claim has expired
     */
    <T> T deleteRevision(RevisionClaim claim, NiFiUser user, DeleteRevisionTask<T> task) throws ExpiredRevisionClaimException;

    /**
     * Clears any revisions that are currently held and resets the Revision Manager so that the revisions
     * present are those provided in the given collection
     */
    void reset(Collection<Revision> revisions);

    /**
     * @return a List of all Revisions managed by this Revision Manager
     */
    List<Revision> getAllRevisions();
}
