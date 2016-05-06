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
import java.util.Set;
import java.util.function.Supplier;

import org.apache.nifi.web.InvalidRevisionException;
import org.apache.nifi.web.Revision;


/**
 * <p>
 * A Revision Manager provides the ability to prevent clients of the Web API from
 * stepping on one another. This is done by providing claims and locking mechanisms
 * for components individually.
 * </p>
 *
 * <p>
 * Clients that will modify a resource must do so using a two-phase commit. First,
 * the client will issue a request that includes an HTTP Header of "X-NcmExpects".
 * This indicates that the request will not actually be performed but rather that the
 * node should validate that the request could in fact be performed. If all nodes respond
 * with a 150-Continue response, then the second phase will commence. The second phase
 * will consist of replicating the same request but without the "X-NcmExpects" header.
 * </p>
 *
 * <p>
 * When the first phase of the two-phase commit is processed, the Revision Manager should
 * be used to obtain a Revision Claim by calling the {@link #requestClaim(Collection)}
 * method. If a Claim is granted, then the request validation may continue. If the
 * Claim is not granted, the request should fail and the second phase should not
 * be performed.
 * </p>
 *
 * <p>
 * If the first phase of the above two-phase commit completes and all nodes indicate that the
 * request may continue, this means that all nodes have provided granted a Claim on the Revisions
 * that are relevant. This Claim will automatically expire after some time. This expiration
 * means that if the node that issues the first phase never initiates the second phase (if the node
 * dies or loses network connectivitiy, for instance), then the Revision Claim will expire and
 * the Revision will remain unchanged.
 * </p>
 *
 * <p>
 * When the second phase begins, changes to the resource(s) must be made with the Revisions
 * locked. This is accomplished by wrapping the logic in a {@link Runnable} and passing the Runnable,
 * along with the {@link RevisionClaim} to the {@link #updateRevision(RevisionClaim, Supplier)} method.
 * </p>
 */
public interface RevisionManager {

    /**
     * <p>
     * Attempts to obtain a Revision Claim for Revisions supplied. If a Revision Claim
     * is granted, no other thread will be allowed to modify any of the components for
     * which a Revision is claimed until either the Revision Claim is relinquished by
     * calling the {@link #updateRevision(RevisionClaim, Runnable)} method or the
     * {@link #releaseClaim(RevisionClaim)} method, or the Revision Claim expires.
     * </p>
     *
     * <p>
     * This method is atomic. If a Revision Claim is unable to be obtained for any of the
     * provided Revisions, then no Revision Claim will be obtained.
     * </p>
     *
     * @param revisions a Set of Revisions, each of which corresponds to a different
     *            component for which a Claim is to be acquired.
     *
     * @return the Revision Claim that was granted, if one was granted.
     *
     * @throws InvalidRevisionException if any of the Revisions provided is out-of-date.
     */
    RevisionClaim requestClaim(Collection<Revision> revisions) throws InvalidRevisionException;

    /**
     * <p>
     * A convenience method that will call {@link #requestClaim(Collection)} by wrapping the given
     * Revision in a Collection
     * </p>
     *
     * @param revision the revision to request a claim for
     *
     * @return the Revision Claim that was granted, if one was granted.
     *
     * @throws InvalidRevisionException if any of the Revisions provided is out-of-date.
     */
    RevisionClaim requestClaim(Revision revision) throws InvalidRevisionException;

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
     * @param modifier the name of the entity that is modifying the resource
     * @param task the task that is responsible for updating the components whose Revisions are claimed by the given
     *            RevisionClaim. The returned Revision set should include a Revision for each Revision that is the
     *            supplied Revision Claim. If there exists any Revision in the provided RevisionClaim that is not part
     *            of the RevisionClaim returned by the task, then the Revision is assumed to have not been modified.
     *
     * @return a RevisionUpdate object that represents the new version of the component that was updated
     *
     * @throws ExpiredRevisionClaimException if the Revision Claim has expired
     */
    <T> RevisionUpdate<T> updateRevision(RevisionClaim claim, String modifier, UpdateRevisionTask<T> task) throws ExpiredRevisionClaimException;

    /**
     * Performs the given task that is expected to remove a component from the flow. As a result,
     * the Revision for the component referenced by the RevisionClaim will be removed.
     *
     * @param claim the Revision Claim that is responsible for holding a Claim on the Revisions for each component that is
     *            to be removed
     * @param task the task that is responsible for deleting the components whose Revisions are claimed by the given RevisionClaim
     * @return the value returned from the DeleteRevisionTask
     *
     * @throws ExpiredRevisionClaimException if the Revision Claim has expired
     */
    <T> T deleteRevision(RevisionClaim claim, DeleteRevisionTask<T> task) throws ExpiredRevisionClaimException;

    /**
     * Performs some operation to obtain an object of type T whose identifier is provided via
     * the componentId argument, and return that object of type T while holding a Read Lock on
     * the Revision for that object. Note that the callback provided must never modify the object
     * with the given ID.
     *
     * @param callback the callback that is to be performed with the Read Lock held
     * @return the value returned from the callback
     */
    <T> T get(String componentId, ReadOnlyRevisionCallback<T> callback);

    /**
     * Performs some operation to obtain an object of type T whose identifier is provided via
     * the componentId argument, and return that object of type T while holding a Read Lock on
     * the Revision for that object. Note that the callback provided must never modify the object
     * with the given ID.
     *
     * @param callback the callback that is to be performed with the Read Lock held
     * @return the value returned from the callback
     */
    <T> T get(Set<String> componentId, Supplier<T> callback);

    /**
     * Releases the claims on the revisions held by the given Revision Claim, if all of the Revisions
     * are up-to-date.
     *
     * @param claim the claim that holds the revisions
     *
     * @return <code>true</code> if the claim was released, <code>false</code> if the Revisions were not
     *         up-to-date
     */
    boolean releaseClaim(RevisionClaim claim);
}
