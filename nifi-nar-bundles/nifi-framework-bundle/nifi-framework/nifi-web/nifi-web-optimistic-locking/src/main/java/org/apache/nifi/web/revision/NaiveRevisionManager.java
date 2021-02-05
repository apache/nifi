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

import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.web.InvalidRevisionException;
import org.apache.nifi.web.Revision;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * <p>
 * This class implements a naive approach for Revision Management.
 * Each call into the Revision Manager will block until any previously held
 * lock is expired or unlocked. This provides a very simple solution but can
 * likely be improved by allowing, for instance, multiple threads to obtain
 * temporary locks simultaneously, etc.
 * </p>
 */
public class NaiveRevisionManager implements RevisionManager {
    private static final Logger logger = LoggerFactory.getLogger(NaiveRevisionManager.class);

    private final ConcurrentMap<String, Revision> revisionMap = new ConcurrentHashMap<>();
    private final AtomicLong revisionUpdateCounter = new AtomicLong(0L);

    @Override
    public synchronized void reset(final RevisionSnapshot revisionSnapshot) {
        final Collection<Revision> revisions = revisionSnapshot.getRevisions();
        logger.info("Resetting Revisions for all components. {} revisions will be removed, {} will be added", revisionMap.size(), revisions.size());
        logger.debug("New Revisions: {}", revisions);

        revisionMap.clear();

        for (final Revision revision : revisions) {
            revisionMap.put(revision.getComponentId(), revision);
        }

        revisionUpdateCounter.set(revisionSnapshot.getRevisionUpdateCount());
    }

    @Override
    public synchronized RevisionSnapshot getAllRevisions() {
        return new RevisionSnapshot(revisionMap.values(), revisionUpdateCounter.get());
    }

    @Override
    public synchronized long getRevisionUpdateCount() {
        return revisionUpdateCounter.get();
    }

    @Override
    public synchronized Revision getRevision(final String componentId) {
        return revisionMap.computeIfAbsent(componentId, id -> new Revision(0L, null, componentId));
    }

    @Override
    public synchronized <T> T deleteRevision(final RevisionClaim claim, final NiFiUser user, final DeleteRevisionTask<T> task) throws ExpiredRevisionClaimException {
        Objects.requireNonNull(user);
        logger.debug("Attempting to delete revision using {}", claim);
        final List<Revision> revisionList = new ArrayList<>(claim.getRevisions());
        revisionList.sort(new RevisionComparator());

        // Verify the provided revisions.
        String failedId = null;
        for (final Revision revision : revisionList) {
            final Revision curRevision = getRevision(revision.getComponentId());
            if (!curRevision.equals(revision)) {
                throw new ExpiredRevisionClaimException("Invalid Revision was given for component with ID '" + failedId + "'");
            }
        }

        // Perform the action provided
        final T taskResult = task.performTask();

        for (final Revision revision : revisionList) {
            revisionMap.remove(revision.getComponentId());
        }

        revisionUpdateCounter.addAndGet(revisionList.size());

        return taskResult;
    }

    @Override
    public synchronized <T> RevisionUpdate<T> updateRevision(final RevisionClaim originalClaim, final NiFiUser user, final UpdateRevisionTask<T> task) throws ExpiredRevisionClaimException {
        Objects.requireNonNull(user);
        logger.debug("Attempting to update revision using {}", originalClaim);

        final List<Revision> revisionList = new ArrayList<>(originalClaim.getRevisions());
        revisionList.sort(new RevisionComparator());

        for (final Revision revision : revisionList) {
            final Revision currentRevision = getRevision(revision.getComponentId());
            final boolean verified = revision.equals(currentRevision);

            if (!verified) {
                // Throw an Exception indicating that we failed to obtain the locks
                throw new InvalidRevisionException("Invalid Revision was given for component with ID '" + revision.getComponentId() + "'");
            }
        }

        // We successfully verified all revisions.
        logger.debug("Successfully verified Revision Claim for all revisions");

        // Perform the update
        final RevisionUpdate<T> updatedComponent = task.update();

        // If the update succeeded then put the updated revisions into the revisionMap
        // If an exception is thrown during the update we don't want to update revision so it is ok to bounce out of this method
        if (updatedComponent != null) {
            final Set<Revision> updatedRevisions = updatedComponent.getUpdatedRevisions();
            for (final Revision updatedRevision : updatedRevisions) {
                revisionMap.put(updatedRevision.getComponentId(), updatedRevision);
            }

            revisionUpdateCounter.addAndGet(updatedRevisions.size());
        }

        return updatedComponent;
    }

}
