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
package org.apache.nifi.registry.revision.naive;

import org.apache.nifi.registry.revision.api.DeleteRevisionTask;
import org.apache.nifi.registry.revision.api.EntityModification;
import org.apache.nifi.registry.revision.api.ExpiredRevisionClaimException;
import org.apache.nifi.registry.revision.api.InvalidRevisionException;
import org.apache.nifi.registry.revision.api.Revision;
import org.apache.nifi.registry.revision.api.RevisionClaim;
import org.apache.nifi.registry.revision.api.RevisionManager;
import org.apache.nifi.registry.revision.api.RevisionUpdate;
import org.apache.nifi.registry.revision.api.UpdateResult;
import org.apache.nifi.registry.revision.api.UpdateRevisionTask;
import org.apache.nifi.registry.revision.standard.RevisionComparator;
import org.apache.nifi.registry.revision.standard.StandardRevisionUpdate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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


    @Override
    public void reset(final Collection<Revision> revisions) {
        synchronized (this) { // avoid allowing two threads to reset versions concurrently
            revisionMap.clear();

            for (final Revision revision : revisions) {
                revisionMap.put(revision.getEntityId(), revision);
            }
        }
    }

    @Override
    public List<Revision> getAllRevisions() {
        return new ArrayList<>(revisionMap.values());
    }

    @Override
    public Map<String, Revision> getRevisionMap() {
        return new HashMap<>(revisionMap);
    }

    @Override
    public Revision getRevision(final String componentId) {
        return revisionMap.computeIfAbsent(componentId, id -> new Revision(0L, null, componentId));
    }

    @Override
    public <T> T deleteRevision(final RevisionClaim claim, final DeleteRevisionTask<T> task) throws ExpiredRevisionClaimException {
        logger.debug("Attempting to delete revision using {}", claim);
        final List<Revision> revisionList = new ArrayList<>(claim.getRevisions());
        revisionList.sort(new RevisionComparator());

        // Verify the provided revisions.
        String failedId = null;
        for (final Revision revision : revisionList) {
            final Revision curRevision = getRevision(revision.getEntityId());
            if (!curRevision.equals(revision)) {
                throw new ExpiredRevisionClaimException("Invalid Revision was given for entity with ID '" + failedId + "'");
            }
        }

        // Perform the action provided
        final T taskResult = task.performTask();

        for (final Revision revision : revisionList) {
            revisionMap.remove(revision.getEntityId());
        }

        return taskResult;
    }

    @Override
    public <T> RevisionUpdate<T> updateRevision(final RevisionClaim originalClaim, final UpdateRevisionTask<T> task)
            throws ExpiredRevisionClaimException {
        logger.debug("Attempting to update revision using {}", originalClaim);

        final List<Revision> revisionList = new ArrayList<>(originalClaim.getRevisions());
        revisionList.sort(new RevisionComparator());

        for (final Revision revision : revisionList) {
            final Revision currentRevision = getRevision(revision.getEntityId());
            final boolean verified = revision.equals(currentRevision);

            if (!verified) {
                // Throw an Exception indicating that we failed to obtain the locks
                throw new InvalidRevisionException("Invalid Revision was given for entity with ID '" + revision.getEntityId() + "'");
            }
        }

        // We successfully verified all revisions.
        logger.debug("Successfully verified Revision Claim for all revisions");

        // Perform the update
        // If an exception is thrown we don't want to update revision so it is ok to bounce out of this method
        final UpdateResult<T> updateResult = task.update();
        if (updateResult == null) {
            return null;
        }

        // The update succeeded so increment the revisions
        final Set<Revision> incrementedRevisions = new HashSet<>();
        for (final Revision incomingRevision : revisionList) {
            final String entityId = incomingRevision.getEntityId();
            final String clientId = incomingRevision.getClientId();

            // retrieve the revision from the map here because the incoming revision may have been
            // verified based on the client id and may not contain the latest version
            final Revision existingRevision = revisionMap.get(entityId);
            final Revision incrementedRevision = existingRevision.incrementRevision(clientId);
            incrementedRevisions.add(incrementedRevision);

            revisionMap.put(entityId, incrementedRevision);
        }

        // Create the result with the updated entity and updated revisions
        final T updatedEntity = updateResult.getEntity();
        final String updaterIdentity = updateResult.updaterIdentity();

        final Revision updatedEntityRevision = revisionMap.get(updateResult.getEntityId());
        final EntityModification entityModification = new EntityModification(updatedEntityRevision, updaterIdentity);

        return new StandardRevisionUpdate<>(updatedEntity, entityModification, incrementedRevisions);
    }

}
