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

import java.text.Collator;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import org.apache.nifi.web.FlowModification;
import org.apache.nifi.web.InvalidRevisionException;
import org.apache.nifi.web.Revision;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private final long claimExpirationNanos;
    private final ConcurrentMap<String, RevisionLock> revisionLockMap = new ConcurrentHashMap<>();

    public NaiveRevisionManager() {
        this(1, TimeUnit.MINUTES);
    }

    /**
     * Constructs a new NaiveRevisionManager that uses the given number of Nanoseconds as the expiration time
     * for a Revision Claims
     *
     * @param claimNanos the number of nanoseconds that a Revision Claim should last
     */
    public NaiveRevisionManager(final long claimExpiration, final TimeUnit timeUnit) {
        this.claimExpirationNanos = timeUnit.toNanos(claimExpiration);
    }

    @Override
    public RevisionClaim requestClaim(Revision revision) throws InvalidRevisionException {
        return requestClaim(Collections.singleton(revision));
    }

    @Override
    public RevisionClaim requestClaim(final Collection<Revision> revisions) {
        logger.debug("Attempting to claim Revisions {}", revisions);

        // Try to obtain a Revision Claim (temporary lock) on all revisions
        final List<Revision> revisionList = new ArrayList<>(revisions);
        revisionList.sort(new RevisionComparator());

        ClaimResult failedClaimResult = null;
        final Set<RevisionLock> locksObtained = new HashSet<>();
        for (int i = 0; i < revisionList.size(); i++) {
            final Revision revision = revisionList.get(i);
            final RevisionLock revisionLock = getRevisionLock(revision);

            final ClaimResult claimResult = revisionLock.requestClaim(revision);
            logger.debug("Obtained Revision Claim for {}", revision);

            if (claimResult.isSuccessful()) {
                locksObtained.add(revisionLock);
            } else {
                logger.debug("Failed to obtain Revision Claim for component with ID {} because Current Revision is {} but supplied Revision is {}",
                    revision.getComponentId(), claimResult.getLastModification().getRevision(), revision);

                failedClaimResult = claimResult;
                break;
            }
        }

        // if we got a Revision Claim on each Revision, return a successful result
        if (locksObtained.size() == revisionList.size()) {
            logger.debug("Obtained Revision Claim for all components");

            // it's possible that obtaining the locks took a while if we are obtaining
            // many. Renew the timestamp to ensure that the first locks obtained don't
            // expire too quickly.
            final long timestamp = System.nanoTime() + claimExpirationNanos;
            for (final RevisionLock revisionLock : locksObtained) {
                revisionLock.renewExpiration(timestamp);
            }

            return new StandardRevisionClaim(revisions);
        }

        // We failed to obtain all of the Revision Claims necessary. Since
        // we need this call to atomically obtain all or nothing, we have to now
        // release the locks that we did obtain.
        for (final RevisionLock revisionLock : locksObtained) {
            revisionLock.releaseClaim();
        }

        final FlowModification lastMod = failedClaimResult.getLastModification();
        if (lastMod.getRevision().getClientId() == null || lastMod.getRevision().getClientId().trim().isEmpty() || lastMod.getRevision().getVersion() == null) {
            throw new InvalidRevisionException(String.format("Given revision %s does not match current revision %s.",
                failedClaimResult.getProposedRevision(), lastMod.getRevision()));
        } else {
            throw new InvalidRevisionException(String.format("Component %s has been updated by '%s'. Please refresh to synchronize the view.",
                failedClaimResult.getProposedRevision().getComponentId(), lastMod.getLastModifier()));
        }
    }

    @Override
    public Revision getRevision(final String componentId) {
        final RevisionLock revisionLock = revisionLockMap.computeIfAbsent(componentId,
            id -> new RevisionLock(new FlowModification(new Revision(0L, null, componentId), null), claimExpirationNanos));

        return revisionLock.getRevision();
    }

    @Override
    public <T> T deleteRevision(final RevisionClaim claim, final DeleteRevisionTask<T> task) throws ExpiredRevisionClaimException {
        logger.debug("Attempting to delete revision using {}", claim);
        int successCount = 0;
        final List<Revision> revisionList = new ArrayList<>(claim.getRevisions());
        revisionList.sort(new RevisionComparator());

        String failedId = null;
        for (final Revision revision : revisionList) {
            final RevisionLock revisionLock = getRevisionLock(revision);
            final boolean verified = revisionLock.requestWriteLock(revision);

            if (verified) {
                logger.debug("Verified Revision Claim for {}", revision);
                successCount++;
            } else {
                logger.debug("Failed to verify Revision Claim for {}", revision);
                failedId = revision.getComponentId();
                break;
            }
        }

        if (successCount == revisionList.size()) {
            logger.debug("Successfully verified Revision Claim for all revisions");

            final T taskValue = task.performTask();
            for (final Revision revision : revisionList) {
                deleteRevisionLock(revision);
                logger.debug("Deleted Revision {}", revision);
            }

            return taskValue;
        }

        // We failed to obtain a thread lock for all revisions. Relinquish
        // any Revision Claims that we have
        for (int i = 0; i < successCount; i++) {
            final Revision revision = revisionList.get(i);
            final RevisionLock revisionLock = getRevisionLock(revision);
            revisionLock.relinquishRevisionClaim(revision);
            logger.debug("Relinquished lock for {}", revision);
        }

        // Throw an Exception indicating that we failed to obtain the locks
        throw new ExpiredRevisionClaimException("Invalid Revision was given for component with ID '" + failedId + "'");
    }

    @Override
    public <T> RevisionUpdate<T> updateRevision(final RevisionClaim originalClaim, final String modifier, final UpdateRevisionTask<T> task) throws ExpiredRevisionClaimException {
        int successCount = 0;
        logger.debug("Attempting to update revision using {}", originalClaim);

        final List<Revision> revisionList = new ArrayList<>(originalClaim.getRevisions());
        revisionList.sort(new RevisionComparator());

        String failedId = null;
        for (final Revision revision : revisionList) {
            final RevisionLock revisionLock = getRevisionLock(revision);
            final boolean verified = revisionLock.requestWriteLock(revision);

            if (verified) {
                logger.debug("Verified Revision Claim for {}", revision);
                successCount++;
            } else {
                logger.debug("Failed to verify Revision Claim for {}", revision);
                failedId = revision.getComponentId();
                break;
            }
        }

        // We successfully verified all revisions.
        if (successCount == revisionList.size()) {
            logger.debug("Successfully verified Revision Claim for all revisions");

            RevisionUpdate<T> updatedComponent = null;
            try {
                updatedComponent = task.update();
            } finally {
                // Release the lock that we are holding and update the revision.
                // To do this, we need to map the old revision to the new revision
                // so that we have an efficient way to lookup the pairing, so that
                // we can easily obtain the old revision and the new revision for
                // the same component in order to call #unlock on the RevisionLock
                final Map<Revision, Revision> updatedRevisions = new HashMap<>();
                final Map<String, Revision> revisionsByComponentId = new HashMap<>();
                for (final Revision revision : revisionList) {
                    updatedRevisions.put(revision, revision);
                    revisionsByComponentId.put(revision.getComponentId(), revision);
                }

                if (updatedComponent != null) {
                    for (final Revision updatedRevision : updatedComponent.getUpdatedRevisions()) {
                        final Revision oldRevision = revisionsByComponentId.get(updatedRevision.getComponentId());
                        if (oldRevision != null) {
                            updatedRevisions.put(oldRevision, updatedRevision);
                        }
                    }
                }

                for (final Revision revision : revisionList) {
                    getRevisionLock(revision).unlock(revision, updatedRevisions.get(revision), modifier);
                }
            }

            return updatedComponent;
        }

        // We failed to obtain a thread lock for all revisions. Relinquish
        // any Revision Claims that we have
        for (int i = 0; i < successCount; i++) {
            final Revision revision = revisionList.get(i);
            final RevisionLock revisionLock = getRevisionLock(revision);
            revisionLock.cancelWriteLock();
            logger.debug("Relinquished lock for {}", revision);
        }

        // Throw an Exception indicating that we failed to obtain the locks
        throw new InvalidRevisionException("Invalid Revision was given for component with ID '" + failedId + "'");
    }

    @Override
    public boolean releaseClaim(final RevisionClaim claim) {
        boolean success = true;

        final List<Revision> revisions = new ArrayList<>(claim.getRevisions());
        revisions.sort(new RevisionComparator());

        for (final Revision revision : revisions) {
            final RevisionLock revisionLock = getRevisionLock(revision);
            success = revisionLock.relinquishRevisionClaim(revision) && success;
        }

        return success;
    }

    @Override
    public <T> T get(final String componentId, final ReadOnlyRevisionCallback<T> callback) {
        final RevisionLock revisionLock = revisionLockMap.computeIfAbsent(componentId, id -> new RevisionLock(new FlowModification(new Revision(0L, null, id), null), claimExpirationNanos));
        logger.debug("Attempting to obtain read lock for {}", revisionLock.getRevision());
        revisionLock.acquireReadLock();
        logger.debug("Obtained read lock for {}", revisionLock.getRevision());

        try {
            return callback.withRevision(revisionLock.getRevision());
        } finally {
            logger.debug("Releasing read lock for {}", revisionLock.getRevision());
            revisionLock.relinquishReadLock();
        }
    }

    @Override
    public <T> T get(final Set<String> componentIds, final Supplier<T> callback) {
        final List<String> sortedIds = new ArrayList<>(componentIds);
        sortedIds.sort(Collator.getInstance());

        final Stack<RevisionLock> revisionLocks = new Stack<>();
        for (final String componentId : sortedIds) {
            final RevisionLock revisionLock = revisionLockMap.computeIfAbsent(componentId, id -> new RevisionLock(new FlowModification(new Revision(0L, null, id), null), claimExpirationNanos));
            logger.debug("Attempting to obtain read lock for {}", revisionLock.getRevision());
            revisionLock.acquireReadLock();
            revisionLocks.push(revisionLock);
            logger.debug("Obtained read lock for {}", revisionLock.getRevision());
        }

        logger.debug("Obtained read lock for all necessary components; calling call-back");
        try {
            return callback.get();
        } finally {
            while (!revisionLocks.isEmpty()) {
                final RevisionLock lock = revisionLocks.pop();
                logger.debug("Releasing read lock for {}", lock.getRevision());
                lock.relinquishReadLock();
            }
        }
    }

    private void deleteRevisionLock(final Revision revision) {
        final RevisionLock revisionLock = revisionLockMap.remove(revision.getComponentId());
        if (revisionLock == null) {
            return;
        }


        revisionLock.releaseClaim();
    }

    private RevisionLock getRevisionLock(final Revision revision) {
        return revisionLockMap.computeIfAbsent(revision.getComponentId(), id -> new RevisionLock(new FlowModification(revision, null), claimExpirationNanos));
    }


    private static class RevisionLock {
        private final AtomicReference<FlowModification> lastModReference = new AtomicReference<>();
        private final AtomicReference<LockStamp> lockStamp = new AtomicReference<>();
        private final long lockNanos;
        private final ReadWriteLock threadLock = new ReentrantReadWriteLock();

        public RevisionLock(final FlowModification lastMod, final long lockNanos) {
            this.lockNanos = lockNanos;
            lastModReference.set(lastMod);
        }

        /**
         * Requests that a Revision Claim be granted for the proposed Revision
         *
         * @param proposedRevision the revision to obtain a Claim for
         *
         * @return <code>true</code> if the Revision is valid and a Claim has been granted, <code>false</code> otherwise
         */
        public ClaimResult requestClaim(final Revision proposedRevision) {
            // acquire the claim, blocking if necessary.
            acquireClaim(proposedRevision.getClientId());

            threadLock.writeLock().lock();
            try {
                // check if the revision is correct
                final FlowModification lastModification = lastModReference.get();
                final Revision currentRevision = lastModification.getRevision();
                if (proposedRevision.equals(currentRevision)) {
                    // revision is correct - return true
                    return new ClaimResult(true, lastModification, proposedRevision);
                }

                // revision is incorrect. Release the Claim and return false
                releaseClaim();
                logger.debug("Cannot obtain Revision Claim {} because the Revision is out-of-date. Current revision is {}", proposedRevision, currentRevision);
                return new ClaimResult(false, lastModification, proposedRevision);
            } finally {
                threadLock.writeLock().unlock();
            }
        }

        /**
         * Verifies that the given Revision has a Claim against it already and that the Claim belongs
         * to the same client as the given Revision. If so, upgrades the Revision Claim to a lock that
         * will not be relinquished until the {@link #unlock(Revision)} method is called.
         *
         * @param proposedRevision the current Revision
         * @return <code>true</code> if the Revision Claim was upgraded to a lock, <code>false</code> otherwise
         * @throws ExpiredRevisionClaimException if the Revision Claim for the given Revision has already expired
         */
        public boolean requestWriteLock(final Revision proposedRevision) throws ExpiredRevisionClaimException {
            Objects.requireNonNull(proposedRevision);
            threadLock.writeLock().lock();

            if (getRevision().equals(proposedRevision)) {
                final LockStamp stamp = lockStamp.get();

                if (stamp == null) {
                    logger.debug("Attempted to obtain write lock for {} but no Claim was obtained", proposedRevision);
                    throw new IllegalStateException("No claim has been obtained for " + proposedRevision + " so cannot lock the component for modification");
                }

                if (stamp.getClientId() == null || stamp.getClientId().equals(proposedRevision.getClientId())) {
                    // TODO - Must make sure that we don't have an expired stamp if it is the result of another
                    // operation taking a long time. I.e., Client A fires off two requests for Component X. If the
                    // first one takes 2 minutes to complete, it should not result in the second request getting
                    // rejected. I.e., we want to ensure that if the request is received before the Claim expired,
                    // that we do not throw an ExpiredRevisionClaimException. Expiration of the Revision is intended
                    // only to avoid the case where a node obtains a Claim and then the node is lost or otherwise does
                    // not fulfill the second phase of the two-phase commit.
                    // We may need a Queue of updates (queue would need to be bounded, with a request getting
                    // rejected if queue is full).
                    if (stamp.isExpired()) {
                        threadLock.writeLock().unlock();
                        throw new ExpiredRevisionClaimException("Claim for " + proposedRevision + " has expired");
                    }

                    // Intentionally leave the thread lock in a locked state!
                    return true;
                } else {
                    logger.debug("Failed to verify {} because the Client ID was not the same as the Lock Stamp's Client ID (Lock Stamp was {})", proposedRevision, stamp);
                }
            }

            // revision is wrong. Unlock thread lock and return false
            threadLock.writeLock().unlock();
            return false;
        }

        private void acquireClaim(final String clientId) {
            while (true) {
                final LockStamp stamp = lockStamp.get();

                if (stamp == null || stamp.isExpired()) {
                    final long now = System.nanoTime();
                    final boolean lockObtained = lockStamp.compareAndSet(stamp, new LockStamp(clientId, now + lockNanos));
                    if (lockObtained) {
                        return;
                    }
                } else {
                    Thread.yield();
                }
            }
        }

        public void acquireReadLock() {
            // Wait until we can claim the lock stamp
            boolean obtained = false;
            while (!obtained) {
                // If the lock stamp is not null, then there is either an active Claim or a
                // write lock held. Wait until it is null and then replace it atomically
                // with a LockStamp that does not expire (expiration time is Long.MAX_VALUE).
                final LockStamp curStamp = lockStamp.get();
                obtained = (curStamp == null || curStamp.isExpired()) && lockStamp.compareAndSet(curStamp, new LockStamp(null, Long.MAX_VALUE));

                if (!obtained) {
                    // Could not obtain lock. Yield so that we don't sit
                    // around doing nothing with the thread.
                    Thread.yield();
                }
            }

            // Now we can obtain the read lock without problem.
            threadLock.readLock().lock();
        }

        public void relinquishReadLock() {
            lockStamp.set(null);
            threadLock.readLock().unlock();
        }

        private void releaseClaim() {
            lockStamp.set(null);
        }

        /**
         * Releases the Revision Claim if and only if the current revision matches the proposed revision
         *
         * @param proposedRevision the proposed revision to check against the current revision
         * @return <code>true</code> if the Revision Claim was relinquished, <code>false</code> otherwise
         */
        public boolean relinquishRevisionClaim(final Revision proposedRevision) {
            threadLock.writeLock().lock();
            try {
                if (getRevision().equals(proposedRevision)) {
                    releaseClaim();
                    return true;
                }

                return false;
            } finally {
                threadLock.writeLock().unlock();
            }
        }

        /**
         * Releases the lock and any Revision Claim that is held for the given Revision and
         * updates the revision
         *
         * @param proposedRevision the current Revision
         * @param updatedRevision the Revision to update the current revision to
         */
        public void unlock(final Revision proposedRevision, final Revision updatedRevision, final String modifier) {
            final Revision curRevision = getRevision();
            if (curRevision == null) {
                throw new IllegalMonitorStateException("Cannot unlock " + proposedRevision + " because it is not locked");
            }

            if (!curRevision.equals(proposedRevision)) {
                // Intentionally leave the thread lock in a locked state!
                throw new IllegalMonitorStateException("Cannot unlock " + proposedRevision + " because the version is not valid");
            }

            lastModReference.set(new FlowModification(updatedRevision, modifier));

            // Set stamp to null to indicate that it is not locked.
            releaseClaim();

            // Thread Lock should already be locked if this is called.
            threadLock.writeLock().unlock();
        }

        public void cancelWriteLock() {
            releaseClaim();
            threadLock.writeLock().unlock();
        }

        /**
         * Updates expiration time to the given timestamp
         *
         * @param timestamp the new expiration timestamp in nanoseconds
         */
        public void renewExpiration(final long timestamp) {
            final LockStamp stamp = lockStamp.get();
            final String clientId = stamp == null ? null : stamp.getClientId();
            lockStamp.set(new LockStamp(clientId, timestamp));
        }

        public Revision getRevision() {
            final FlowModification lastMod = lastModReference.get();
            return (lastMod == null) ? null : lastMod.getRevision();
        }
    }


    private static class LockStamp {
        private final String clientId;
        private final long expirationTimestamp;

        public LockStamp(final String clientId, final long expirationTimestamp) {
            this.clientId = clientId;
            this.expirationTimestamp = expirationTimestamp;
        }

        public String getClientId() {
            return clientId;
        }

        public boolean isExpired() {
            return System.nanoTime() > expirationTimestamp;
        }

        @Override
        public String toString() {
            return clientId;
        }
    }

    private static class ClaimResult {
        private final boolean successful;
        private final FlowModification lastMod;
        private final Revision proposedRevision;

        public ClaimResult(final boolean successful, final FlowModification lastMod, final Revision proposedRevision) {
            this.successful = successful;
            this.lastMod = lastMod;
            this.proposedRevision = proposedRevision;
        }

        public boolean isSuccessful() {
            return successful;
        }

        public FlowModification getLastModification() {
            return lastMod;
        }

        public Revision getProposedRevision() {
            return proposedRevision;
        }
    }
}
