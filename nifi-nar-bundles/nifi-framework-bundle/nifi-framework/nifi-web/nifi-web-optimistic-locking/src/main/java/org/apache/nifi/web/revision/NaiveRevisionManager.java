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
import java.util.stream.Collectors;

import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
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

    public NaiveRevisionManager(final NiFiProperties properties) {
        this(getRequestTimeoutMillis(properties), TimeUnit.MILLISECONDS);
    }

    /**
     * Constructs a new NaiveRevisionManager that uses the given amount of time as the expiration time
     * for a Revision Claims
     *
     * @param claimExpiration how long a Revision Claim should last
     * @param timeUnit the TimeUnit of 'claimExpiration'
     */
    public NaiveRevisionManager(final long claimExpiration, final TimeUnit timeUnit) {
        this.claimExpirationNanos = timeUnit.toNanos(claimExpiration);
    }

    private static long getRequestTimeoutMillis(final NiFiProperties properties) {
        return FormatUtils.getTimeDuration(properties.getProperty(NiFiProperties.REQUEST_REPLICATION_CLAIM_TIMEOUT,
            NiFiProperties.DEFAULT_REQUEST_REPLICATION_CLAIM_TIMEOUT), TimeUnit.MILLISECONDS);
    }

    @Override
    public RevisionClaim requestClaim(final Revision revision, final NiFiUser user) throws InvalidRevisionException {
        Objects.requireNonNull(user);
        return requestClaim(Collections.singleton(revision), user);
    }

    @Override
    public void reset(final Collection<Revision> revisions) {
        final Map<String, RevisionLock> copy;
        synchronized (this) {
            copy = new HashMap<>(revisionLockMap);
            revisionLockMap.clear();

            for (final Revision revision : revisions) {
                revisionLockMap.put(revision.getComponentId(), new RevisionLock(new FlowModification(revision, null), claimExpirationNanos));
            }
        }

        for (final RevisionLock lock : copy.values()) {
            lock.clear();
        }
    }

    @Override
    public List<Revision> getAllRevisions() {
        return revisionLockMap.values().stream()
            .map(lock -> lock.getRevision())
            .collect(Collectors.toList());
    }

    @Override
    public RevisionClaim requestClaim(final Collection<Revision> revisions, final NiFiUser user) {
        Objects.requireNonNull(user);
        logger.debug("Attempting to claim Revisions {}", revisions);

        // Try to obtain a Revision Claim (temporary lock) on all revisions
        final List<Revision> revisionList = new ArrayList<>(revisions);
        revisionList.sort(new RevisionComparator());

        ClaimResult failedClaimResult = null;
        final Set<RevisionLock> locksObtained = new HashSet<>();
        for (int i = 0; i < revisionList.size(); i++) {
            final Revision revision = revisionList.get(i);
            final RevisionLock revisionLock = getRevisionLock(revision);

            final ClaimResult claimResult = revisionLock.requestClaim(revision, user);
            logger.trace("Obtained Revision Claim for {}", revision);

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
            logger.trace("Obtained Revision Claim for all components");

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
        logger.debug("Failed to obtain all necessary Revisions; releasing claims for {}", locksObtained);
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
        final RevisionLock revisionLock = getRevisionLock(new Revision(0L, null, componentId));
        return revisionLock.getRevision();
    }

    @Override
    public <T> T deleteRevision(final RevisionClaim claim, final NiFiUser user, final DeleteRevisionTask<T> task) throws ExpiredRevisionClaimException {
        Objects.requireNonNull(user);
        logger.debug("Attempting to delete revision using {}", claim);
        int successCount = 0;
        final List<Revision> revisionList = new ArrayList<>(claim.getRevisions());
        revisionList.sort(new RevisionComparator());

        String failedId = null;
        for (final Revision revision : revisionList) {
            final RevisionLock revisionLock = getRevisionLock(revision);
            final boolean verified = revisionLock.requestWriteLock(revision, user);

            if (verified) {
                logger.trace("Verified Revision Claim for {}", revision);
                successCount++;
            } else {
                logger.debug("Failed to verify Revision Claim for {}", revision);
                failedId = revision.getComponentId();
                break;
            }
        }

        if (successCount == revisionList.size()) {
            logger.debug("Successfully verified Revision Claim for all revisions {}", claim);

            final T taskValue;
            try {
                taskValue = task.performTask();
            } catch (final Exception e) {
                logger.debug("Failed to perform Claim Deletion task. Will relinquish the Revision Claims for the following revisions: {}", revisionList);

                for (final Revision revision : revisionList) {
                    final RevisionLock revisionLock = getRevisionLock(revision);
                    revisionLock.unlock(revision, revision, user.getUserName());
                    logger.debug("Relinquished lock for {}", revision);
                }

                throw e;
            }

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
            revisionLock.relinquishRevisionClaim(revision, null);
            logger.debug("Relinquished lock for {}", revision);
        }

        // Throw an Exception indicating that we failed to obtain the locks
        throw new ExpiredRevisionClaimException("Invalid Revision was given for component with ID '" + failedId + "'");
    }

    @Override
    public <T> RevisionUpdate<T> updateRevision(final RevisionClaim originalClaim, final NiFiUser user, final UpdateRevisionTask<T> task) throws ExpiredRevisionClaimException {
        Objects.requireNonNull(user);
        int successCount = 0;
        logger.debug("Attempting to update revision using {}", originalClaim);

        final List<Revision> revisionList = new ArrayList<>(originalClaim.getRevisions());
        revisionList.sort(new RevisionComparator());

        String failedId = null;
        for (final Revision revision : revisionList) {
            final RevisionLock revisionLock = getRevisionLock(revision);
            final boolean verified = revisionLock.requestWriteLock(revision, user);

            if (verified) {
                logger.trace("Verified Revision Claim for {}", revision);
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
                    final Revision updatedRevision = updatedRevisions.get(revision);
                    getRevisionLock(revision).unlock(revision, updatedRevision, user.getUserName());

                    if (updatedRevision.getVersion() != revision.getVersion()) {
                        logger.debug("Unlocked Revision {} and updated associated Version to {}", revision, updatedRevision.getVersion());
                    } else {
                        logger.debug("Unlocked Revision {} without updating Version", revision);
                    }
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
    public boolean releaseClaim(final RevisionClaim claim, final NiFiUser user) {
        Objects.requireNonNull(user);
        boolean success = true;

        final List<Revision> revisions = new ArrayList<>(claim.getRevisions());
        revisions.sort(new RevisionComparator());

        for (final Revision revision : revisions) {
            final RevisionLock revisionLock = getRevisionLock(revision);
            success = revisionLock.relinquishRevisionClaim(revision, user) && success;
        }

        return success;
    }

    @Override
    public boolean cancelClaim(String componentId) {
        logger.debug("Attempting to cancel claim for component {}", componentId);
        final Revision revision = new Revision(0L, null, componentId);

        final RevisionLock revisionLock = getRevisionLock(revision);
        if (revisionLock == null) {
            logger.debug("No Revision Lock exists for Component {} - there is no claim to cancel", componentId);
            return false;
        }

        return revisionLock.releaseClaimIfCurrentThread(null);
    }

    @Override
    public boolean cancelClaim(Revision revision) {
        logger.debug("Attempting to cancel claim for {}", revision);

        final RevisionLock revisionLock = getRevisionLock(revision);
        if (revisionLock == null) {
            logger.debug("No Revision Lock exists for {} - there is no claim to cancel", revision);
            return false;
        }

        return revisionLock.releaseClaimIfCurrentThread(revision);
    }

    @Override
    public boolean cancelClaims(final Set<Revision> revisions) {
        boolean successful = false;
        for (final Revision revision : revisions) {
            successful = cancelClaim(revision);
        }

        return successful;
    }

    @Override
    public <T> T get(final String componentId, final ReadOnlyRevisionCallback<T> callback) {
        final RevisionLock revisionLock = getRevisionLock(new Revision(0L, null, componentId));
        logger.debug("Attempting to obtain read lock for {}", revisionLock.getRevision());
        revisionLock.acquireReadLock(null, revisionLock.getRevision().getClientId());
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

        logger.debug("Will attempt to obtain read locks for components {}", componentIds);
        for (final String componentId : sortedIds) {
            final RevisionLock revisionLock = getRevisionLock(new Revision(0L, null, componentId));

            logger.trace("Attempting to obtain read lock for {}", revisionLock.getRevision());
            revisionLock.acquireReadLock(null, revisionLock.getRevision().getClientId());
            revisionLocks.push(revisionLock);
            logger.trace("Obtained read lock for {}", revisionLock.getRevision());
        }

        logger.debug("Obtained read lock for all necessary components {}; calling call-back", componentIds);
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

    private synchronized void deleteRevisionLock(final Revision revision) {
        final RevisionLock revisionLock = revisionLockMap.remove(revision.getComponentId());
        if (revisionLock == null) {
            return;
        }

        revisionLock.releaseClaim();
    }

    private synchronized RevisionLock getRevisionLock(final Revision revision) {
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
        public ClaimResult requestClaim(final Revision proposedRevision, final NiFiUser user) {
            // acquire the claim, blocking if necessary.
            acquireClaim(user, proposedRevision.getClientId());

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
        public boolean requestWriteLock(final Revision proposedRevision, final NiFiUser user) throws ExpiredRevisionClaimException {
            Objects.requireNonNull(proposedRevision);
            threadLock.writeLock().lock();

            boolean releaseLock = true;
            try {
                if (getRevision().equals(proposedRevision)) {
                    final LockStamp stamp = lockStamp.get();

                    if (stamp == null) {
                        final IllegalStateException ise = new IllegalStateException("No claim has been obtained for " + proposedRevision + " so cannot lock the component for modification");
                        logger.debug("Attempted to obtain write lock for {} but no Claim was obtained; throwing IllegalStateException", proposedRevision, ise);
                        throw ise;
                    }

                    final boolean userEqual = stamp.getUser() == null || stamp.getUser().equals(user);
                    if (!userEqual) {
                        logger.debug("Failed to verify {} because the User was not the same as the Lock Stamp's User (Lock Stamp was {})", proposedRevision, stamp);
                        throw new InvalidRevisionException("Cannot obtain write lock for " + proposedRevision + " because it was claimed by " + stamp.getUser());
                    }

                    final boolean clientIdEqual = stamp.getClientId() == null || stamp.getClientId().equals(proposedRevision.getClientId());
                    if (!clientIdEqual) {
                        logger.debug("Failed to verify {} because the Client ID was not the same as the Lock Stamp's Client ID (Lock Stamp was {})", proposedRevision, stamp);
                        throw new InvalidRevisionException("Cannot obtain write lock for " + proposedRevision + " because it was claimed with a different Client ID");
                    }

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
                        throw new ExpiredRevisionClaimException("Claim for " + proposedRevision + " has expired");
                    }

                    // Intentionally leave the thread lock in a locked state!
                    releaseLock = false;
                    return true;
                }
            } finally {
                if (releaseLock) {
                    threadLock.writeLock().unlock();
                }
            }

            return false;
        }

        private void acquireClaim(final NiFiUser user, final String clientId) {
            while (true) {
                final LockStamp stamp = lockStamp.get();

                if (stamp == null || stamp.isExpired()) {
                    final long now = System.nanoTime();
                    final boolean lockObtained = lockStamp.compareAndSet(stamp, new LockStamp(user, clientId, now + lockNanos));
                    if (lockObtained) {
                        return;
                    }
                } else {
                    Thread.yield();
                }
            }
        }

        public void acquireReadLock(final NiFiUser user, final String clientId) {
            // Wait until we can claim the lock stamp
            boolean obtained = false;
            while (!obtained) {
                // If the lock stamp is not null, then there is either an active Claim or a
                // write lock held. Wait until it is null and then replace it atomically
                // with a LockStamp that does not expire (expiration time is Long.MAX_VALUE).
                final LockStamp curStamp = lockStamp.get();
                final boolean nullOrExpired = (curStamp == null || curStamp.isExpired());
                obtained = nullOrExpired && lockStamp.compareAndSet(curStamp, new LockStamp(user, clientId, Long.MAX_VALUE));

                if (!obtained) {
                    // Could not obtain lock. Yield so that we don't sit around doing nothing with the thread.
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

        public void clear() {
            threadLock.writeLock().lock();
            try {
                releaseClaim();
            } finally {
                threadLock.writeLock().unlock();
            }
        }

        public boolean releaseClaimIfCurrentThread(final Revision revision) {
            threadLock.writeLock().lock();
            try {
                final LockStamp stamp = lockStamp.get();
                if (stamp == null) {
                    logger.debug("Cannot cancel claim for {} because there is no claim held", getRevision());
                    return false;
                }

                if (revision != null && !getRevision().equals(revision)) {
                    throw new InvalidRevisionException("Cannot release claim because the provided Revision is not valid");
                }

                if (stamp.isObtainedByCurrentThread()) {
                    releaseClaim();
                    logger.debug("Successfully canceled claim for {}", getRevision());
                    return true;
                }

                logger.debug("Cannot cancel claim for {} because it is held by Thread {} and current Thread is {}",
                    getRevision(), stamp.obtainingThread, Thread.currentThread().getName());
                return false;
            } finally {
                threadLock.writeLock().unlock();
            }
        }

        /**
         * Releases the Revision Claim if and only if the current revision matches the proposed revision
         *
         * @param proposedRevision the proposed revision to check against the current revision
         * @return <code>true</code> if the Revision Claim was relinquished, <code>false</code> otherwise
         */
        public boolean relinquishRevisionClaim(final Revision proposedRevision, final NiFiUser user) {
            threadLock.writeLock().lock();
            try {
                final LockStamp stamp = lockStamp.get();
                final boolean userOk = stamp == null || stamp.getUser().equals(user);
                if (userOk) {
                    if (getRevision().equals(proposedRevision)) {
                        releaseClaim();
                        return true;
                    }
                } else {
                    throw new InvalidRevisionException("Cannot relinquish claim for " + proposedRevision + " because it was claimed by " + stamp.getUser());
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

            final NiFiUser user;
            final String clientId;
            if (stamp == null) {
                user = null;
                clientId = null;
            } else {
                user = stamp.getUser();
                clientId = stamp.getClientId();
            }

            lockStamp.set(new LockStamp(user, clientId, timestamp));
        }

        public Revision getRevision() {
            final FlowModification lastMod = lastModReference.get();
            return (lastMod == null) ? null : lastMod.getRevision();
        }
    }


    private static class LockStamp {
        private final NiFiUser user;
        private final String clientId;
        private final long expirationTimestamp;
        private final Thread obtainingThread;

        public LockStamp(final NiFiUser user, final String clientId, final long expirationTimestamp) {
            this.user = user;
            this.clientId = clientId;
            this.expirationTimestamp = expirationTimestamp;
            this.obtainingThread = Thread.currentThread();
        }

        public NiFiUser getUser() {
            return user;
        }

        public String getClientId() {
            return clientId;
        }

        public boolean isExpired() {
            return System.nanoTime() > expirationTimestamp;
        }

        public boolean isObtainedByCurrentThread() {
            return obtainingThread == Thread.currentThread();
        }

        @Override
        public String toString() {
            return "LockStamp[user=" + user + ", clientId=" + clientId + ", expired=" + isExpired() + "]";
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
