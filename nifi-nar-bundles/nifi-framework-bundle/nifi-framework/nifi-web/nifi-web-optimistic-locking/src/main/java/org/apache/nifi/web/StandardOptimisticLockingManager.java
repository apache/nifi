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
package org.apache.nifi.web;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.nifi.cluster.context.ClusterContext;
import org.apache.nifi.cluster.context.ClusterContextThreadLocal;
import org.apache.nifi.web.security.user.NiFiUserUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements the OptimisticLockingManager interface.
 *
 */
public class StandardOptimisticLockingManager implements OptimisticLockingManager {

    private static final Logger logger = LoggerFactory.getLogger(StandardOptimisticLockingManager.class);

    private static final String INVALID_REVISION_ERROR = "Given revision %s does not match current revision %s.";
    private static final String SYNC_ERROR = "This NiFi instance has been updated by '%s'. Please refresh to synchronize the view.";

    private Revision currentRevision = new Revision(0L, "");
    private String lastModifier = "unknown";
    private final Lock lock = new ReentrantLock();

    private void lock() {
        lock.lock();
    }

    private void unlock() {
        lock.unlock();
    }

    private void checkRevision(final Revision revision) {
        final FlowModification lastMod = getLastModification();

        // with lock, verify revision
        boolean approved = lastMod.getRevision().equals(revision);

        if (!approved) {
            logger.debug("Revision check failed because current revision is " + lastMod.getRevision() + " but supplied revision is " + revision);

            if (lastMod.getRevision().getClientId() == null || lastMod.getRevision().getClientId().trim().isEmpty() || lastMod.getRevision().getVersion() == null) {
                throw new InvalidRevisionException(String.format(INVALID_REVISION_ERROR, revision, lastMod.getRevision()));
            } else {
                throw new InvalidRevisionException(String.format(SYNC_ERROR, lastMod.getLastModifier()));
            }
        }
    }

    private Revision updateRevision(final Revision updatedRevision) {
        // record the current modification
        setLastModification(new FlowModification(updatedRevision, NiFiUserUtils.getNiFiUserName()));

        // return the revision
        return updatedRevision;
    }

    @Override
    public <T> ConfigurationSnapshot<T> configureFlow(Revision revision, ConfigurationRequest<T> configurationRequest) {
        lock();
        try {
            // check the revision
            checkRevision(revision);

            // execute the configuration request
            final ConfigurationResult<T> result = configurationRequest.execute();

            // update the revision
            final Revision newRevision = updateRevision(incrementRevision(revision.getClientId()));

            // build the result
            return new ConfigurationSnapshot(newRevision.getVersion(), result.getConfiguration(), result.isNew());
        } finally {
            unlock();
        }
    }

    @Override
    public void setRevision(UpdateRevision updateRevision) {
        lock();
        try {
            final Revision updatedRevision = updateRevision.execute(getLastModification().getRevision());

            // update the revision
            if (updatedRevision != null) {
                updateRevision(updatedRevision);
            }
        } finally {
            unlock();
        }
    }

    @Override
    public FlowModification getLastModification() {
        lock();
        try {
            final Revision revision;
            final ClusterContext ctx = ClusterContextThreadLocal.getContext();
            if (ctx == null || ctx.getRevision() == null) {
                revision = currentRevision;
            } else {
                revision = ctx.getRevision();
            }

            return new FlowModification(revision, lastModifier);
        } finally {
            unlock();
        }
    }

    private void setLastModification(final FlowModification lastModification) {
        lock();
        try {
            // record the last modifier
            lastModifier = lastModification.getLastModifier();

            // record the updated revision in the cluster context if possible
            final ClusterContext ctx = ClusterContextThreadLocal.getContext();
            if (ctx != null) {
                ctx.setRevision(lastModification.getRevision());
            } else {
                currentRevision = lastModification.getRevision();
            }
        } finally {
            unlock();
        }
    }

    private Revision incrementRevision(String clientId) {
        final Revision current = getLastModification().getRevision();

        final long incrementedVersion;
        if (current.getVersion() == null) {
            incrementedVersion = 0;
        } else {
            incrementedVersion = current.getVersion() + 1;
        }
        return new Revision(incrementedVersion, clientId);
    }

}
