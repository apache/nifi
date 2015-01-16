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

import org.apache.nifi.cluster.context.ClusterContext;
import org.apache.nifi.cluster.context.ClusterContextThreadLocal;

/**
 * An optimistic locking manager that provides for optimistic locking in a clustered
 * environment.
 * 
 * @author unattributed
 */
public class ClusterAwareOptimisticLockingManager implements OptimisticLockingManager {

    private final OptimisticLockingManager optimisticLockingManager;
    
    public ClusterAwareOptimisticLockingManager(final OptimisticLockingManager optimisticLockingManager) {
        this.optimisticLockingManager = optimisticLockingManager;
    }
    
    @Override
    public Revision checkRevision(Revision revision) throws InvalidRevisionException {
        final Revision currentRevision = getRevision();
        if(currentRevision.equals(revision) == false) {
            throw new InvalidRevisionException(String.format("Given revision %s does not match current revision %s.", revision, currentRevision));
        } else {
            return revision.increment(revision.getClientId());
        }
    }

    @Override
    public boolean isCurrent(Revision revision) {
        return getRevision().equals(revision);
    }

    @Override
    public Revision getRevision() {
        final ClusterContext ctx = ClusterContextThreadLocal.getContext();
        if(ctx == null || ctx.getRevision() == null) {
            return optimisticLockingManager.getRevision();
        } else {
            return ctx.getRevision();
        }
    }

    @Override
    public void setRevision(final Revision revision) {
        final ClusterContext ctx = ClusterContextThreadLocal.getContext();
        if(ctx != null) {
            ctx.setRevision(revision);
        }
        optimisticLockingManager.setRevision(revision);
    }

    @Override
    public Revision incrementRevision() {
        final Revision currentRevision = getRevision();
        final Revision incRevision = currentRevision.increment();
        setRevision(incRevision);
        return incRevision;
    }

    @Override
    public Revision incrementRevision(final String clientId) {
        final Revision currentRevision = getRevision();
        final Revision incRevision = currentRevision.increment(clientId);
        setRevision(incRevision);
        return incRevision;
    }

    @Override
    public String getLastModifier() {
        return optimisticLockingManager.getLastModifier();
    }

    @Override
    public void setLastModifier(final String lastModifier) {
        optimisticLockingManager.setLastModifier(lastModifier);
    }

}
