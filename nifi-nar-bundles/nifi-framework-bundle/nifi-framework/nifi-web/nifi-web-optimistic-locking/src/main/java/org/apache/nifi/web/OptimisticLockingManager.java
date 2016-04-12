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

import org.apache.nifi.web.revision.RevisionManager;

/**
 * A manager for optimistic locking based on revisions. A revision is composed
 * of a client ID and a version number. Two revisions are considered equal if
 * either their version numbers match or their client IDs match.
 *
 * @deprecated This class has been deprecated in favor of {@link RevisionManager}
 */
@Deprecated
public interface OptimisticLockingManager {

    /**
     * Attempts to execute the specified configuration request using the
     * specified revision within a lock.
     *
     * @param <T> type of snapshot
     * @param revision revision
     * @param configurationRequest request
     * @return snapshot
     */
    <T> ConfigurationSnapshot<T> configureFlow(Revision revision, ConfigurationRequest<T> configurationRequest);

    /**
     * Updates the revision using the specified revision within a lock.
     *
     * @param updateRevision new revision
     */
    void setRevision(UpdateRevision updateRevision);

    /**
     * Returns the last flow modification. This is a combination of the revision
     * and the user who performed the modification.
     *
     * @return the last modification
     */
    FlowModification getLastModification();

}
