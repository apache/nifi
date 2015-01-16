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

/**
 * A manager for optimistic locking based on revisions. A revision is composed
 * of a client ID and a version number. Two revisions are considered equal if
 * either their version numbers match or their client IDs match.
 *
 * @author unattributed
 */
public interface OptimisticLockingManager {

    /**
     * Checks the specified revision against the current revision. If the check
     * succeeds, then the current revision's version is incremented and the
     * current revision's client ID is set to the given revision's client ID.
     *
     * If the given revision's version is null, then the revision's client ID
     * must match for the current revision's client ID for the check to succeed.
     *
     * If the versions and the clientIds do not match, then an
     * InvalidRevisionException.
     *
     * @param revision the revision to check
     *
     * @return the current revision
     *
     * @throws InvalidRevisionException if the given revision does not match the
     * current revision
     */
    Revision checkRevision(Revision revision) throws InvalidRevisionException;

    /**
     * Returns true if the given revision matches the current revision.
     *
     * @param revision a revision
     * @return true if given revision is current; false otherwise.
     */
    boolean isCurrent(Revision revision);

    /**
     * @return the current revision
     */
    Revision getRevision();

    /**
     * Sets the current revision.
     *
     * @param revision a revision
     */
    void setRevision(Revision revision);

    /**
     * Increments the current revision's version.
     *
     * @return the current revision
     */
    Revision incrementRevision();

    /**
     * Increments the current revision's version and sets the current revision's
     * client ID to the given client ID.
     *
     * @param clientId a client ID
     * @return the current revision
     */
    Revision incrementRevision(String clientId);

    /**
     * @return the last modifier.
     */
    String getLastModifier();

    /**
     * Sets the last modifier.
     *
     * @param lastModifier the last modifier
     */
    void setLastModifier(String lastModifier);
}
