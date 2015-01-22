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
 * Implements the OptimisticLockingManager interface.
 *
 * @author unattributed
 */
public class StandardOptimisticLockingManager implements OptimisticLockingManager {

    private Revision currentRevision = new Revision(0L, "");

    private String lastModifier = "unknown";

    @Override
    public Revision checkRevision(Revision revision) throws InvalidRevisionException {
        if (currentRevision.equals(revision) == false) {
            throw new InvalidRevisionException(String.format("Given revision %s does not match current revision %s.", revision, currentRevision));
        } else {
            currentRevision = revision.increment(revision.getClientId());
            return currentRevision;
        }
    }

    @Override
    public boolean isCurrent(Revision revision) {
        return currentRevision.equals(revision);
    }

    @Override
    public Revision getRevision() {
        return currentRevision;
    }

    @Override
    public void setRevision(Revision revision) {
        currentRevision = revision;
    }

    @Override
    public Revision incrementRevision() {
        currentRevision = currentRevision.increment();
        return currentRevision;
    }

    @Override
    public Revision incrementRevision(String clientId) {
        currentRevision = currentRevision.increment(clientId);
        return currentRevision;
    }

    @Override
    public String getLastModifier() {
        return lastModifier;
    }

    @Override
    public void setLastModifier(String lastModifier) {
        this.lastModifier = lastModifier;
    }

}
