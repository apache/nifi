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

import java.io.Serializable;

/**
 * A model object representing a revision. Equality is defined as matching
 * component ID and either a matching version number or matching non-empty client IDs.
 *
 * @Immutable
 * @Threadsafe
 */
public class Revision implements Serializable {
    private static final long serialVersionUID = 988658790374170022L;

    /**
     * the version number
     */
    private final Long version;

    /**
     * the client ID
     */
    private final String clientId;

    /**
     * the ID of the component that this revision belongs to, or <code>null</code> if
     * the revision is not attached to any component but rather is attached to the entire
     * data flow.
     */
    private final String componentId;

    public Revision(Long version, String clientId, String componentId) {
        if (version == null) {
            throw new IllegalArgumentException("The revision must be specified.");
        }
        if (componentId == null) {
            throw new IllegalArgumentException("The componentId must be specified.");
        }

        this.version = version;
        this.clientId = clientId;
        this.componentId = componentId;
    }

    public String getClientId() {
        return clientId;
    }

    public Long getVersion() {
        return version;
    }

    public String getComponentId() {
        return componentId;
    }

    /**
     * Returns a new Revision that has the same Client ID and Component ID as this one
     * but with a larger version
     *
     * @return the updated Revision
     */
    public Revision incrementRevision(final String clientId) {
        return new Revision(version + 1, clientId, componentId);
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }

        if ((obj instanceof Revision) == false) {
            return false;
        }

        Revision thatRevision = (Revision) obj;
        // ensure that component ID's are the same (including null)
        if (thatRevision.getComponentId() == null && getComponentId() != null) {
            return false;
        }
        if (thatRevision.getComponentId() != null && getComponentId() == null) {
            return false;
        }
        if (thatRevision.getComponentId() != null && !thatRevision.getComponentId().equals(getComponentId())) {
            return false;
        }

        if (this.version != null && this.version.equals(thatRevision.version)) {
            return true;
        } else {
            return clientId != null && !clientId.trim().isEmpty() && clientId.equals(thatRevision.getClientId());
        }

    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 59 * hash + (this.componentId != null ? this.componentId.hashCode() : 0);
        hash = 59 * hash + (this.version != null ? this.version.hashCode() : 0);
        hash = 59 * hash + (this.clientId != null ? this.clientId.hashCode() : 0);
        return hash;
    }

    @Override
    public String toString() {
        return "[" + version + ", " + clientId + ", " + componentId + ']';
    }
}
