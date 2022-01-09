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
package org.apache.nifi.registry.revision.api;

/**
 * A revision for an entity which is made up of the entity id, a version, and an optional client id.
 *
 * NOTE: This API is considered a framework level API for the NiFi ecosystem and may evolve as
 * the NiFi PMC and committers deem necessary. It is not considered a public extension point.
 */
public class Revision {

    private final Long version;
    private final String clientId;
    private final String entityId;

    /**
     * @param version the version number for the revision
     * @param clientId the id of the client creating the revision, or null if one is not provided
     * @param entityId the id of the component the revision belongs to
     */
    public Revision(final Long version, final String clientId, final String entityId) {
        if (version == null) {
            throw new IllegalArgumentException("The revision must be specified.");
        }
        if (entityId == null) {
            throw new IllegalArgumentException("The entityId must be specified.");
        }

        this.version = version;
        this.clientId = clientId;
        this.entityId = entityId;
    }

    public String getClientId() {
        return clientId;
    }

    public Long getVersion() {
        return version;
    }

    public String getEntityId() {
        return entityId;
    }

    /**
     * Returns a new Revision that has the same Client ID and Component ID as this one, but with a larger version.
     *
     * @return the updated Revision
     */
    public Revision incrementRevision(final String clientId) {
        return new Revision(version + 1, clientId, entityId);
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

        final Revision thatRevision = (Revision) obj;
        // ensure that component ID's are the same (including null)
        if (thatRevision.getEntityId() == null && getEntityId() != null) {
            return false;
        }
        if (thatRevision.getEntityId() != null && getEntityId() == null) {
            return false;
        }
        if (thatRevision.getEntityId() != null && !thatRevision.getEntityId().equals(getEntityId())) {
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
        hash = 59 * hash + (this.entityId != null ? this.entityId.hashCode() : 0);
        hash = 59 * hash + (this.version != null ? this.version.hashCode() : 0);
        hash = 59 * hash + (this.clientId != null ? this.clientId.hashCode() : 0);
        return hash;
    }

    @Override
    public String toString() {
        return "[" + version + ", " + clientId + ", " + entityId + ']';
    }

}
