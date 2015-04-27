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
 * A model object representing a revision. Equality is defined as either a
 * matching version number or matching non-empty client IDs.
 *
 * @author unattributed
 * @Immutable
 * @Threadsafe
 */
public class Revision implements Serializable {

    /**
     * the version number
     */
    private final Long version;

    /**
     * the client ID
     */
    private final String clientId;

    public Revision(Long revision, String clientId) {
        this.version = revision;
        this.clientId = clientId;
    }

    public String getClientId() {
        return clientId;
    }

    public Long getVersion() {
        return version;
    }

    @Override
    public boolean equals(final Object obj) {

        if ((obj instanceof Revision) == false) {
            return false;
        }

        Revision thatRevision = (Revision) obj;
        if (this.version != null && this.version.equals(thatRevision.version)) {
            return true;
        } else {
            return clientId != null && !clientId.trim().isEmpty() && clientId.equals(thatRevision.getClientId());
        }

    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 59 * hash + (this.version != null ? this.version.hashCode() : 0);
        hash = 59 * hash + (this.clientId != null ? this.clientId.hashCode() : 0);
        return hash;
    }

    @Override
    public String toString() {
        return "[" + version + ", " + clientId + ']';
    }
}
