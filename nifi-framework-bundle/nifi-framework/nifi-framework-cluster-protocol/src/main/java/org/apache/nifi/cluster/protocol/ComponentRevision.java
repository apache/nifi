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

package org.apache.nifi.cluster.protocol;

import org.apache.nifi.web.Revision;

public class ComponentRevision {
    private Long version;
    private String clientId;
    private String componentId;

    public Long getVersion() {
        return version;
    }

    public void setVersion(Long version) {
        this.version = version;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getComponentId() {
        return componentId;
    }

    public void setComponentId(String componentId) {
        this.componentId = componentId;
    }

    public Revision toRevision() {
        return new Revision(getVersion(), getClientId(), getComponentId());
    }

    public static ComponentRevision fromRevision(final Revision revision) {
        final ComponentRevision componentRevision = new ComponentRevision();
        componentRevision.setVersion(revision.getVersion());
        componentRevision.setClientId(revision.getClientId());
        componentRevision.setComponentId(revision.getComponentId());
        return componentRevision;
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }

        if ((obj instanceof ComponentRevision) == false) {
            return false;
        }

        ComponentRevision thatRevision = (ComponentRevision) obj;
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
}
