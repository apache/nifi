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
import org.apache.nifi.web.revision.RevisionSnapshot;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class ComponentRevisionSnapshot {
    private List<ComponentRevision> componentRevisions;
    private Long revisionUpdateCount;

    public List<ComponentRevision> getComponentRevisions() {
        return componentRevisions;
    }

    public void setComponentRevisions(final List<ComponentRevision> componentRevisions) {
        this.componentRevisions = componentRevisions;
    }

    public Long getRevisionUpdateCount() {
        return revisionUpdateCount;
    }

    public void setRevisionUpdateCount(final Long revisionUpdateCount) {
        this.revisionUpdateCount = revisionUpdateCount;
    }

    public static ComponentRevisionSnapshot fromRevisionSnapshot(final RevisionSnapshot revisionSnapshot) {
        final List<ComponentRevision> componentRevisions = revisionSnapshot.getRevisions().stream()
            .map(ComponentRevision::fromRevision)
            .collect(Collectors.toList());

        final ComponentRevisionSnapshot componentRevisionSnapshot = new ComponentRevisionSnapshot();
        componentRevisionSnapshot.setComponentRevisions(componentRevisions);
        componentRevisionSnapshot.setRevisionUpdateCount(revisionSnapshot.getRevisionUpdateCount());
        return componentRevisionSnapshot;
    }

    public RevisionSnapshot toRevisionSnapshot() {
        final List<Revision> revisions = componentRevisions == null ? Collections.emptyList() : componentRevisions.stream()
            .map(ComponentRevision::toRevision)
            .collect(Collectors.toList());
        final long updateCount = revisionUpdateCount == null ? 0L : revisionUpdateCount;

        return new RevisionSnapshot(revisions, updateCount);
    }
}
