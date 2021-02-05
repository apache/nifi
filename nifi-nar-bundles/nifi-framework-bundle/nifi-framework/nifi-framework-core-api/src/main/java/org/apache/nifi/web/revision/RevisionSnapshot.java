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

package org.apache.nifi.web.revision;

import org.apache.nifi.web.Revision;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;

public class RevisionSnapshot {
    private final Collection<Revision> revisions;
    private final long revisionUpdateCount;

    public RevisionSnapshot(final Collection<Revision> revisions, final long revisionUpdateCount) {
        this.revisions = new ArrayList<>(revisions);
        this.revisionUpdateCount = revisionUpdateCount;
    }

    public Collection<Revision> getRevisions() {
        return revisions;
    }

    public long getRevisionUpdateCount() {
        return revisionUpdateCount;
    }

    @Override
    public String toString() {
        return "RevisionSnapshot[" +
            "revisionUpdateCount=" + revisionUpdateCount +
            ", revisions=" + revisions +
            "]";
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final RevisionSnapshot that = (RevisionSnapshot) o;
        return revisionUpdateCount == that.revisionUpdateCount && revisions.equals(that.revisions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(revisions, revisionUpdateCount);
    }
}
