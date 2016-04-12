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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.nifi.web.Revision;

/**
 * <p>
 * A task that is responsible for updating some component(s).
 * </p>
 */
public interface UpdateRevisionTask<T> {
    /**
     * Updates one or more components and returns updated Revisions for those components
     *
     * @return the updated revisions for the components
     */
    RevisionUpdate<T> update();

    /**
     * Returns a new Revision that has the same Client ID and Component ID as the given one
     * but with a larger version
     *
     * @param revision the revision to update
     * @return the updated Revision
     */
    default Revision incrementRevision(Revision revision) {
        return new Revision(revision.getVersion() + 1, revision.getClientId(), revision.getComponentId());
    }

    /**
     * Returns a Collection of Revisions that contains an updated version of all Revisions passed in
     *
     * @param revisions the Revisions to update
     * @return a Collection of all Revisions that are passed in
     */
    default Collection<Revision> incrementRevisions(Revision... revisions) {
        final List<Revision> updated = new ArrayList<>(revisions.length);
        for (final Revision revision : revisions) {
            updated.add(incrementRevision(revision));
        }

        return updated;
    }
}
