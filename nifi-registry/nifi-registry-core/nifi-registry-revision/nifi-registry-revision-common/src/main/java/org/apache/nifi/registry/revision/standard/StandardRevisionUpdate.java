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
package org.apache.nifi.registry.revision.standard;

import org.apache.nifi.registry.revision.api.EntityModification;
import org.apache.nifi.registry.revision.api.Revision;
import org.apache.nifi.registry.revision.api.RevisionUpdate;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class StandardRevisionUpdate<T> implements RevisionUpdate<T> {
    private final T entity;
    private final EntityModification lastModification;
    private final Set<Revision> updatedRevisions;

    public StandardRevisionUpdate(final T entity, final EntityModification lastModification) {
        this(entity, lastModification, null);
    }

    public StandardRevisionUpdate(final T entity, final EntityModification lastModification, final Set<Revision> updatedRevisions) {
        this.entity = entity;
        this.lastModification = lastModification;
        this.updatedRevisions = updatedRevisions == null ? new HashSet<>() : new HashSet<>(updatedRevisions);
        if (lastModification != null) {
            this.updatedRevisions.add(lastModification.getRevision());
        }
    }


    @Override
    public T getEntity() {
        return entity;
    }

    @Override
    public EntityModification getLastModification() {
        return lastModification;
    }

    @Override
    public Set<Revision> getUpdatedRevisions() {
        return Collections.unmodifiableSet(updatedRevisions);
    }

    @Override
    public String toString() {
        return "[Entity=" + entity + ", Last Modification=" + lastModification + "]";
    }

}
