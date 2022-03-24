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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.nifi.web.FlowModification;
import org.apache.nifi.web.Revision;

public class StandardRevisionUpdate<T> implements RevisionUpdate<T> {
    private final T component;
    private final FlowModification lastModification;
    private final Set<Revision> updatedRevisions;

    public StandardRevisionUpdate(final T component, final FlowModification lastModification) {
        this(component, lastModification, null);
    }

    public StandardRevisionUpdate(final T component, final FlowModification lastModification, final Set<Revision> updatedRevisions) {
        this.component = component;
        this.lastModification = lastModification;
        this.updatedRevisions = updatedRevisions == null ? new HashSet<>() : new HashSet<>(updatedRevisions);
        if (lastModification != null) {
            this.updatedRevisions.add(lastModification.getRevision());
        }
    }


    @Override
    public T getComponent() {
        return component;
    }

    @Override
    public FlowModification getLastModification() {
        return lastModification;
    }

    @Override
    public Set<Revision> getUpdatedRevisions() {
        return Collections.unmodifiableSet(updatedRevisions);
    }

    @Override
    public String toString() {
        return "[Component=" + component + ", Last Modification=" + lastModification + "]";
    }

}
