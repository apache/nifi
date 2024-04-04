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

package org.apache.nifi.provenance.authorization;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.nifi.authorization.AccessDeniedException;
import org.apache.nifi.provenance.PlaceholderProvenanceEvent;
import org.apache.nifi.provenance.ProvenanceEventRecord;

public interface EventAuthorizer {

    /**
     * Determines whether or not the has access to the given Provenance Event.
     * This method does not imply the user is directly attempting to access the specified resource. If the user is
     * attempting a direct access use Authorizable.authorize().
     *
     * @param event the event to authorize
     * @return is authorized
     */
    boolean isAuthorized(ProvenanceEventRecord event);

    /**
     * Authorizes the current user for the specified action on the specified resource. This method does
     * imply the user is directly accessing the specified resource.
     *
     * @param event the event to authorize
     * @throws AccessDeniedException if the user is not authorized
     */
    void authorize(ProvenanceEventRecord event) throws AccessDeniedException;

    /**
     * Filters out any events that the user is not authorized to access
     *
     * @param events the events to filtered
     * @return a List that contains only events from the original, for which the user has access
     */
    default List<ProvenanceEventRecord> filterUnauthorizedEvents(List<ProvenanceEventRecord> events) {
        return events.stream()
            .filter(event -> isAuthorized(event))
            .collect(Collectors.toList());
    }

    /**
     * Returns a Set of provenance events for which any of the given events that the user does not
     * have access to has been replaced by a placeholder event
     *
     * @param events the events to filter
     * @return a Set containing only provenance events that the user has access to
     */
    default Set<ProvenanceEventRecord> replaceUnauthorizedWithPlaceholders(Set<ProvenanceEventRecord> events) {
        return events.stream()
            .map(event -> isAuthorized(event) ? event : new PlaceholderProvenanceEvent(event))
            .collect(Collectors.toSet());
    }

    public static final EventAuthorizer GRANT_ALL = new EventAuthorizer() {
        @Override
        public boolean isAuthorized(ProvenanceEventRecord event) {
            return true;
        }

        @Override
        public void authorize(ProvenanceEventRecord event) throws AccessDeniedException {
        }

        @Override
        public List<ProvenanceEventRecord> filterUnauthorizedEvents(List<ProvenanceEventRecord> events) {
            return events;
        }

        @Override
        public Set<ProvenanceEventRecord> replaceUnauthorizedWithPlaceholders(Set<ProvenanceEventRecord> events) {
            return events;
        }
    };

    public static final EventAuthorizer DENY_ALL = new EventAuthorizer() {
        @Override
        public boolean isAuthorized(ProvenanceEventRecord event) {
            return false;
        }

        @Override
        public void authorize(ProvenanceEventRecord event) throws AccessDeniedException {
            throw new AccessDeniedException();
        }

        @Override
        public List<ProvenanceEventRecord> filterUnauthorizedEvents(List<ProvenanceEventRecord> events) {
            return Collections.emptyList();
        }

        @Override
        public Set<ProvenanceEventRecord> replaceUnauthorizedWithPlaceholders(Set<ProvenanceEventRecord> events) {
            return events.stream()
                .map(event -> new PlaceholderProvenanceEvent(event))
                .collect(Collectors.toSet());
        }
    };
}
