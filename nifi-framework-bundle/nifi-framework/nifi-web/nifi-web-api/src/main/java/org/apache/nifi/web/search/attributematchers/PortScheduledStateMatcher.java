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
package org.apache.nifi.web.search.attributematchers;

import org.apache.commons.lang3.Strings;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.web.search.query.SearchQuery;

import java.util.List;

public class PortScheduledStateMatcher implements AttributeMatcher<Port> {
    private static final String SEARCH_TERM_DISABLED = "disabled";
    private static final String SEARCH_TERM_INVALID = "invalid";
    private static final String SEARCH_TERM_RUNNING = "running";
    private static final String SEARCH_TERM_STOPPED = "stopped";

    private static final String MATCH_PREFIX = "Run status: ";
    private static final String MATCH_DISABLED = "Disabled";
    private static final String MATCH_INVALID = "Invalid";
    private static final String MATCH_RUNNING = "Running";
    private static final String MATCH_STOPPED = "Stopped";

    @Override
    public void match(final Port component, final SearchQuery query, final List<String> matches) {
        final String searchTerm = query.getTerm();

        if (ScheduledState.DISABLED.equals(component.getScheduledState())) {
            if (Strings.CI.contains(SEARCH_TERM_DISABLED, searchTerm)) {
                matches.add(MATCH_PREFIX + MATCH_DISABLED);
            }
        } else if (Strings.CI.contains(SEARCH_TERM_INVALID, searchTerm) && !component.isValid()) {
            matches.add(MATCH_PREFIX + MATCH_INVALID);
        } else if (ScheduledState.RUNNING.equals(component.getScheduledState()) && Strings.CI.contains(SEARCH_TERM_RUNNING, searchTerm)) {
            matches.add(MATCH_PREFIX + MATCH_RUNNING);
        } else if (ScheduledState.STOPPED.equals(component.getScheduledState()) && Strings.CI.contains(SEARCH_TERM_STOPPED, searchTerm)) {
            matches.add(MATCH_PREFIX + MATCH_STOPPED);
        }
    }
}
