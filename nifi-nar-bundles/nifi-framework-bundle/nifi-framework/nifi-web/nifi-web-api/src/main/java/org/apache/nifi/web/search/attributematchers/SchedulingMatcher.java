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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.web.search.query.SearchQuery;

import java.util.List;

import static org.apache.nifi.scheduling.SchedulingStrategy.EVENT_DRIVEN;
import static org.apache.nifi.scheduling.SchedulingStrategy.PRIMARY_NODE_ONLY;
import static org.apache.nifi.scheduling.SchedulingStrategy.TIMER_DRIVEN;

public class SchedulingMatcher implements AttributeMatcher<ProcessorNode> {
    private static final String SEARCH_TERM_EVENT = "event";
    private static final String SEARCH_TERM_TIMER = "timer";
    private static final String SEARCH_TERM_PRIMARY = "primary";

    private static final String MATCH_PREFIX = "Scheduling strategy: ";
    private static final String MATCH_EVENT = "Event driven";
    private static final String MATCH_TIMER = "Timer driven";
    private static final String MATCH_PRIMARY = "On primary node";

    @Override
    public void match(final ProcessorNode component, final SearchQuery query, final List<String> matches) {
        final String searchTerm = query.getTerm();
        final SchedulingStrategy schedulingStrategy = component.getSchedulingStrategy();

        if (EVENT_DRIVEN.equals(schedulingStrategy) && StringUtils.containsIgnoreCase(SEARCH_TERM_EVENT, searchTerm)) {
            matches.add(MATCH_PREFIX + MATCH_EVENT);
        } else if (TIMER_DRIVEN.equals(schedulingStrategy) && StringUtils.containsIgnoreCase(SEARCH_TERM_TIMER, searchTerm)) {
            matches.add(MATCH_PREFIX + MATCH_TIMER);
        } else if (PRIMARY_NODE_ONLY.equals(schedulingStrategy) && StringUtils.containsIgnoreCase(SEARCH_TERM_PRIMARY, searchTerm)) {
            // PRIMARY_NODE_ONLY has been deprecated as a SchedulingStrategy and replaced by PRIMARY as an ExecutionNode.
            matches.add(MATCH_PREFIX + MATCH_PRIMARY);
        }
    }
}
