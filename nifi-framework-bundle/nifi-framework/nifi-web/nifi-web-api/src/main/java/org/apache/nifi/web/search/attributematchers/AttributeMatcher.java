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
import org.apache.nifi.web.search.query.SearchQuery;

import java.util.List;

/**
 * Represents an elementary match based on a given attribute, like name or description, depending on the implementation.
 *
 * @param <T> The component type.
 */
public interface AttributeMatcher<T> {
    String SEPARATOR = ": ";

    /**
     * Executing the match.
     *
     * @param component The component to match against.
     * @param query The search query to match.
     * @param matches Aggregator for the match results.
     */
    void match(T component, SearchQuery query, List<String> matches);

    /**
     * Helper method for implementations to execute simple text based matches.
     *
     * @param searchTerm The search term to match.
     * @param subject The component's textual attribute to match against.
     * @param label The descriptor of the match's nature.
     * @param matches Aggregator for the match results.
     */
    static void addIfMatching(final String searchTerm, final String subject, final String label, final List<String> matches) {
        final String match = label + SEPARATOR + subject;

        if (StringUtils.containsIgnoreCase(subject, searchTerm) && matches != null && !matches.contains(match)) {
            matches.add(match);
        }
    }
}
