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
package org.apache.nifi.web.search;

import org.apache.nifi.web.api.dto.search.ComponentSearchResultDTO;
import org.apache.nifi.web.search.query.SearchQuery;

import java.util.Optional;

/**
 * Service responsible to clamp all the possible matches for a given component type.
 *
 * @param <COMPONENT_TYPE> The component type.
 */
public interface ComponentMatcher<COMPONENT_TYPE> {

    /**
     * Tries to match the incoming search query against a given component.
     *
     * @param component The component to match against.
     * @param query The search query to match.
     *
     * @return The result of the matching. Returns with {@link Optional#empty()} if there was no match, contains {@link ComponentSearchResultDTO}
     * with the details of the results in case there was at least one match for the given component and query.
     */
    Optional<ComponentSearchResultDTO> match(COMPONENT_TYPE component, SearchQuery query);
}
