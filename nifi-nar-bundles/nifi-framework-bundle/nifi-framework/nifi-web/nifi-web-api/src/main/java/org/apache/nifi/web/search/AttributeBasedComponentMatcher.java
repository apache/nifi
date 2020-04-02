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
import org.apache.nifi.web.search.attributematchers.AttributeMatcher;
import org.apache.nifi.web.search.query.SearchQuery;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

public class AttributeBasedComponentMatcher<T> implements ComponentMatcher<T> {
    private final List<AttributeMatcher<T>> attributeMatchers = new ArrayList<>();
    private final Function<T, String> getComponentIdentifier;
    private final Function<T, String> getComponentName;

    public AttributeBasedComponentMatcher(
               final List<AttributeMatcher<T>> attributeMatchers,
               final Function<T, String> getComponentIdentifier,
               final Function<T, String> getComponentName) {
        this.getComponentIdentifier = getComponentIdentifier;
        this.getComponentName = getComponentName;
        this.attributeMatchers.addAll(attributeMatchers);
    }

    @Override
    public final Optional<ComponentSearchResultDTO> match(final T component, final SearchQuery query) {
        final List<String> matches = new LinkedList<>();
        attributeMatchers.forEach(matcher -> matcher.match(component, query, matches));

        return matches.isEmpty()
                ? Optional.empty()
                : Optional.of(generateResult(component, matches));
    }

    private ComponentSearchResultDTO generateResult(final T component, final List<String> matches) {
        final ComponentSearchResultDTO result = new ComponentSearchResultDTO();
        result.setId(getComponentIdentifier.apply(component));
        result.setName(getComponentName.apply(component));
        result.setMatches(matches);
        return result;
    }
}
