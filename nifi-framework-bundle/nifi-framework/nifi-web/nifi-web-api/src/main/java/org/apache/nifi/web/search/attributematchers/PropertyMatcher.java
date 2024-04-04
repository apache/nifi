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
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ComponentNode;
import org.apache.nifi.web.search.query.SearchQuery;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.nifi.web.search.attributematchers.AttributeMatcher.addIfMatching;

public class PropertyMatcher implements AttributeMatcher<ComponentNode> {
    private static final String LABEL_NAME = "Property name";
    private static final String LABEL_VALUE = "Property value";
    private static final String LABEL_DESCRIPTION = "Property description";

    private final static String FILTER_NAME_PROPERTIES = "properties";
    private final static Set<String> FILTER_VALUES_PROPERTIES_EXCLUSION = new HashSet<>(Arrays.asList("no", "none", "false", "exclude", "0"));

    @Override
    public void match(final ComponentNode component, final SearchQuery query, final List<String> matches) {
        final String searchTerm = query.getTerm();

        if (!propertiesAreFilteredOut(query)) {
            for (final Map.Entry<PropertyDescriptor, String> entry : component.getRawPropertyValues().entrySet()) {
                final PropertyDescriptor descriptor = entry.getKey();
                addIfMatching(searchTerm, descriptor.getName(), LABEL_NAME, matches);
                addIfMatching(searchTerm, descriptor.getDescription(), LABEL_DESCRIPTION, matches);

                // never include sensitive properties values in search results
                if (!descriptor.isSensitive()) {
                    final String value = Optional.ofNullable(entry.getValue()).orElse(descriptor.getDefaultValue());

                    // evaluate if the value matches the search criteria
                    if (StringUtils.containsIgnoreCase(value, searchTerm)) {
                        matches.add(LABEL_VALUE + SEPARATOR + descriptor.getName() + " - " + value);
                    }
                }
            }
        }
    }

    private boolean propertiesAreFilteredOut(final SearchQuery query) {
        return query.hasFilter(FILTER_NAME_PROPERTIES) && FILTER_VALUES_PROPERTIES_EXCLUSION.contains(query.getFilter(FILTER_NAME_PROPERTIES));
    }
}
