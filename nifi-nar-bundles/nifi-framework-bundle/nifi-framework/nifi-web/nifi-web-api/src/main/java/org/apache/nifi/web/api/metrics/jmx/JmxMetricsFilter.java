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
package org.apache.nifi.web.api.metrics.jmx;

import org.apache.nifi.web.api.dto.JmxMetricsResultDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

public class JmxMetricsFilter {
    private static final Logger LOGGER = LoggerFactory.getLogger(JmxMetricsFilter.class);
    private static final String MATCH_NOTHING = "~^";
    private static final String NAME_SEPARATOR = "\\|";
    private static final String REPLACE_CHARACTERS = "[\\^$.*()]";
    private static final String EMPTY = "";
    private final Pattern allowedNameFilter;
    private final Set<String> beanNameFilters;

    public JmxMetricsFilter(final String allowedNameFilter, final String beanNameFilter) {
        this.allowedNameFilter = createPattern(allowedNameFilter);
        this.beanNameFilters = createBeanNameFilters(beanNameFilter);
    }

    private Pattern createPattern(final String filter) {
        try {
            if (filter == null || filter.isEmpty()) {
                return Pattern.compile(MATCH_NOTHING);
            } else {
                return Pattern.compile(filter);
            }
        } catch (PatternSyntaxException e) {
            LOGGER.warn("Invalid JMX MBean filter pattern ignored [{}]", filter);
            return Pattern.compile(MATCH_NOTHING);
        }
    }

    private Set<String> createBeanNameFilters(final String filter) {
        if (filter == null || filter.isEmpty()) {
            return Collections.emptySet();
        } else {
            return Arrays.stream(
                    filter.split(NAME_SEPARATOR)).map(
                            name -> name.replaceAll(REPLACE_CHARACTERS, EMPTY)
                    )
                    .filter(Predicate.not(String::isBlank))
                    .collect(Collectors.toSet());
        }
    }

    public Collection<JmxMetricsResultDTO> filter(final Collection<JmxMetricsResultDTO> results) {
        return results.stream()
                .filter(result -> allowedNameFilter.asPredicate().test(result.getBeanName()))
                .filter(result -> {
                    final boolean included;
                    if (beanNameFilters.isEmpty()) {
                        included = true;
                    } else {
                        final String beanName = result.getBeanName();
                        included = beanNameFilters.stream().anyMatch(beanName::contains);
                    }
                    return included;
                })
                .collect(Collectors.toList());
    }
}
