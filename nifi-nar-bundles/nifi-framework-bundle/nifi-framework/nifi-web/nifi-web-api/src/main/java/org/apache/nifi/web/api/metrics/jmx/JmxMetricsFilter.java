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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

public class JmxMetricsFilter {
    private static final Logger LOGGER = LoggerFactory.getLogger(JmxMetricsFilter.class);
    private final static String MATCH_NOTHING = "~^";
    private final static String MATCH_ALL = "";
    private final static String SEPARATOR = ";\\s?";
    private final static String REPLACEMENT = "|";
    private final Pattern blackListingFilter;
    private final Pattern beanNameFilter;

    public JmxMetricsFilter(final String blackListingFilter, final String beanNameFilter) {
        this.blackListingFilter = createPattern(blackListingFilter, MATCH_NOTHING);
        this.beanNameFilter = createPattern(beanNameFilter, MATCH_ALL);
    }

    private Pattern createPattern(final String filters, final String defaultValue) {
        try {
            if (filters == null || filters.isEmpty()) {
                return Pattern.compile(defaultValue);
            } else {
                return Pattern.compile(filters.replaceAll(SEPARATOR, REPLACEMENT));
            }
        } catch (PatternSyntaxException e) {
            LOGGER.warn("Invalid filter {} , will use default filtering.", filters);
            return Pattern.compile(defaultValue);
        }
    }

    public Collection<JmxMetricsResult> filter(final Collection<JmxMetricsResult> results) {
        return results.stream()
                .filter(result -> blackListingFilter.asPredicate().negate().test(result.getBeanName()))
                .filter(result -> beanNameFilter.asPredicate().test(result.getBeanName()))
                .collect(Collectors.toList());
    }
}
