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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JmxMetricsFilterTest {
    private static final String ALLOW_ALL_PATTERN = ".*";
    private static final String EMPTY_STRING_PATTERN = "";
    private static final String BEAN_NAME_FILTER = "%s|%s";
    private static final String INVALID_REGEX = "(";
    private static final String TEST_BEAN_NAME_ONE = "testBean1";
    private static final String TEST_BEAN_NAME_TWO = "testBean2";
    private static final JmxMetricsResultDTO RESULT_ONE = new JmxMetricsResultDTO(TEST_BEAN_NAME_ONE, null, null);
    private static final JmxMetricsResultDTO RESULT_TWO = new JmxMetricsResultDTO(TEST_BEAN_NAME_TWO, null, null);
    private static List<JmxMetricsResultDTO> results;

    @BeforeAll
    public static void init() {
        results = new ArrayList<>();
        results.add(RESULT_ONE);
        results.add(RESULT_TWO);
    }

    @Test
    public void testNotProvidingFiltersReturnsAllMBeans() {
        final JmxMetricsFilter metricsFilter = new JmxMetricsFilter(ALLOW_ALL_PATTERN, EMPTY_STRING_PATTERN);

        final Collection<JmxMetricsResultDTO> actual = metricsFilter.filter(results);

        assertEquals(actual.size(), 2);
        assertTrue(actual.containsAll(results));
    }

    @Test
    public void testAllowedNameFiltersRemovesMBeanFromResult() {
        final JmxMetricsFilter metricsFilter = new JmxMetricsFilter(TEST_BEAN_NAME_ONE, EMPTY_STRING_PATTERN);

        final Collection<JmxMetricsResultDTO> actual = metricsFilter.filter(results);

        assertEquals(actual.size(), 1);
        assertTrue(actual.contains(RESULT_ONE));
        assertFalse(actual.contains(RESULT_TWO));
    }

    @Test
    public void testBeanNameFiltersReturnsTheSpecifiedMBeansOnly() {
        final JmxMetricsFilter metricsFilter = new JmxMetricsFilter(ALLOW_ALL_PATTERN, String.format(BEAN_NAME_FILTER, TEST_BEAN_NAME_ONE, TEST_BEAN_NAME_TWO));

        final Collection<JmxMetricsResultDTO> actual = metricsFilter.filter(results);

        assertEquals(actual.size(), 2);
        assertTrue(actual.containsAll(results));
    }

    @Test
    public void testInvalidAllowedNameFilterRevertingBackToDefaultFiltering() {
        final JmxMetricsFilter metricsFilter = new JmxMetricsFilter(INVALID_REGEX, EMPTY_STRING_PATTERN);

        final Collection<JmxMetricsResultDTO> actual = metricsFilter.filter(results);

        assertTrue(actual.isEmpty());
    }

    @Test
    public void testInvalidBeanNameFilteringRevertingBackToDefaultFiltering() {
        final JmxMetricsFilter metricsFilter = new JmxMetricsFilter(ALLOW_ALL_PATTERN, INVALID_REGEX);

        final Collection<JmxMetricsResultDTO> actual = metricsFilter.filter(results);

        assertEquals(actual.size(), 2);
        assertTrue(actual.containsAll(results));
    }

    @Test
    public void testInvalidFiltersRevertingBackToDefaultFiltering() {
        final JmxMetricsFilter metricsFilter = new JmxMetricsFilter(INVALID_REGEX, INVALID_REGEX);

        final Collection<JmxMetricsResultDTO> actual = metricsFilter.filter(results);

        assertTrue(actual.isEmpty());
    }

    @Test
    public void testAllowedNameFilterHasPriority() {
        final JmxMetricsFilter metricsFilter = new JmxMetricsFilter(TEST_BEAN_NAME_TWO, TEST_BEAN_NAME_ONE);

        final Collection<JmxMetricsResultDTO> actual = metricsFilter.filter(results);

        assertTrue(actual.isEmpty());
    }

    @Test
    public void testAllowedNameFilterHasPriorityWhenTheSameFiltersApplied() {
        final JmxMetricsFilter metricsFilter = new JmxMetricsFilter(TEST_BEAN_NAME_TWO, String.format(BEAN_NAME_FILTER, TEST_BEAN_NAME_ONE, TEST_BEAN_NAME_TWO));

        final Collection<JmxMetricsResultDTO> actual = metricsFilter.filter(results);

        assertEquals(actual.size(), 1);
        assertFalse(actual.contains(RESULT_ONE));
        assertTrue(actual.contains(RESULT_TWO));
    }
}