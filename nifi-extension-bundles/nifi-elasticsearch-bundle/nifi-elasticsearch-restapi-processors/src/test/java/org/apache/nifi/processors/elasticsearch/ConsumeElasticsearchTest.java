/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License") you may not use this file except in compliance with
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
package org.apache.nifi.processors.elasticsearch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.elasticsearch.SearchResponse;
import org.apache.nifi.processors.elasticsearch.api.PaginatedJsonQueryParameters;
import org.apache.nifi.util.StringUtils;
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ConsumeElasticsearchTest extends SearchElasticsearchTest {
    private static final String DEFAULT_QUERY_SORT_CLAUSE_ONLY = String.format("{\"sort\":[%s]}", CONSUME_ELASTICSEARCH_SORT_CLAUSE);
    private static final String DEFAULT_QUERY_FILTERS_ONLY = "{\"query\":{\"bool\":{\"filter\":[]}}}";
    private static final String DEFAULT_RANGE_FILTER = String.format("{\"range\":{\"%s\":{\"gt\":\"%s\"}}}", RANGE_FIELD_NAME, RANGE_FIELD_VALUE);

    private static final List<Map<String, Object>> FIRST_PAGE_OF_HITS = Arrays.asList(
            createHit(0),
            createHit(1),
            createHit(2),
            createHit(3),
            createHit(4)
    );

    private static final List<Map<String, Object>> SECOND_PAGE_OF_HITS = Arrays.asList(
            createHit(5),
            createHit(6),
            createHit(7),
            createHit(8),
            createHit(9)
    );
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static Map<String, Object> createHit(final int value) {
        return Collections.singletonMap("_source", Collections.singletonMap(RANGE_FIELD_NAME, value));
    }

    @BeforeAll
    public static void setUpBeforeClass() throws Exception {
        AbstractPaginatedJsonQueryElasticsearchTest.setUpBeforeClass();
    }

    @Override
    AbstractPaginatedJsonQueryElasticsearch getProcessor() {
        return new ConsumeElasticsearch();
    }

    @Override
    Scope getStateScope() {
        return Scope.CLUSTER;
    }

    private TestRunner runner;

    @BeforeEach
    void setUp() {
        runner = createRunner(false);
    }

    @Override
    TestRunner createRunner(final boolean returnAggs) {
        final TestRunner runner = super.createRunner(returnAggs);
        runner.setValidateExpressionUsage(false);

        // onScheduled method isn't always triggered (because the processor isn't always executed through the TestRunner)
        // so set the trackingRange fields directly as well as in the ProcessContext
        runner.setProperty(ConsumeElasticsearch.RANGE_FIELD, RANGE_FIELD_NAME);
        ((ConsumeElasticsearch) runner.getProcessor()).trackingRangeField = RANGE_FIELD_NAME;
        setTrackingSortOrder(runner, RANGE_SORT_ORDER);

        return runner;
    }

    void setTrackingSortOrder(final TestRunner runner, final String sortOrder) {
        runner.setProperty(ConsumeElasticsearch.RANGE_FIELD_SORT_ORDER, sortOrder);
        ((ConsumeElasticsearch) runner.getProcessor()).trackingSortOrder = sortOrder;
    }

    @ParameterizedTest
    @CsvSource({",", "123456,", ",654321", "123456,654321"})
    void testRangeValue(final String initialValue, final String stateValue) throws IOException {
        if (StringUtils.isNotBlank(initialValue)) {
            runner.setProperty(ConsumeElasticsearch.RANGE_INITIAL_VALUE, initialValue);
        }

        if (StringUtils.isNotBlank(stateValue)) {
            runner.getStateManager().setState(Collections.singletonMap(ConsumeElasticsearch.STATE_RANGE_VALUE, stateValue), getStateScope());
        }

        runOnce(runner);

        final String query = getService(runner).getQuery();
        if (StringUtils.isBlank(initialValue) && StringUtils.isBlank(stateValue)) {
            // no "query" if no initial/existing state value, but "sort" is always present
            assertEquals(DEFAULT_QUERY_SORT_CLAUSE_ONLY, query);
        } else {
            // existing state value should override any initial value from processor properties
            final String rangeValue = JsonPath.read(query, String.format("$.query.bool.filter[0].range.%s.gt", RANGE_FIELD_NAME));
            assertEquals(StringUtils.isNotBlank(stateValue) ? stateValue : initialValue, rangeValue);
        }
    }

    @ParameterizedTest
    @CsvSource({"asc,0,true", "asc,0,false", "asc,1,true", "asc,1,false",
            "desc,0,true", "desc,0,false", "desc,1,true", "desc,1,false"})
    void testTrackingValueFromHits(final String sortOrder, final int pageCount, final boolean hasHits) {
        setTrackingSortOrder(runner, sortOrder);

        final List<Map<String, Object>> hits;
        if (!hasHits) {
            hits = Collections.emptyList();
        } else {
            hits = pageCount == 0 ? FIRST_PAGE_OF_HITS : SECOND_PAGE_OF_HITS;
        }
        final SearchResponse response = new SearchResponse(hits, null, null, null, null, hits.size(), 1, false, null);

        final String defaultUnset = "unset";
        final PaginatedJsonQueryParameters paginatedJsonQueryParameters = new PaginatedJsonQueryParameters();
        paginatedJsonQueryParameters.setKeepAlive("10S");
        paginatedJsonQueryParameters.setPageCount(pageCount);
        paginatedJsonQueryParameters.setTrackingRangeValue(defaultUnset);

        ((ConsumeElasticsearch) runner.getProcessor()).updateQueryParameters(paginatedJsonQueryParameters, response);

        if ("asc".equals(sortOrder)) {
            if (hasHits) {
                final List<Map<String, Object>> expectedHits = pageCount == 0 ? FIRST_PAGE_OF_HITS : SECOND_PAGE_OF_HITS;
                assertEquals(getHitValue(expectedHits.getLast()), paginatedJsonQueryParameters.getTrackingRangeValue());
            } else {
                assertEquals(defaultUnset, paginatedJsonQueryParameters.getTrackingRangeValue());
            }
        } else {
            if (pageCount == 0) {
                if (hasHits) {
                    assertEquals(getHitValue(FIRST_PAGE_OF_HITS.getFirst()), paginatedJsonQueryParameters.getTrackingRangeValue());
                } else {
                    assertEquals(defaultUnset, paginatedJsonQueryParameters.getTrackingRangeValue());
                }
            } else {
                assertEquals(defaultUnset, paginatedJsonQueryParameters.getTrackingRangeValue());
            }
        }
    }

    @SuppressWarnings("unchecked")
    private String getHitValue(final Map<String, Object> hit) {
        return String.valueOf(((Map<String, Object>) hit.get("_source")).get(RANGE_FIELD_NAME));
    }

    @Test
    void testNoSorts() throws IOException {
        final TestRunner runner = createRunner(false);
        runner.removeProperty(ConsumeElasticsearch.SORT);

        final Map<String, Object> query = new HashMap<>();
        ((ConsumeElasticsearch) runner.getProcessor()).addSortClause(query, null, runner.getProcessContext(), OBJECT_MAPPER);

        assertEquals(getDefaultQuerySortOnly(), query);
    }

    @Test
    void testSingleAdditionalSort() throws IOException {
        final TestRunner runner = createRunner(false);
        runner.setProperty(ConsumeElasticsearch.SORT, "{\"foo\":\"bar\"}");

        final Map<String, Object> query = new HashMap<>();
        ((ConsumeElasticsearch) runner.getProcessor()).addSortClause(query, null, runner.getProcessContext(), OBJECT_MAPPER);

        final Map<String, List<Map<String, Object>>> expected = getDefaultQuerySortOnly();
        addExpectedSortClause(expected, Collections.singletonMap("foo", "bar"));

        assertEquals(expected, query);
    }

    @Test
    void testMultipleAdditionalSorts() throws IOException {
        final TestRunner runner = createRunner(false);
        runner.setProperty(ConsumeElasticsearch.SORT, "[{\"foo\":\"bar\"},{\"baz\":\"biz\"}]");

        final Map<String, Object> query = new HashMap<>();
        ((ConsumeElasticsearch) runner.getProcessor()).addSortClause(query, null, runner.getProcessContext(), OBJECT_MAPPER);

        final Map<String, List<Map<String, Object>>> expected = getDefaultQuerySortOnly();
        addExpectedSortClause(expected, Collections.singletonMap("foo", "bar"));
        addExpectedSortClause(expected, Collections.singletonMap("baz", "biz"));

        assertEquals(expected, query);
    }

    @Test
    void testTrackingFieldSortAlreadyPresent() throws IOException {
        final TestRunner runner = createRunner(false);
        final String existingRangeFieldSort = String.format("{\"%s\":\"bar\"}", RANGE_FIELD_NAME);
        runner.setProperty(ConsumeElasticsearch.SORT, existingRangeFieldSort);

        final Map<String, Object> query = new HashMap<>();
        ((ConsumeElasticsearch) runner.getProcessor()).addSortClause(query, null, runner.getProcessContext(), OBJECT_MAPPER);

        final Map<String, Object> expected = TEST_MAPPER.readValue(String.format("{\"sort\":[%s]}", existingRangeFieldSort), new TypeReference<>() { });

        assertEquals(expected, query);
    }

    private Map<String, List<Map<String, Object>>> getDefaultQuerySortOnly() throws JsonProcessingException {
        return TEST_MAPPER.readValue(DEFAULT_QUERY_SORT_CLAUSE_ONLY, new TypeReference<>() { });
    }

    private void addExpectedSortClause(final Map<String, List<Map<String, Object>>> expectedQuery, final Map<String, Object> expectedSortClause) {
        expectedQuery.get("sort").add(expectedSortClause);
    }

    @Test
    void testSingleAdditionalFilterNoInitialRangeValue() throws IOException {
        final TestRunner runner = createRunner(false);
        runner.removeProperty(ConsumeElasticsearch.RANGE_INITIAL_VALUE);
        runner.setProperty(ConsumeElasticsearch.ADDITIONAL_FILTERS, "{\"foo\":\"bar\"}");

        final Map<String, Object> query = new HashMap<>();
        ((ConsumeElasticsearch) runner.getProcessor()).addQueryClause(query, null, runner.getProcessContext(), OBJECT_MAPPER);

        final Map<String, Map<String, Map<String, List<Map<String, Object>>>>> expected = getDefaultQueryFiltersOnly();
        addExpectedFilterClause(expected, Collections.singletonMap("foo", "bar"));

        assertEquals(expected, query);
    }

    @Test
    void testSingleAdditionalFilterWithInitialRangeValue() throws IOException {
        final TestRunner runner = createRunner(false);
        runner.setProperty(ConsumeElasticsearch.RANGE_INITIAL_VALUE, RANGE_FIELD_VALUE);
        runner.setProperty(ConsumeElasticsearch.ADDITIONAL_FILTERS, "{\"foo\":\"bar\"}");

        final Map<String, Object> query = new HashMap<>();
        ((ConsumeElasticsearch) runner.getProcessor()).addQueryClause(query, null, runner.getProcessContext(), OBJECT_MAPPER);

        final Map<String, Map<String, Map<String, List<Map<String, Object>>>>> expected = getDefaultQueryFiltersOnly();
        addExpectedFilterClause(expected, getDefaultRangeFilterClause());
        addExpectedFilterClause(expected, Collections.singletonMap("foo", "bar"));

        assertEquals(expected, query);
    }

    @Test
    void testMultipleAdditionalFilters() throws IOException {
        final TestRunner runner = createRunner(false);
        runner.setProperty(ConsumeElasticsearch.RANGE_INITIAL_VALUE, RANGE_FIELD_VALUE);
        runner.setProperty(ConsumeElasticsearch.ADDITIONAL_FILTERS, "[{\"foo\":\"bar\"},{\"biz\":\"baz\"}]");

        final Map<String, Object> query = new HashMap<>();
        ((ConsumeElasticsearch) runner.getProcessor()).addQueryClause(query, null, runner.getProcessContext(), OBJECT_MAPPER);

        final Map<String, Map<String, Map<String, List<Map<String, Object>>>>> expected = getDefaultQueryFiltersOnly();
        addExpectedFilterClause(expected, getDefaultRangeFilterClause());
        addExpectedFilterClause(expected, Collections.singletonMap("foo", "bar"));
        addExpectedFilterClause(expected, Collections.singletonMap("biz", "baz"));

        assertEquals(expected, query);
    }

    @Test
    void testRangeDateFormat() throws IOException {
        final TestRunner runner = createRunner(false);
        runner.setProperty(ConsumeElasticsearch.RANGE_INITIAL_VALUE, RANGE_FIELD_VALUE);
        runner.setProperty(ConsumeElasticsearch.RANGE_DATE_FORMAT, "epoch-millis");

        final Map<String, Object> query = new HashMap<>();
        ((ConsumeElasticsearch) runner.getProcessor()).addQueryClause(query, null, runner.getProcessContext(), OBJECT_MAPPER);

        final Map<String, Object> rangeFilterClause = getDefaultRangeFilterClause();
        addExpectedRangeFilterClauseField(rangeFilterClause, "format", "epoch-millis");
        final Map<String, Map<String, Map<String, List<Map<String, Object>>>>> expected = getDefaultQueryFiltersOnly();
        addExpectedFilterClause(expected, rangeFilterClause);

        assertEquals(expected, query);
    }

    @Test
    void testRangeTimezone() throws IOException {

        final TestRunner runner = createRunner(false);
        runner.setProperty(ConsumeElasticsearch.RANGE_INITIAL_VALUE, RANGE_FIELD_VALUE);
        runner.setProperty(ConsumeElasticsearch.RANGE_TIME_ZONE, "Europe/London");

        final Map<String, Object> query = new HashMap<>();
        ((ConsumeElasticsearch) runner.getProcessor()).addQueryClause(query, null, runner.getProcessContext(), OBJECT_MAPPER);

        final Map<String, Object> rangeFilterClause = getDefaultRangeFilterClause();
        addExpectedRangeFilterClauseField(rangeFilterClause, "time_zone", "Europe/London");
        final Map<String, Map<String, Map<String, List<Map<String, Object>>>>> expected = getDefaultQueryFiltersOnly();
        addExpectedFilterClause(expected, rangeFilterClause);

        assertEquals(expected, query);
    }

    private Map<String, Map<String, Map<String, List<Map<String, Object>>>>> getDefaultQueryFiltersOnly() throws JsonProcessingException {
        return TEST_MAPPER.readValue(DEFAULT_QUERY_FILTERS_ONLY, new TypeReference<>() { });
    }

    private void addExpectedFilterClause(final Map<String, Map<String, Map<String, List<Map<String, Object>>>>> expectedQuery, final Map<String, Object> expectedFilterClause) {
        expectedQuery.get("query").get("bool").get("filter").add(expectedFilterClause);
    }

    private Map<String, Object> getDefaultRangeFilterClause() throws JsonProcessingException {
        return TEST_MAPPER.readValue(DEFAULT_RANGE_FILTER, new TypeReference<>() { });
    }

    @SuppressWarnings("unchecked")
    private void addExpectedRangeFilterClauseField(final Map<String, Object> filterClause, final String fieldName, final Object fieldValue) {
        ((Map<String, Map<String, Object>>) filterClause.get("range")).get(RANGE_FIELD_NAME).put(fieldName, fieldValue);
    }
}
