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

import org.apache.nifi.components.state.Scope;
import org.apache.nifi.processors.elasticsearch.api.PaginationType;
import org.apache.nifi.processors.elasticsearch.api.ResultOutputStrategy;
import org.apache.nifi.state.MockStateManager;
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SearchElasticsearchTest extends AbstractPaginatedJsonQueryElasticsearchTest {
    @BeforeAll
    public static void setUpBeforeClass() throws Exception {
        AbstractPaginatedJsonQueryElasticsearchTest.setUpBeforeClass();
    }

    @Override
    AbstractPaginatedJsonQueryElasticsearch getProcessor() {
        return new SearchElasticsearch();
    }

    @Override
    Scope getStateScope() {
        return Scope.LOCAL;
    }

    @Override
    boolean isInput() {
        return false;
    }

    @ParameterizedTest
    @EnumSource(PaginationType.class)
    void testPaginationExpiration(final PaginationType paginationType) throws Exception {
        // test flowfile per page
        final TestRunner runner = createRunner(false);
        final TestElasticsearchClientService service = AbstractJsonQueryElasticsearchTest.getService(runner);
        service.setMaxPages(2);
        runner.setProperty(AbstractPaginatedJsonQueryElasticsearch.PAGINATION_TYPE, paginationType);
        runner.setProperty(AbstractPaginatedJsonQueryElasticsearch.PAGINATION_KEEP_ALIVE, "1 sec");
        setQuery(runner, matchAllWithSortByMsgWithSizeQuery);

        // first page
        runOnce(runner);
        AbstractJsonQueryElasticsearchTest.testCounts(runner, 0, 1, 0, 0);
        runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).getFirst().assertAttributeEquals("hit.count", "10");
        runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).getFirst().assertAttributeEquals("page.number", "1");
        assertState(runner, paginationType, 10, 1, false);
        if (runner.getProcessor() instanceof ConsumeElasticsearch) {
            assertFalse(getService(runner).getQuery().contains("\"five\""));
        }

        if (paginationType == PaginationType.SEARCH_AFTER) {
            Thread.sleep(2000); // Slightly longer than PAGINATION_KEEP_ALIVE of 1 sec

            runner.clearTransferState();

            // does not expire
            runOnce(runner);
            AbstractJsonQueryElasticsearchTest.testCounts(runner, 0, 1, 0, 0);
            runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).getFirst().assertAttributeEquals("hit.count", "10");
            runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).getFirst().assertAttributeEquals("page.number", "2");
            assertState(runner, paginationType, 20, 2, false);
            if (runner.getProcessor() instanceof ConsumeElasticsearch) {
                // trackingRangeValue should be retained after previous query expiry
                assertTrue(getService(runner).getQuery().contains("\"five\""));
            }
            runner.clearTransferState();

        } else {
            // wait for expiration
            final Instant expiration = Instant.ofEpochMilli(Long.parseLong(runner.getStateManager().getState(getStateScope()).get(SearchElasticsearch.STATE_PAGE_EXPIRATION_TIMESTAMP)));
            while (expiration.isAfter(Instant.now())) {
                Thread.sleep(10);
            }

            if ("true".equalsIgnoreCase(System.getenv("CI"))) {
                // allow extra time if running in CI Pipeline to prevent intermittent timing-issue failures
                Thread.sleep(1000);
            }

            service.resetPageCount();
            runner.clearTransferState();

            // first page again (new query after first query expired)
            runOnce(runner);
            AbstractJsonQueryElasticsearchTest.testCounts(runner, 0, 1, 0, 0);
            runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).getFirst().assertAttributeEquals("hit.count", "10");
            runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).getFirst().assertAttributeEquals("page.number", "1");
            assertState(runner, paginationType, 10, 1, false);
            if (runner.getProcessor() instanceof ConsumeElasticsearch) {
                // trackingRangeValue should be retained after previous query expiry
                assertTrue(getService(runner).getQuery().contains("\"five\""));
            }
            runner.clearTransferState();

            // second page
            runOnce(runner);
            AbstractJsonQueryElasticsearchTest.testCounts(runner, 0, 1, 0, 0);
            runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).getFirst().assertAttributeEquals("hit.count", "10");
            runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).getFirst().assertAttributeEquals("page.number", "2");
            assertState(runner, paginationType, 20, 2, false);
            if (runner.getProcessor() instanceof ConsumeElasticsearch) {
                assertTrue(getService(runner).getQuery().contains("\"five\""));
            }
            runner.clearTransferState();
        }
    }

    @Override
    void validatePagination(final TestRunner runner, final ResultOutputStrategy resultOutputStrategy, final PaginationType paginationType, final int iteration) throws IOException {
        final boolean perResponseResultOutputStrategy = ResultOutputStrategy.PER_RESPONSE.equals(resultOutputStrategy);
        final boolean perHitResultOutputStrategy = ResultOutputStrategy.PER_HIT.equals(resultOutputStrategy);
        final int expectedHitCount = 10 * iteration;

        if (perResponseResultOutputStrategy && (iteration == 1 || iteration == 2)) {
            AbstractJsonQueryElasticsearchTest.testCounts(runner, 0, 1, 0, 0);
            runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).getFirst().assertAttributeEquals("hit.count", "10");
            runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).getFirst().assertAttributeEquals("page.number", String.valueOf(iteration));
            assertState(runner, paginationType, expectedHitCount, iteration, false);
        } else if (perHitResultOutputStrategy && (iteration == 1 || iteration == 2)) {
            AbstractJsonQueryElasticsearchTest.testCounts(runner, 0, 10, 0, 0);
            runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).forEach(hit -> {
                hit.assertAttributeEquals("hit.count", "1");
                hit.assertAttributeEquals("page.number", String.valueOf(iteration));
            });
            assertState(runner, paginationType, expectedHitCount, iteration, false);
        } else if ((perResponseResultOutputStrategy || perHitResultOutputStrategy) && iteration == 3) {
            AbstractJsonQueryElasticsearchTest.testCounts(runner, 0, 0, 0, 0);
            if (runner.getProcessor() instanceof ConsumeElasticsearch) {
                assertEquals("five", runner.getStateManager().getState(getStateScope()).get(ConsumeElasticsearch.STATE_RANGE_VALUE));
            }
            assertState(runner, paginationType, 20, 3, true);
        } else if (ResultOutputStrategy.PER_QUERY.equals(resultOutputStrategy)) {
            AbstractJsonQueryElasticsearchTest.testCounts(runner, 0, 1, 0, 0);
            runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).getFirst().assertAttributeEquals("hit.count", "20");
            // the "last" page.number is used, so 2 here because there were 2 pages of hits
            runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).getFirst().assertAttributeEquals("page.number", "2");
            assertEquals(20, runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).getFirst().getContent().split("\n").length);
            if (runner.getProcessor() instanceof ConsumeElasticsearch) {
                assertEquals("five", runner.getStateManager().getState(getStateScope()).get(ConsumeElasticsearch.STATE_RANGE_VALUE));
            }
            assertState(runner, paginationType, 20, 3, true);
        }
    }

    @ParameterizedTest
    @EnumSource(PaginationType.class)
    void testPaginationWithoutRestartOnFinish(final PaginationType paginationType) throws Exception {
        final TestRunner runner = createRunner(false);
        final TestElasticsearchClientService service = AbstractJsonQueryElasticsearchTest.getService(runner);
        service.setMaxPages(2);
        runner.setProperty(AbstractPaginatedJsonQueryElasticsearch.PAGINATION_TYPE, paginationType);
        runner.setProperty(AbstractPaginatedJsonQueryElasticsearch.SEARCH_RESULTS_SPLIT, ResultOutputStrategy.PER_RESPONSE);
        runner.setProperty(SearchElasticsearch.RESTART_ON_FINISH, "false");
        setQuery(runner, matchAllWithSortByMsgWithSizeQuery);

        runOnce(runner);

        AbstractJsonQueryElasticsearchTest.testCounts(runner, 0, 1, 0, 0);
        runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).getLast().assertAttributeEquals("hit.count", "10");
        runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).getLast().assertAttributeEquals("page.number", "1");
        assertState(runner, paginationType, 10, 1, false);
        assertFalse(runner.isYieldCalled());

        runOnce(runner);

        AbstractJsonQueryElasticsearchTest.testCounts(runner, 0, 2, 0, 0);
        runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).getLast().assertAttributeEquals("hit.count", "10");
        runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).getLast().assertAttributeEquals("page.number", "2");
        assertState(runner, paginationType, 20, 2, false);
        assertFalse(runner.isYieldCalled());

        runOnce(runner);

        AbstractJsonQueryElasticsearchTest.testCounts(runner, 0, 2, 0, 0);
        assertState(runner, paginationType, 20, 3, true);
        assertFalse(runner.isYieldCalled());

        runOnce(runner);

        AbstractJsonQueryElasticsearchTest.testCounts(runner, 0, 2, 0, 0);
        assertState(runner, paginationType, 20, 3, true);
        assertTrue(runner.isYieldCalled());
    }

    private void assertState(final TestRunner runner, final PaginationType paginationType, final int hitCount, final int pageCount, final boolean finished) throws IOException {
        final MockStateManager stateManager = runner.getStateManager();

        stateManager.assertStateEquals(SearchElasticsearch.STATE_HIT_COUNT, Integer.toString(hitCount), getStateScope());
        stateManager.assertStateEquals(SearchElasticsearch.STATE_PAGE_COUNT, Integer.toString(pageCount), getStateScope());
        if (runner.getProcessor() instanceof ConsumeElasticsearch) {
            stateManager.assertStateEquals(ConsumeElasticsearch.STATE_RANGE_VALUE, "five", getStateScope());
        } else {
            stateManager.assertStateNotSet(ConsumeElasticsearch.STATE_RANGE_VALUE, getStateScope());
        }

        stateManager.assertStateEquals(ConsumeElasticsearch.STATE_FINISHED, Boolean.toString(finished), getStateScope());
        if (finished) {
            stateManager.assertStateNotSet(SearchElasticsearch.STATE_SCROLL_ID, getStateScope());
            stateManager.assertStateNotSet(SearchElasticsearch.STATE_PIT_ID, getStateScope());
            stateManager.assertStateNotSet(SearchElasticsearch.STATE_SEARCH_AFTER, getStateScope());
            stateManager.assertStateNotSet(SearchElasticsearch.STATE_PAGE_EXPIRATION_TIMESTAMP, getStateScope());
        } else {
            if (paginationType == PaginationType.SEARCH_AFTER) {
                stateManager.assertStateNotSet(SearchElasticsearch.STATE_PAGE_EXPIRATION_TIMESTAMP, getStateScope());
            } else {
                final String pageExpirationTimestamp = stateManager.getState(getStateScope()).get(SearchElasticsearch.STATE_PAGE_EXPIRATION_TIMESTAMP);
                assertTrue(Long.parseLong(pageExpirationTimestamp) > Instant.now().toEpochMilli());
            }

            switch (paginationType) {
                case SCROLL:
                    stateManager.assertStateEquals(SearchElasticsearch.STATE_SCROLL_ID, "scrollId-" + pageCount, getStateScope());
                    stateManager.assertStateNotSet(SearchElasticsearch.STATE_PIT_ID, getStateScope());
                    stateManager.assertStateNotSet(SearchElasticsearch.STATE_SEARCH_AFTER, getStateScope());
                    break;
                case POINT_IN_TIME:
                    stateManager.assertStateNotSet(SearchElasticsearch.STATE_SCROLL_ID, getStateScope());
                    stateManager.assertStateEquals(SearchElasticsearch.STATE_PIT_ID, "pitId-" + pageCount, getStateScope());
                    stateManager.assertStateEquals(SearchElasticsearch.STATE_SEARCH_AFTER, "[\"searchAfter-" + pageCount + "\"]", getStateScope());
                    break;
                case SEARCH_AFTER:
                    stateManager.assertStateNotSet(SearchElasticsearch.STATE_SCROLL_ID, getStateScope());
                    stateManager.assertStateNotSet(SearchElasticsearch.STATE_PIT_ID, getStateScope());
                    stateManager.assertStateEquals(SearchElasticsearch.STATE_SEARCH_AFTER, "[\"searchAfter-" + pageCount + "\"]", getStateScope());
                    break;
            }
        }
    }
}
