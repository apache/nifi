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
import static org.junit.jupiter.api.Assertions.fail;

public class SearchElasticsearchTest extends AbstractPaginatedJsonQueryElasticsearchTest {
    @BeforeAll
    public static void setUpBeforeClass() throws Exception {
        AbstractPaginatedJsonQueryElasticsearchTest.setUpBeforeClass();
    }

    AbstractPaginatedJsonQueryElasticsearch getProcessor() {
        return new SearchElasticsearch();
    }

    Scope getStateScope() {
        return Scope.LOCAL;
    }

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
        assertState(runner, paginationType, 10, 1);
        if (runner.getProcessor() instanceof ConsumeElasticsearch) {
            assertFalse(getService(runner).getQuery().contains("\"five\""));
        }

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
        assertState(runner, paginationType, 10, 1);
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
        assertState(runner, paginationType, 20, 2);
        if (runner.getProcessor() instanceof ConsumeElasticsearch) {
            assertTrue(getService(runner).getQuery().contains("\"five\""));
        }
        runner.clearTransferState();
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
            assertState(runner, paginationType, expectedHitCount, iteration);
        } else if (perHitResultOutputStrategy && (iteration == 1 || iteration == 2)) {
            AbstractJsonQueryElasticsearchTest.testCounts(runner, 0, 10, 0, 0);
            runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).forEach(hit -> {
                hit.assertAttributeEquals("hit.count", "1");
                hit.assertAttributeEquals("page.number", String.valueOf(iteration));
            });
            assertState(runner, paginationType, expectedHitCount, iteration);
        } else if ((perResponseResultOutputStrategy || perHitResultOutputStrategy) && iteration == 3) {
            AbstractJsonQueryElasticsearchTest.testCounts(runner, 0, 0, 0, 0);
            if (runner.getProcessor() instanceof ConsumeElasticsearch) {
                assertEquals("five", runner.getStateManager().getState(getStateScope()).get(ConsumeElasticsearch.STATE_RANGE_VALUE));
            } else {
                assertTrue(runner.getStateManager().getState(getStateScope()).toMap().isEmpty());
            }
        } else if (ResultOutputStrategy.PER_QUERY.equals(resultOutputStrategy)) {
            AbstractJsonQueryElasticsearchTest.testCounts(runner, 0, 1, 0, 0);
            runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).getFirst().assertAttributeEquals("hit.count", "20");
            // the "last" page.number is used, so 2 here because there were 2 pages of hits
            runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).getFirst().assertAttributeEquals("page.number", "2");
            assertEquals(20, runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).getFirst().getContent().split("\n").length);
            if (runner.getProcessor() instanceof ConsumeElasticsearch) {
                assertEquals("five", runner.getStateManager().getState(getStateScope()).get(ConsumeElasticsearch.STATE_RANGE_VALUE));
            } else {
                assertTrue(runner.getStateManager().getState(getStateScope()).toMap().isEmpty());
            }
        }
    }

    private void assertState(final TestRunner runner, final PaginationType paginationType, final int hitCount, final int pageCount) throws IOException {
        final MockStateManager stateManager = runner.getStateManager();

        stateManager.assertStateEquals(SearchElasticsearch.STATE_HIT_COUNT, Integer.toString(hitCount), getStateScope());
        stateManager.assertStateEquals(SearchElasticsearch.STATE_PAGE_COUNT, Integer.toString(pageCount), getStateScope());
        if (runner.getProcessor() instanceof ConsumeElasticsearch) {
            stateManager.assertStateEquals(ConsumeElasticsearch.STATE_RANGE_VALUE, "five", getStateScope());
        } else {
            stateManager.assertStateNotSet(ConsumeElasticsearch.STATE_RANGE_VALUE, getStateScope());
        }

        final String pageExpirationTimestamp = stateManager.getState(getStateScope()).get(SearchElasticsearch.STATE_PAGE_EXPIRATION_TIMESTAMP);
        assertTrue(Long.parseLong(pageExpirationTimestamp) > Instant.now().toEpochMilli());

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
            default:
                fail("Unknown paginationType: " + paginationType);
        }
    }
}
