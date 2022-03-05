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

package org.apache.nifi.processors.elasticsearch

import org.apache.nifi.components.AllowableValue
import org.apache.nifi.components.state.Scope
import org.apache.nifi.state.MockStateManager
import org.apache.nifi.util.TestRunner
import org.junit.Test

import java.time.Instant

import static groovy.json.JsonOutput.prettyPrint
import static groovy.json.JsonOutput.toJson
import static org.hamcrest.CoreMatchers.is
import static org.hamcrest.MatcherAssert.assertThat
import static org.junit.Assert.fail

class SearchElasticsearchTest extends AbstractPaginatedJsonQueryElasticsearchTest {
    AbstractPaginatedJsonQueryElasticsearch getProcessor() {
        return new SearchElasticsearch()
    }

    boolean isStateUsed() {
        return true
    }

    boolean isInput() {
        return false
    }

    @Test
    void testScrollError() {
        final TestRunner runner = createRunner(false)
        final TestElasticsearchClientService service = getService(runner)
        service.setMaxPages(2)
        service.setThrowErrorInSearch(false)
        runner.setProperty(AbstractPaginatedJsonQueryElasticsearch.PAGINATION_TYPE, AbstractPaginatedJsonQueryElasticsearch.PAGINATION_SCROLL)
        runner.setProperty(AbstractJsonQueryElasticsearch.QUERY, prettyPrint(toJson([size: 10, sort: [ msg: "desc"], query: [ match_all: [:] ]])))

        // initialise search
        runOnce(runner)
        testCounts(runner, 0, 1, 0, 0)
        runner.clearTransferState()

        // scroll (error)
        service.setThrowErrorInSearch(true)
        runOnce(runner)
        testCounts(runner, 0, 0, 0, 0)
        assertThat(runner.getLogger().getErrorMessages().stream()
                .anyMatch({ logMessage ->
                    logMessage.getMsg().contains("Could not query documents") &&
                            logMessage.getThrowable().getMessage() == "Simulated IOException - scroll"
                }),
                is(true)
        )
    }

    @Test
    void testScrollExpiration() {
        testPaginationExpiration(AbstractPaginatedJsonQueryElasticsearch.PAGINATION_SCROLL)
    }

    @Test
    void testPitExpiration() {
        testPaginationExpiration(AbstractPaginatedJsonQueryElasticsearch.PAGINATION_POINT_IN_TIME)
    }

    @Test
    void testSearchAfterExpiration() {
        testPaginationExpiration(AbstractPaginatedJsonQueryElasticsearch.PAGINATION_SEARCH_AFTER)
    }

    private void testPaginationExpiration(final AllowableValue paginationType) {
        // test flowfile per page
        final TestRunner runner = createRunner(false)
        final TestElasticsearchClientService service = getService(runner)
        service.setMaxPages(2)
        runner.setProperty(AbstractPaginatedJsonQueryElasticsearch.PAGINATION_TYPE, paginationType)
        runner.setProperty(AbstractPaginatedJsonQueryElasticsearch.PAGINATION_KEEP_ALIVE, "1 sec")
        runner.setProperty(AbstractJsonQueryElasticsearch.QUERY, prettyPrint(toJson([size: 10, sort: [ msg: "desc"], query: [ match_all: [:] ]])))

        // first page
        runOnce(runner)
        testCounts(runner, 0, 1, 0, 0)
        runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).get(0).assertAttributeEquals("hit.count", "10")
        runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).get(0).assertAttributeEquals("page.number", "1")
        assertState(runner.getStateManager(), paginationType, 10, 1)

        // wait for expiration
        final Instant expiration = Instant.ofEpochMilli(Long.parseLong(runner.getStateManager().getState(Scope.LOCAL).get(SearchElasticsearch.STATE_PAGE_EXPIRATION_TIMESTAMP)))
        while (expiration.isAfter(Instant.now())) {
            Thread.sleep(10)
        }
        if ("true".equalsIgnoreCase(System.getenv("CI"))) {
            // allow extra time if running in CI Pipeline to prevent intermittent timing-issue failures
            Thread.sleep(1000)
        }
        service.resetPageCount()
        runner.clearTransferState()

        // first page again (new query after first query expired)
        runOnce(runner)
        testCounts(runner, 0, 1, 0, 0)
        runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).get(0).assertAttributeEquals("hit.count", "10")
        runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).get(0).assertAttributeEquals("page.number", "1")
        assertState(runner.getStateManager(), paginationType, 10, 1)
        runner.clearTransferState()

        // second page
        runOnce(runner)
        testCounts(runner, 0, 1, 0, 0)
        runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).get(0).assertAttributeEquals("hit.count", "10")
        runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).get(0).assertAttributeEquals("page.number", "2")
        assertState(runner.getStateManager(), paginationType, 20, 2)
        runner.clearTransferState()
    }

    void testPagination(final AllowableValue paginationType) {
        // test flowfile per page
        final TestRunner runner = createRunner(false)
        final TestElasticsearchClientService service = getService(runner)
        service.setMaxPages(2)
        runner.setProperty(AbstractPaginatedJsonQueryElasticsearch.PAGINATION_TYPE, paginationType)
        runner.setProperty(AbstractJsonQueryElasticsearch.QUERY, prettyPrint(toJson([size: 10, sort: [ msg: "desc"], query: [ match_all: [:] ]])))

        // first page
        runOnce(runner)
        testCounts(runner, 0, 1, 0, 0)
        runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).get(0).assertAttributeEquals("hit.count", "10")
        runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).get(0).assertAttributeEquals("page.number", "1")
        assertState(runner.getStateManager(), paginationType, 10, 1)
        runner.clearTransferState()

        // second page
        runOnce(runner)
        testCounts(runner, 0, 1, 0, 0)
        runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).get(0).assertAttributeEquals("hit.count", "10")
        runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).get(0).assertAttributeEquals("page.number", "2")
        assertState(runner.getStateManager(), paginationType, 20, 2)
        runner.clearTransferState()

        // third page - no hits
        runOnce(runner)
        testCounts(runner, 0, 0, 0, 0)
        assertThat(runner.getStateManager().getState(Scope.LOCAL).toMap().isEmpty(), is(true))
        reset(runner)


        // test hits splitting
        runner.setProperty(AbstractPaginatedJsonQueryElasticsearch.SEARCH_RESULTS_SPLIT, AbstractJsonQueryElasticsearch.FLOWFILE_PER_HIT)

        // first page
        runOnce(runner)
        testCounts(runner, 0, 10, 0, 0)
        runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).forEach(
                { hit ->
                    hit.assertAttributeEquals("hit.count", "1")
                    hit.assertAttributeEquals("page.number", "1")
                }
        )
        assertState(runner.getStateManager(), paginationType, 10, 1)
        runner.clearTransferState()

        // second page
        runOnce(runner)
        testCounts(runner, 0, 10, 0, 0)
        runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).forEach(
                { hit ->
                    hit.assertAttributeEquals("hit.count", "1")
                    hit.assertAttributeEquals("page.number", "2")
                }
        )
        assertState(runner.getStateManager(), paginationType, 20, 2)
        runner.clearTransferState()

        // third page - no hits
        runOnce(runner)
        testCounts(runner, 0, 0, 0, 0)
        assertThat(runner.getStateManager().getState(Scope.LOCAL).toMap().isEmpty(), is(true))
        reset(runner)


        // test hits combined
        runner.setProperty(AbstractPaginatedJsonQueryElasticsearch.SEARCH_RESULTS_SPLIT, AbstractPaginatedJsonQueryElasticsearch.FLOWFILE_PER_QUERY)
        // hits are combined from all pages within a single trigger of the processor
        runOnce(runner)
        testCounts(runner, 0, 1, 0, 0)
        runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).get(0).assertAttributeEquals("hit.count", "20")
        // the "last" page.number is used, so 2 here because there were 2 pages of hits
        runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).get(0).assertAttributeEquals("page.number", "2")
        assertThat(
                runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).get(0).getContent().split("\n").length,
                is(20)
        )
        assertThat(runner.getStateManager().getState(Scope.LOCAL).toMap().isEmpty(), is(true))
    }

    private static void assertState(final MockStateManager stateManager, final AllowableValue paginationType,
                                    final int hitCount, final int pageCount) {
        stateManager.assertStateEquals(SearchElasticsearch.STATE_HIT_COUNT, Integer.toString(hitCount), Scope.LOCAL)
        stateManager.assertStateEquals(SearchElasticsearch.STATE_PAGE_COUNT, Integer.toString(pageCount), Scope.LOCAL)

        final String pageExpirationTimestamp = stateManager.getState(Scope.LOCAL).get(SearchElasticsearch.STATE_PAGE_EXPIRATION_TIMESTAMP)
        assertThat(Long.parseLong(pageExpirationTimestamp) > Instant.now().toEpochMilli(), is(true))

        switch (paginationType) {
            case AbstractPaginatedJsonQueryElasticsearch.PAGINATION_SCROLL:
                stateManager.assertStateEquals(SearchElasticsearch.STATE_SCROLL_ID, "scrollId-${pageCount}", Scope.LOCAL)
                stateManager.assertStateNotSet(SearchElasticsearch.STATE_PIT_ID, Scope.LOCAL)
                stateManager.assertStateNotSet(SearchElasticsearch.STATE_SEARCH_AFTER, Scope.LOCAL)
                break
            case AbstractPaginatedJsonQueryElasticsearch.PAGINATION_POINT_IN_TIME:
                stateManager.assertStateNotSet(SearchElasticsearch.STATE_SCROLL_ID, Scope.LOCAL)
                stateManager.assertStateEquals(SearchElasticsearch.STATE_PIT_ID, "pitId-${pageCount}", Scope.LOCAL)
                stateManager.assertStateEquals(SearchElasticsearch.STATE_SEARCH_AFTER, "[\"searchAfter-${pageCount}\"]", Scope.LOCAL)
                break
            case AbstractPaginatedJsonQueryElasticsearch.PAGINATION_SEARCH_AFTER:
                stateManager.assertStateNotSet(SearchElasticsearch.STATE_SCROLL_ID, Scope.LOCAL)
                stateManager.assertStateNotSet(SearchElasticsearch.STATE_PIT_ID, Scope.LOCAL)
                stateManager.assertStateEquals(SearchElasticsearch.STATE_SEARCH_AFTER, "[\"searchAfter-${pageCount}\"]", Scope.LOCAL)
                break
            default:
                fail("Unknown paginationType: ${paginationType}")
        }
    }
}
