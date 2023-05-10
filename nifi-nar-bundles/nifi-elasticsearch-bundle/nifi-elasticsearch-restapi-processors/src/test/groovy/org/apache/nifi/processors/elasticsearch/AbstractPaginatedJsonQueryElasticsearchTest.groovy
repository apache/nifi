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

import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processors.elasticsearch.api.AggregationResultsFormat
import org.apache.nifi.processors.elasticsearch.api.PaginationType
import org.apache.nifi.processors.elasticsearch.api.ResultOutputStrategy
import org.apache.nifi.processors.elasticsearch.api.SearchResultsFormat
import org.apache.nifi.provenance.ProvenanceEventType
import org.apache.nifi.util.MockFlowFile
import org.apache.nifi.util.TestRunner
import org.junit.jupiter.api.Test

import static groovy.json.JsonOutput.prettyPrint
import static groovy.json.JsonOutput.toJson
import static org.hamcrest.CoreMatchers.equalTo
import static org.hamcrest.CoreMatchers.is
import static org.hamcrest.MatcherAssert.assertThat
import static org.junit.jupiter.api.Assertions.assertFalse
import static org.junit.jupiter.api.Assertions.assertThrows
import static org.junit.jupiter.api.Assertions.assertTrue

abstract class AbstractPaginatedJsonQueryElasticsearchTest extends AbstractJsonQueryElasticsearchTest<AbstractPaginatedJsonQueryElasticsearch> {
    abstract boolean isInput()

    @Test
    void testInvalidPaginationProperties() {
        final TestRunner runner = createRunner(false)
        runner.setProperty(AbstractJsonQueryElasticsearch.QUERY, prettyPrint(toJson([query: [match_all: [:]]])))
        runner.setProperty(AbstractPaginatedJsonQueryElasticsearch.PAGINATION_KEEP_ALIVE, "not-a-period")
        runner.setProperty(AbstractPaginatedJsonQueryElasticsearch.PAGINATION_TYPE, "not-enum")

        final AssertionError assertionError = assertThrows(AssertionError.class, runner.&run)
        assertThat(assertionError.getMessage(), equalTo(String.format("Processor has 2 validation failures:\n" +
                "'%s' validated against 'not-enum' is invalid because Given value not found in allowed set '%s'\n" +
                "'%s' validated against 'not-a-period' is invalid because Must be of format <duration> <TimeUnit> where <duration> " +
                "is a non-negative integer and TimeUnit is a supported Time Unit, such as: nanos, millis, secs, mins, hrs, days\n",
                AbstractPaginatedJsonQueryElasticsearch.PAGINATION_TYPE.getName(), PaginationType.values().collect { p -> p.getValue() }.join(", "),
                AbstractPaginatedJsonQueryElasticsearch.PAGINATION_KEEP_ALIVE.getName(),
                AbstractPaginatedJsonQueryElasticsearch.PAGINATION_KEEP_ALIVE.getName()
        )))
    }

    @Test
    void testSinglePage() {
        // paged query hits (no splitting)
        final TestRunner runner = createRunner(false)
        runner.setProperty(AbstractJsonQueryElasticsearch.QUERY, prettyPrint(toJson([query: [match_all: [:]]])))
        MockFlowFile input = runOnce(runner)
        testCounts(runner, isInput() ? 1 : 0, 1, 0, 0)
        FlowFile hits = runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).get(0)
        hits.assertAttributeEquals("hit.count", "10")
        hits.assertAttributeEquals("page.number", "1")
        assertOutputContent(hits.getContent(), 10, false)
        assertThat(
                runner.getProvenanceEvents().stream().filter({ pe ->
                    pe.getEventType() == ProvenanceEventType.RECEIVE &&
                            pe.getAttribute("uuid") == runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).get(0).getAttribute("uuid")
                }).count(),
                is(1L)
        )
        assertSendEvent(runner, input)
        reset(runner)


        // paged query hits splitting
        runner.setProperty(AbstractJsonQueryElasticsearch.SEARCH_RESULTS_SPLIT, ResultOutputStrategy.PER_HIT.getValue())
        input = runOnce(runner)
        testCounts(runner, isInput() ? 1 : 0, 10, 0, 0)
        runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).forEach(
                { hit ->
                    hit.assertAttributeEquals("hit.count", "1")
                    hit.assertAttributeEquals("page.number", "1")
                    assertOutputContent(hit.getContent(), 1, false)
                    assertThat(
                            runner.getProvenanceEvents().stream().filter({ pe ->
                                pe.getEventType() == ProvenanceEventType.RECEIVE &&
                                        pe.getAttribute("uuid") == hit.getAttribute("uuid")
                            }).count(),
                            is(1L)
                    )
                }
        )
        assertSendEvent(runner, input)
        reset(runner)


        // paged query hits combined
        runner.setProperty(AbstractJsonQueryElasticsearch.SEARCH_RESULTS_SPLIT, ResultOutputStrategy.PER_QUERY.getValue())
        input = runOnce(runner)
        testCounts(runner, isInput() ? 1 : 0, 1, 0, 0)
        hits = runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).get(0)
        hits.assertAttributeEquals("hit.count", "10")
        hits.assertAttributeEquals("page.number", "1")
        assertOutputContent(hits.getContent(), 10, true)
        assertThat(
                runner.getProvenanceEvents().stream().filter({ pe ->
                    pe.getEventType() == ProvenanceEventType.RECEIVE &&
                            pe.getAttribute("uuid") == hits.getAttribute("uuid")
                }).count(),
                is(1L)
        )
        assertSendEvent(runner, input)
    }

   static void assertFormattedResult(final SearchResultsFormat searchResultsFormat, final Map<String, Object> hit) {
        assertFalse(hit.isEmpty())
        switch(searchResultsFormat) {
            case SearchResultsFormat.SOURCE_ONLY:
                assertFalse(hit.containsKey("_source"))
                assertFalse(hit.containsKey("_index"))
                break
            case SearchResultsFormat.METADATA_ONLY:
                assertFalse(hit.containsKey("_source"))
                assertTrue(hit.containsKey("_index"))
                break
            case SearchResultsFormat.FULL:
                assertTrue(hit.containsKey("_source"))
                assertTrue(hit.containsKey("_index"))
                break
            default:
                throw new IllegalArgumentException("Unknown SearchResultsFormat value: " + searchResultsFormat.toString())
        }
    }

    private void assertResultsFormat(final TestRunner runner, final ResultOutputStrategy resultOutputStrategy, final SearchResultsFormat searchResultsFormat) {
        int flowFileCount
        String hitsCount
        boolean ndjson = false

        switch (resultOutputStrategy) {
            case ResultOutputStrategy.PER_QUERY:
                flowFileCount = 1
                hitsCount = "10"
                ndjson = true
                break
            case ResultOutputStrategy.PER_HIT:
                flowFileCount = 10
                hitsCount = "1"
                break
            case ResultOutputStrategy.PER_RESPONSE:
                flowFileCount = 1
                hitsCount = "10"
                break
            default:
                throw new IllegalArgumentException("Unknown ResultOutputStrategy value: " + resultOutputStrategy.toString())
        }

        // Test Relationship counts
        testCounts(runner, isInput() ? 1 : 0, flowFileCount, 0, 0)

        // Per response outputs an array of values
        runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).forEach({ hit ->
            hit.assertAttributeEquals("hit.count", hitsCount)
            assertOutputContent(hit.getContent(), hitsCount as int, ndjson)
            if (ResultOutputStrategy.PER_RESPONSE == resultOutputStrategy) {
                OBJECT_MAPPER.readValue(hit.getContent(), ArrayList.class).forEach(h -> {
                    assertFormattedResult(searchResultsFormat, h as Map<String, Object>)
                })
            } else {
                final Map<String, Object> h = OBJECT_MAPPER.readValue(hit.getContent(), Map.class)
                assertFormattedResult(searchResultsFormat, h)
            }
            assertThat(
                    runner.getProvenanceEvents().stream().filter({ pe ->
                        pe.getEventType() == ProvenanceEventType.RECEIVE &&
                                pe.getAttribute("uuid") == hit.getAttribute("uuid")
                    }).count(),
                    is(1L)
            )
        })
    }

    @Test
    void testResultsFormat() throws Exception {
        for (final ResultOutputStrategy resultOutputStrategy : ResultOutputStrategy.values()) {
            final TestRunner runner = createRunner(false)
            runner.setProperty(AbstractJsonQueryElasticsearch.QUERY, prettyPrint(toJson([query: [match_all: [:]], "sort": [[message: [order: "asc"]]]])))
            runner.setProperty(AbstractJsonQueryElasticsearch.SEARCH_RESULTS_SPLIT, resultOutputStrategy.getValue())

            // Test against each results format
            for (final SearchResultsFormat searchResultsFormat : SearchResultsFormat.values()) {
                runner.setProperty(AbstractJsonQueryElasticsearch.SEARCH_RESULTS_FORMAT, searchResultsFormat.getValue())

                // Test against each pagination type
                for (final PaginationType paginationType : PaginationType.values()) {
                    runner.setProperty(AbstractPaginatedJsonQueryElasticsearch.PAGINATION_TYPE, paginationType.getValue())

                    runOnce(runner)
                    assertResultsFormat(runner, resultOutputStrategy, searchResultsFormat)
                    reset(runner)
                }
            }
        }
    }

    @Test
    void testScrollError() {
        final TestRunner runner = createRunner(false)
        final TestElasticsearchClientService service = getService(runner)
        service.setThrowErrorInDelete(true)
        runner.setProperty(AbstractPaginatedJsonQueryElasticsearch.PAGINATION_TYPE, PaginationType.SCROLL.getValue())
        runner.setProperty(AbstractJsonQueryElasticsearch.QUERY, prettyPrint(toJson([sort: [msg: "desc"], query: [match_all: [:]]])))

        // still expect "success" output for exception during final clean-up
        runMultiple(runner, 2)
        testCounts(runner, isInput() ? 1 : 0, 1, 0, 0)

        // check error was caught and logged
        assertThat(runner.getLogger().getWarnMessages().stream()
                .anyMatch({ logMessage ->
                    logMessage.getMsg().contains("Error while cleaning up Elasticsearch pagination resources") &&
                            logMessage.getMsg().contains("Simulated IOException - deleteScroll")
                }),
                is(true)
        )
    }

    @Test
    void testDeletePitError() {
        final TestRunner runner = createRunner(false)
        final TestElasticsearchClientService service = getService(runner)
        service.setThrowErrorInDelete(true)
        runner.setProperty(AbstractJsonQueryElasticsearch.SEARCH_RESULTS_FORMAT, SearchResultsFormat.FULL.getValue())
        runner.setProperty(AbstractJsonQueryElasticsearch.AGGREGATION_RESULTS_FORMAT, AggregationResultsFormat.FULL.getValue())
        runner.setProperty(AbstractPaginatedJsonQueryElasticsearch.PAGINATION_TYPE, PaginationType.POINT_IN_TIME.getValue())
        runner.setProperty(AbstractJsonQueryElasticsearch.QUERY, prettyPrint(toJson([sort: [msg: "desc"], query: [match_all: [:]]])))

        // still expect "success" output for exception during final clean-up
        runMultiple(runner, 2)
        testCounts(runner, isInput() ? 1 : 0, 1, 0, 0)

        // check error was caught and logged
        assertThat(runner.getLogger().getWarnMessages().stream()
                .anyMatch({ logMessage ->
                    logMessage.getMsg().contains("Error while cleaning up Elasticsearch pagination resources") &&
                            logMessage.getMsg().contains("Simulated IOException - deletePointInTime")
                }),
                is(true)
        )
    }

    @Test
    void testInitialisePitError() {
        final TestRunner runner = createRunner(false)
        final TestElasticsearchClientService service = getService(runner)
        service.setThrowErrorInPit(true)
        runner.setProperty(AbstractPaginatedJsonQueryElasticsearch.PAGINATION_TYPE, PaginationType.POINT_IN_TIME.getValue())
        runner.setProperty(AbstractJsonQueryElasticsearch.QUERY, prettyPrint(toJson([sort: [msg: "desc"], query: [match_all: [:]]])))

        // expect "failure" output for exception during query setup
        runOnce(runner)
        testCounts(runner, 0, 0, isInput() ? 1 : 0, 0)

        // check error was caught and logged
        assertThat(runner.getLogger().getErrorMessages().stream()
                .anyMatch({ logMessage ->
                    logMessage.getMsg().contains("Could not query documents") &&
                            logMessage.getThrowable().getMessage() == "Simulated IOException - initialisePointInTime"
                }),
                is(true)
        )
    }

    @Test
    void testQuerySortError() {
        // test PiT without sort
        final TestRunner runner = createRunner(false)
        runner.setProperty(AbstractPaginatedJsonQueryElasticsearch.PAGINATION_TYPE, PaginationType.POINT_IN_TIME.getValue())
        runner.setProperty(AbstractJsonQueryElasticsearch.QUERY, prettyPrint(toJson([query: [match_all: [:]]])))

        // expect "failure" output for exception during query setup
        runOnce(runner)
        testCounts(runner, 0, 0, isInput() ? 1 : 0, 0)

        // check error was caught and logged
        assertThat(runner.getLogger().getErrorMessages().stream()
                .anyMatch({ logMessage ->
                    logMessage.getMsg().contains("Could not query documents") &&
                            logMessage.getThrowable().getMessage() == "Query using pit/search_after must contain a \"sort\" field"
                }),
                is(true)
        )
        reset(runner)


        // test search_after without sort
        runner.setProperty(AbstractPaginatedJsonQueryElasticsearch.PAGINATION_TYPE, PaginationType.SEARCH_AFTER.getValue())
        runOnce(runner)
        testCounts(runner, 0, 0, isInput() ? 1 : 0, 0)
        assertThat(runner.getLogger().getErrorMessages().stream()
                .anyMatch({ logMessage ->
                    logMessage.getMsg().contains("Could not query documents") &&
                            logMessage.getThrowable().getMessage() == "Query using pit/search_after must contain a \"sort\" field"
                }),
                is(true)
        )
        reset(runner)


        // test scroll without sort (should succeed)
        runner.setProperty(AbstractPaginatedJsonQueryElasticsearch.PAGINATION_TYPE, PaginationType.SCROLL.getValue())
        runMultiple(runner, 2)
        testCounts(runner, isInput() ? 1 : 0, 1, 0, 0)
    }

    @Test
    void testScroll() {
        testPagination(PaginationType.SCROLL)
    }

    @Test
    void testPit() {
        testPagination(PaginationType.POINT_IN_TIME)
    }

    @Test
    void testSearchAfter() {
        testPagination(PaginationType.SEARCH_AFTER)
    }

    abstract void testPagination(final PaginationType paginationType)

    private void runMultiple(final TestRunner runner, final int maxIterations) {
        if (isInput()) {
            // with an input FlowFile, the processor still only triggers a single time and completes all processing
            runOnce(runner)
        } else {
            // with no input, the processor executes multiple times and tracks progress using STATE.Local
            runner.setIncomingConnection(false)
            runner.run(maxIterations, true, true)
        }
    }

    private void assertSendEvent(final TestRunner runner, final MockFlowFile input) {
        if (isInput()) {
            assertThat(
                    runner.getProvenanceEvents().stream().filter({ pe ->
                        pe.getEventType() == ProvenanceEventType.SEND &&
                                pe.getAttribute("uuid") == input.getAttribute("uuid")
                    }).count(),
                    is(1L)
            )
        } else {
            assertThat(runner.getProvenanceEvents().stream().filter({ pe -> pe.getEventType() == ProvenanceEventType.SEND }).count(), is(0L))
        }
    }

    @Test
    void testEmptyHitsFlowFileIsProducedForEachResultSplitSetup() {
        final TestRunner runner = createRunner(false)
        final TestElasticsearchClientService service = getService(runner)
        runner.setProperty(AbstractJsonQueryElasticsearch.QUERY, prettyPrint(toJson([query: [match_all: [:]], "sort": [[message: [order: "asc"]]]])))
        runner.setProperty(AbstractPaginatedJsonQueryElasticsearch.OUTPUT_NO_HITS, "true")
        service.setMaxPages(0)


        for (final PaginationType paginationType : PaginationType.values()) {
            runner.setProperty(AbstractPaginatedJsonQueryElasticsearch.PAGINATION_TYPE, paginationType.getValue())

            for (final ResultOutputStrategy resultOutputStrategy : ResultOutputStrategy.values()) {
                // test that an empty flow file is produced for a per query setup
                runner.setProperty(AbstractPaginatedJsonQueryElasticsearch.SEARCH_RESULTS_SPLIT, resultOutputStrategy.getValue())
                runOnce(runner)
                testCounts(runner, isInput() ? 1 : 0, 1, 0, 0)

                runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).get(0).assertAttributeEquals("hit.count", "0")
                runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).get(0).assertAttributeEquals("page.number", "1")
                runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).get(0).getSize() == 0
                reset(runner)
            }
        }


    }
}
