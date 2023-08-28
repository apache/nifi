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

import org.apache.nifi.processors.elasticsearch.api.AggregationResultsFormat;
import org.apache.nifi.processors.elasticsearch.api.PaginationType;
import org.apache.nifi.processors.elasticsearch.api.ResultOutputStrategy;
import org.apache.nifi.processors.elasticsearch.api.SearchResultsFormat;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class AbstractPaginatedJsonQueryElasticsearchTest extends AbstractJsonQueryElasticsearchTest<AbstractPaginatedJsonQueryElasticsearch> {
   protected static String matchAllWithSortByMsgWithSizeQuery;
   private static final String TEST_DIR = "src/test/resources/AbstractPaginatedJsonQueryElasticsearchTest";
   private static String matchAllWithSortByMessage;
   private static String matchAllWithSortByMsgWithoutSize;

    @BeforeAll
    public static void setUpBeforeClass() throws Exception {
        AbstractJsonQueryElasticsearchTest.setUpBeforeClass();
        matchAllWithSortByMessage = Files.readString(Paths.get(TEST_DIR, "matchAllWithSortByMessageQuery.json"));
        matchAllWithSortByMsgWithoutSize = Files.readString(Paths.get(TEST_DIR,"matchAllWithSortByMsgQueryWithoutSize.json"));
        matchAllWithSortByMsgWithSizeQuery = Files.readString(Paths.get(TEST_DIR, "matchAllWithSortByMsgQueryWithSize.json"));
    }

    public abstract boolean isInput();

    @Test
    public void testInvalidPaginationProperties() {
        final TestRunner runner = createRunner(false);
        runner.setProperty(AbstractJsonQueryElasticsearch.QUERY, matchAllQuery);
        runner.setProperty(AbstractPaginatedJsonQueryElasticsearch.PAGINATION_KEEP_ALIVE, "not-a-period");
        runner.setProperty(AbstractPaginatedJsonQueryElasticsearch.PAGINATION_TYPE, "not-enum");

        final AssertionError assertionError = assertThrows(AssertionError.class, runner::run);
        String expected = String.format("Processor has 2 validation failures:\n" +
                        "'%s' validated against 'not-enum' is invalid because Given value not found in allowed set '%s'\n" +
                        "'%s' validated against 'not-a-period' is invalid because Must be of format <duration> <TimeUnit> where <duration> " +
                        "is a non-negative integer and TimeUnit is a supported Time Unit, such as: nanos, millis, secs, mins, hrs, days\n",
                AbstractPaginatedJsonQueryElasticsearch.PAGINATION_TYPE.getName(),
                Stream.of(PaginationType.values()).map(PaginationType::getValue).collect(Collectors.joining(", ")),
                AbstractPaginatedJsonQueryElasticsearch.PAGINATION_KEEP_ALIVE.getName());
        assertEquals(expected, assertionError.getMessage());
    }

    @Test
    public void testSinglePage() {
        // paged query hits (no splitting)
        final TestRunner runner = createRunner(false);
        runner.setProperty(AbstractJsonQueryElasticsearch.QUERY, matchAllQuery);
        MockFlowFile input = runOnce(runner);
        AbstractJsonQueryElasticsearchTest.testCounts(runner, isInput() ? 1 : 0, 1, 0, 0);
        final MockFlowFile pageQueryHitsNoSplitting = runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).get(0);
        pageQueryHitsNoSplitting.assertAttributeEquals("hit.count", "10");
        pageQueryHitsNoSplitting.assertAttributeEquals("page.number", "1");
        AbstractJsonQueryElasticsearchTest.assertOutputContent(pageQueryHitsNoSplitting.getContent(), 10, false);
        assertEquals(1L, runner.getProvenanceEvents().stream().filter(event ->
                ProvenanceEventType.RECEIVE.equals(event.getEventType()) && event.getAttribute("uuid").equals(pageQueryHitsNoSplitting.getAttribute("uuid"))).count());

        assertSendEvent(runner, input);
        reset(runner);

        // paged query hits splitting
        runner.setProperty(AbstractJsonQueryElasticsearch.SEARCH_RESULTS_SPLIT, ResultOutputStrategy.PER_HIT.getValue());
        input = runOnce(runner);
        AbstractJsonQueryElasticsearchTest.testCounts(runner, isInput() ? 1 : 0, 10, 0, 0);
        runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).forEach(hit -> {
            hit.assertAttributeEquals("hit.count", "1");
            hit.assertAttributeEquals("page.number", "1");
            assertOutputContent(hit.getContent(), 1, false);
            assertEquals(1L, runner.getProvenanceEvents().stream().filter(event ->
                    ProvenanceEventType.RECEIVE.equals(event.getEventType()) && event.getAttribute("uuid").equals(hit.getAttribute("uuid"))).count());
        });
        assertSendEvent(runner, input);
        reset(runner);

        // paged query hits combined
        runner.setProperty(AbstractJsonQueryElasticsearch.SEARCH_RESULTS_SPLIT, ResultOutputStrategy.PER_QUERY.getValue());
        input = runOnce(runner);
        AbstractJsonQueryElasticsearchTest.testCounts(runner, isInput() ? 1 : 0, 1, 0, 0);
        final MockFlowFile pageQueryHitsCombined = runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).get(0);
        pageQueryHitsCombined.assertAttributeEquals("hit.count", "10");
        pageQueryHitsCombined.assertAttributeEquals("page.number", "1");
        AbstractJsonQueryElasticsearchTest.assertOutputContent(pageQueryHitsCombined.getContent(), 10, true);
        assertEquals(1L, runner.getProvenanceEvents().stream().filter(event ->
                ProvenanceEventType.RECEIVE.equals(event.getEventType()) && event.getAttribute("uuid").equals(pageQueryHitsCombined.getAttribute("uuid"))).count());
        assertSendEvent(runner, input);
    }


    public static void assertFormattedResult(final SearchResultsFormat searchResultsFormat, final Map<String, Object> hit) {
        assertFalse(hit.isEmpty());
        switch (searchResultsFormat) {
            case SOURCE_ONLY:
                assertFalse(hit.containsKey("_source"));
                assertFalse(hit.containsKey("_index"));
                break;
            case METADATA_ONLY:
                assertFalse(hit.containsKey("_source"));
                assertTrue(hit.containsKey("_index"));
                break;
            case FULL:
                assertTrue(hit.containsKey("_source"));
                assertTrue(hit.containsKey("_index"));
                break;
            default:
                throw new IllegalArgumentException("Unknown SearchResultsFormat value: " + searchResultsFormat);
        }
    }

    private void assertResultsFormat(final TestRunner runner, final ResultOutputStrategy resultOutputStrategy, final SearchResultsFormat searchResultsFormat) {
        int flowFileCount;
        String hitsCount;
        boolean ndjson = false;

        switch (resultOutputStrategy) {
            case PER_QUERY:
                flowFileCount = 1;
                hitsCount = "10";
                ndjson = true;
                break;
            case PER_HIT:
                flowFileCount = 10;
                hitsCount = "1";
                break;
            case PER_RESPONSE:
                flowFileCount = 1;
                hitsCount = "10";
                break;
            default:
                throw new IllegalArgumentException("Unknown ResultOutputStrategy value: " + resultOutputStrategy);
        }

        // Test Relationship counts
        AbstractJsonQueryElasticsearchTest.testCounts(runner, isInput() ? 1 : 0, flowFileCount, 0, 0);

        // Per response outputs an array of values
        final boolean ndJsonCopy = ndjson;
        runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).forEach( hit -> {
            hit.assertAttributeEquals("hit.count", hitsCount);
            AbstractJsonQueryElasticsearchTest.assertOutputContent(hit.getContent(), Integer.parseInt(hitsCount), ndJsonCopy);
            if (ResultOutputStrategy.PER_RESPONSE.equals(resultOutputStrategy)) {
                JsonUtils.readListOfMaps(hit.getContent()).forEach(h -> assertFormattedResult(searchResultsFormat, h));
            } else {
                final Map<String, Object> h = JsonUtils.readMap(hit.getContent());
                assertFormattedResult(searchResultsFormat, h);
            }

            assertEquals(1L, runner.getProvenanceEvents().stream().filter(event ->
                    ProvenanceEventType.RECEIVE.equals(event.getEventType()) && event.getAttribute("uuid").equals(hit.getAttribute("uuid"))).count());
        });
    }

    @Test
    public void testResultsFormat() {
        for (final ResultOutputStrategy resultOutputStrategy : ResultOutputStrategy.values()) {
            final TestRunner runner = createRunner(false);
            runner.setProperty(AbstractJsonQueryElasticsearch.QUERY, matchAllWithSortByMessage);
            runner.setProperty(AbstractJsonQueryElasticsearch.SEARCH_RESULTS_SPLIT, resultOutputStrategy.getValue());

            // Test against each results format
            for (final SearchResultsFormat searchResultsFormat : SearchResultsFormat.values()) {
                runner.setProperty(AbstractJsonQueryElasticsearch.SEARCH_RESULTS_FORMAT, searchResultsFormat.getValue());

                // Test against each pagination type
                for (final PaginationType paginationType : PaginationType.values()) {
                    runner.setProperty(AbstractPaginatedJsonQueryElasticsearch.PAGINATION_TYPE, paginationType.getValue());

                    runOnce(runner);
                    assertResultsFormat(runner, resultOutputStrategy, searchResultsFormat);
                    reset(runner);
                }
            }
        }
    }

    @Test
    public void testScrollError() {
        final TestRunner runner = createRunner(false);
        final TestElasticsearchClientService service = AbstractJsonQueryElasticsearchTest.getService(runner);
        service.setThrowErrorInDelete(true);
        runner.setProperty(AbstractPaginatedJsonQueryElasticsearch.PAGINATION_TYPE, PaginationType.SCROLL.getValue());
        runner.setProperty(AbstractJsonQueryElasticsearch.QUERY, matchAllWithSortByMsgWithoutSize);

        // still expect "success" output for exception during final clean-up
        runMultiple(runner);
        AbstractJsonQueryElasticsearchTest.testCounts(runner, isInput() ? 1 : 0, 1, 0, 0);
        assertTrue(runner.getLogger().getWarnMessages().stream().anyMatch(logMessage ->
                logMessage.getMsg().contains("Error while cleaning up Elasticsearch pagination resources") && logMessage.getMsg().contains("Simulated IOException - deleteScroll")));
        // check error was caught and logged
    }

    @Test
    public void testDeletePitError() {
        final TestRunner runner = createRunner(false);
        final TestElasticsearchClientService service = AbstractJsonQueryElasticsearchTest.getService(runner);
        service.setThrowErrorInDelete(true);
        runner.setProperty(AbstractJsonQueryElasticsearch.SEARCH_RESULTS_FORMAT, SearchResultsFormat.FULL.getValue());
        runner.setProperty(AbstractJsonQueryElasticsearch.AGGREGATION_RESULTS_FORMAT, AggregationResultsFormat.FULL.getValue());
        runner.setProperty(AbstractPaginatedJsonQueryElasticsearch.PAGINATION_TYPE, PaginationType.POINT_IN_TIME.getValue());
        runner.setProperty(AbstractJsonQueryElasticsearch.QUERY, matchAllWithSortByMsgWithoutSize);

        // still expect "success" output for exception during final clean-up
        runMultiple(runner);
        AbstractJsonQueryElasticsearchTest.testCounts(runner, isInput() ? 1 : 0, 1, 0, 0);
        // check error was caught and logged
        assertTrue(runner.getLogger().getWarnMessages().stream().anyMatch(logMessage ->
                logMessage.getMsg().contains("Error while cleaning up Elasticsearch pagination resources") && logMessage.getMsg().contains("Simulated IOException - deletePointInTime")));
    }

    @Test
    public void testInitialisePitError() {
        final TestRunner runner = createRunner(false);
        final TestElasticsearchClientService service = AbstractJsonQueryElasticsearchTest.getService(runner);
        service.setThrowErrorInPit(true);
        runner.setProperty(AbstractPaginatedJsonQueryElasticsearch.PAGINATION_TYPE, PaginationType.POINT_IN_TIME.getValue());
        runner.setProperty(AbstractJsonQueryElasticsearch.QUERY, matchAllWithSortByMsgWithoutSize);

        // expect "failure" output for exception during query setup
        runOnce(runner);
        AbstractJsonQueryElasticsearchTest.testCounts(runner, 0, 0, isInput() ? 1 : 0, 0);
        // check error was caught and logged
        assertTrue(runner.getLogger().getErrorMessages().stream().anyMatch(logMessage ->
                logMessage.getMsg().contains("Could not query documents") && logMessage.getThrowable().getMessage().contains("Simulated IOException - initialisePointInTime")));
    }

    @Test
    public void testQuerySortError() {
        // test PiT without sort
        final TestRunner runner = createRunner(false);
        runner.setProperty(AbstractPaginatedJsonQueryElasticsearch.PAGINATION_TYPE, PaginationType.POINT_IN_TIME.getValue());
        runner.setProperty(AbstractJsonQueryElasticsearch.QUERY, matchAllQuery);

        // expect "failure" output for exception during query setup
        runOnce(runner);
        AbstractJsonQueryElasticsearchTest.testCounts(runner, 0, 0, isInput() ? 1 : 0, 0);

        // check error was caught and logged
        assertTrue(runner.getLogger().getErrorMessages().stream().anyMatch(logMessage ->
                logMessage.getMsg().contains("Could not query documents") && logMessage.getThrowable().getMessage().equals("Query using pit/search_after must contain a \"sort\" field")));
        reset(runner);


        // test search_after without sort
        runner.setProperty(AbstractPaginatedJsonQueryElasticsearch.PAGINATION_TYPE, PaginationType.SEARCH_AFTER.getValue());
        runOnce(runner);
        AbstractJsonQueryElasticsearchTest.testCounts(runner, 0, 0, isInput() ? 1 : 0, 0);
        assertTrue(runner.getLogger().getErrorMessages().stream().anyMatch(logMessage ->
                logMessage.getMsg().contains("Could not query documents") && logMessage.getThrowable().getMessage().equals("Query using pit/search_after must contain a \"sort\" field")));
        reset(runner);

        // test scroll without sort (should succeed)
        runner.setProperty(AbstractPaginatedJsonQueryElasticsearch.PAGINATION_TYPE, PaginationType.SCROLL.getValue());
        runMultiple(runner);
        AbstractJsonQueryElasticsearchTest.testCounts(runner, isInput() ? 1 : 0, 1, 0, 0);
    }

    @Test
    public void testScroll() throws Exception {
        testPagination(PaginationType.SCROLL);
    }

    @Test
    public void testPit() throws Exception {
        testPagination(PaginationType.POINT_IN_TIME);
    }

    @Test
    public void testSearchAfter() throws Exception {
        testPagination(PaginationType.SEARCH_AFTER);
    }

    private void testPagination(final PaginationType paginationType) throws Exception {
            final TestRunner runner = createRunner(false);
            final TestElasticsearchClientService service = AbstractJsonQueryElasticsearchTest.getService(runner);
            service.setMaxPages(2);
            runner.setProperty(AbstractPaginatedJsonQueryElasticsearch.PAGINATION_TYPE, paginationType.getValue());
            runner.setProperty(AbstractJsonQueryElasticsearch.QUERY, matchAllWithSortByMsgWithSizeQuery);
            // Tests flowfile per page, hits splitting and hits combined
            for (final ResultOutputStrategy resultOutputStrategy : ResultOutputStrategy.values()) {
                runner.setProperty(AbstractPaginatedJsonQueryElasticsearch.SEARCH_RESULTS_SPLIT, resultOutputStrategy.getValue());
                for (int iteration = 1; iteration < 4; iteration++) {
                    // Check that changing OUTPUT_NO_HITS doesn't have any adverse effects on pagination
                    runner.setProperty(AbstractJsonQueryElasticsearch.OUTPUT_NO_HITS, String.valueOf(iteration % 2 > 0).toLowerCase());
                    runOnce(runner);
                    validatePagination(runner, resultOutputStrategy, paginationType, iteration);
                    if (ResultOutputStrategy.PER_QUERY.equals(resultOutputStrategy)) {
                        break;
                    }
                    runner.clearTransferState();
                    if (!isStateUsed()) {
                        // reset PaginatedJsonQueryElasticsearch to re-run the query with different OUTPUT_NO_HITS setting
                        reset(runner);
                    }
                }
                reset(runner);
            }
    }

    abstract void validatePagination(final TestRunner runner, final ResultOutputStrategy resultOutputStrategy, final PaginationType paginationType, int iteration) throws Exception;

    private void runMultiple(final TestRunner runner) {
        if (isInput()) {
            // with an input FlowFile, the processor still only triggers a single time and completes all processing
            runOnce(runner);
        } else {
            // with no input, the processor executes multiple times and tracks progress using STATE.Local
            runner.setIncomingConnection(false);
            runner.run(2, true, true);
        }
    }

    private void assertSendEvent(final TestRunner runner, final MockFlowFile input) {
        if (isInput()) {
            assertEquals(1L, runner.getProvenanceEvents().stream().filter(event ->
                    ProvenanceEventType.SEND.equals(event.getEventType()) && event.getAttribute("uuid").equals(input.getAttribute("uuid"))).count());
        } else {
            assertEquals(0L, runner.getProvenanceEvents().stream().filter(event ->
                    ProvenanceEventType.SEND.equals(event.getEventType())).count());
        }
    }

    @Test
    public void testEmptyHitsFlowFileIsProducedForEachResultSplitSetup() {
        final TestRunner runner = createRunner(false);
        final TestElasticsearchClientService service = AbstractJsonQueryElasticsearchTest.getService(runner);
        runner.setProperty(AbstractJsonQueryElasticsearch.QUERY, matchAllWithSortByMessage);
        runner.setProperty(AbstractPaginatedJsonQueryElasticsearch.OUTPUT_NO_HITS, "true");
        service.setMaxPages(0);

        for (final PaginationType paginationType : PaginationType.values()) {
            runner.setProperty(AbstractPaginatedJsonQueryElasticsearch.PAGINATION_TYPE, paginationType.getValue());

            for (final ResultOutputStrategy resultOutputStrategy : ResultOutputStrategy.values()) {
                // test that an empty flow file is produced for a per query setup
                runner.setProperty(AbstractPaginatedJsonQueryElasticsearch.SEARCH_RESULTS_SPLIT, resultOutputStrategy.getValue());
                runOnce(runner);
                AbstractJsonQueryElasticsearchTest.testCounts(runner, isInput() ? 1 : 0, 1, 0, 0);

                runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).get(0).assertAttributeEquals("hit.count", "0");
                runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).get(0).assertAttributeEquals("page.number", "1");
                assertEquals(0, runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).get(0).getSize());
                reset(runner);
            }
        }
    }
}
