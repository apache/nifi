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
import org.apache.nifi.processors.elasticsearch.api.AggregationResultsFormat;
import org.apache.nifi.processors.elasticsearch.api.ResultOutputStrategy;
import org.apache.nifi.processors.elasticsearch.api.SearchResultsFormat;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class AbstractJsonQueryElasticsearchTest<P extends AbstractJsonQueryElasticsearch> {
    private static final String TEST_DIR = "src/test/resources/AbstractJsonQueryElasticsearchTest";
    private static final String TEST_COMMON_DIR = "src/test/resources/common";
    private static final String INDEX_NAME = "messages";
    protected static String matchAllQuery;
    protected static String matchAllAggregationWithDefaultTermsQuery;

    public abstract P getProcessor();

    public abstract boolean isStateUsed();

    public abstract boolean isInput();

    @BeforeAll
    public static void setUpBeforeClass() throws Exception {
        matchAllQuery = Files.readString(Paths.get(TEST_COMMON_DIR, "matchAllQuery.json"));
        matchAllAggregationWithDefaultTermsQuery = Files.readString(Paths.get(TEST_DIR,"matchAllAggregationWithDefaultTermsQuery.json"));
    }

    @Test
    public void testMandatoryProperties() {
        final TestRunner runner = createRunner(false);
        runner.removeProperty(AbstractJsonQueryElasticsearch.CLIENT_SERVICE);
        runner.removeProperty(AbstractJsonQueryElasticsearch.INDEX);
        runner.removeProperty(AbstractJsonQueryElasticsearch.TYPE);
        runner.removeProperty(AbstractJsonQueryElasticsearch.QUERY);
        runner.removeProperty(AbstractJsonQueryElasticsearch.QUERY_ATTRIBUTE);
        runner.removeProperty(AbstractJsonQueryElasticsearch.AGGREGATION_RESULTS_SPLIT);
        runner.removeProperty(AbstractJsonQueryElasticsearch.SEARCH_RESULTS_SPLIT);
        runner.removeProperty(AbstractJsonQueryElasticsearch.OUTPUT_NO_HITS);

        final AssertionError assertionError = assertThrows(AssertionError.class, runner::run);
        if (getProcessor() instanceof SearchElasticsearch) {
            assertEquals(String.format("Processor has 3 validation failures:\n" + "'%s' is invalid because %s is required\n"
                            + "'%s' is invalid because %s is required\n" + "'%s' is invalid because %s is required\n",
                    AbstractJsonQueryElasticsearch.QUERY.getDisplayName(), AbstractJsonQueryElasticsearch.QUERY.getDisplayName(),
                    AbstractJsonQueryElasticsearch.INDEX.getDisplayName(), AbstractJsonQueryElasticsearch.INDEX.getDisplayName(),
                    AbstractJsonQueryElasticsearch.CLIENT_SERVICE.getDisplayName(), AbstractJsonQueryElasticsearch.CLIENT_SERVICE.getDisplayName()), assertionError.getMessage());
        } else {
            assertEquals(String.format("Processor has 2 validation failures:\n" + "'%s' is invalid because %s is required\n" + "'%s' is invalid because %s is required\n",
                    AbstractJsonQueryElasticsearch.INDEX.getDisplayName(), AbstractJsonQueryElasticsearch.INDEX.getDisplayName(),
                    AbstractJsonQueryElasticsearch.CLIENT_SERVICE.getDisplayName(), AbstractJsonQueryElasticsearch.CLIENT_SERVICE.getDisplayName()), assertionError.getMessage());
        }
    }

    @Test
    public void testInvalidProperties() {
        final TestRunner runner = createRunner(false);
        runner.setProperty(AbstractJsonQueryElasticsearch.CLIENT_SERVICE, "not-a-service");
        runner.setProperty(AbstractJsonQueryElasticsearch.INDEX, "");
        runner.setProperty(AbstractJsonQueryElasticsearch.TYPE, "");
        runner.setProperty(AbstractJsonQueryElasticsearch.QUERY, "not-json");
        runner.setProperty(AbstractJsonQueryElasticsearch.AGGREGATION_RESULTS_SPLIT, "not-enum");
        runner.setProperty(AbstractJsonQueryElasticsearch.SEARCH_RESULTS_SPLIT, "not-enum2");
        runner.setProperty(AbstractJsonQueryElasticsearch.OUTPUT_NO_HITS, "not-boolean");

        final String nonPaginatedResultOutputStrategies =
                ResultOutputStrategy.getNonPaginatedResponseOutputStrategies()
                        .stream().map(ResultOutputStrategy::getValue)
                        .collect(Collectors.joining(", "));
        final String expectedAllowedSplitHits = getProcessor() instanceof AbstractPaginatedJsonQueryElasticsearch ? Arrays.stream(ResultOutputStrategy.values())
                .map(ResultOutputStrategy::getValue)
                .collect(Collectors.joining(", ")) : nonPaginatedResultOutputStrategies;

        final AssertionError assertionError = assertThrows(AssertionError.class, runner::run);
        String expected = String.format("Processor has 8 validation failures:\n" +
                        "'%s' validated against 'not-json' is invalid because %s is not a valid JSON representation due to Unrecognized token 'not': was expecting" +
                        " (JSON String, Number, Array, Object or token 'null', 'true' or 'false')\n" +
                        " at [Source: (String)\"not-json\"; line: 1, column: 4]\n" + "'%s' validated against '' is invalid because %s cannot be empty\n" +
                        "'%s' validated against '' is invalid because %s cannot be empty\n" + "'%s' validated against 'not-a-service' is invalid because" +
                        " Property references a Controller Service that does not exist\n" + "'%s' validated against 'not-enum2' is invalid because Given value not found in allowed set '%s'\n" +
                        "'%s' validated against 'not-enum' is invalid because Given value not found in allowed set '%s'\n" +
                        "'%s' validated against 'not-boolean' is invalid because Given value not found in allowed set 'true, false'\n" +
                        "'%s' validated against 'not-a-service' is invalid because Invalid Controller Service: not-a-service is not a valid Controller Service Identifier\n",
                AbstractJsonQueryElasticsearch.QUERY.getName(), AbstractJsonQueryElasticsearch.QUERY.getName(), AbstractJsonQueryElasticsearch.INDEX.getName(),
                AbstractJsonQueryElasticsearch.INDEX.getName(), AbstractJsonQueryElasticsearch.TYPE.getName(), AbstractJsonQueryElasticsearch.TYPE.getName(),
                AbstractJsonQueryElasticsearch.CLIENT_SERVICE.getDisplayName(), AbstractJsonQueryElasticsearch.SEARCH_RESULTS_SPLIT.getName(), expectedAllowedSplitHits,
                AbstractJsonQueryElasticsearch.AGGREGATION_RESULTS_SPLIT.getName(), nonPaginatedResultOutputStrategies, AbstractJsonQueryElasticsearch.OUTPUT_NO_HITS.getName(),
                AbstractJsonQueryElasticsearch.CLIENT_SERVICE.getDisplayName());
        assertEquals(expected, assertionError.getMessage());
    }

    @Test
    public void testBasicQuery() {
        // test hits (no splitting) - full hit format
        final TestRunner runner = createRunner(false);
        runner.setProperty(AbstractJsonQueryElasticsearch.QUERY, matchAllQuery);
        runner.setProperty(AbstractJsonQueryElasticsearch.SEARCH_RESULTS_FORMAT, SearchResultsFormat.FULL.getValue());
        runOnce(runner);
        testCounts(runner, isInput() ? 1 : 0, 1, 0, 0);
        final MockFlowFile hits = runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).get(0);
        hits.assertAttributeEquals("hit.count", "10");
        assertOutputContent(hits.getContent(), 10, false);
        final List<Map<String, Object>> result = JsonUtils.readListOfMaps(hits.getContent());
        result.forEach(hit -> {
            assertFalse(hit.isEmpty());
            assertTrue(hit.containsKey("_source"));
            assertTrue(hit.containsKey("_index"));
        });
        assertEquals(1L, runner.getProvenanceEvents().stream()
                .filter(pe -> ProvenanceEventType.RECEIVE.equals(pe.getEventType()) && pe.getAttribute("uuid").equals(hits.getAttribute("uuid")))
                .count());
        reset(runner);

        // test splitting hits - _source only format
        runner.setProperty(AbstractJsonQueryElasticsearch.SEARCH_RESULTS_SPLIT, ResultOutputStrategy.PER_HIT.getValue());
        runner.setProperty(AbstractJsonQueryElasticsearch.SEARCH_RESULTS_FORMAT, SearchResultsFormat.SOURCE_ONLY.getValue());
        runOnce(runner);
        testCounts(runner, isInput() ? 1 : 0, 10, 0, 0);

        runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).forEach(hit -> {
            hit.assertAttributeEquals("hit.count", "1");
            assertOutputContent(hit.getContent(), 1, false);
            final Map<String, Object> h = JsonUtils.readMap(hit.getContent());
            assertFalse(h.isEmpty());
            assertFalse(h.containsKey("_source"));
            assertFalse(h.containsKey("_index"));
            // should be the _source content only
            assertTrue(h.containsKey("msg"));

            assertEquals(1L,
                    runner.getProvenanceEvents().stream()
                            .filter(pe ->
                                    pe.getEventType() == ProvenanceEventType.RECEIVE && pe.getAttribute("uuid").equals(hit.getAttribute("uuid")))
                            .count());
        });
        reset(runner);

        // test splitting hits - metadata only format
        runner.setProperty(AbstractJsonQueryElasticsearch.SEARCH_RESULTS_SPLIT, ResultOutputStrategy.PER_HIT.getValue());
        runner.setProperty(AbstractJsonQueryElasticsearch.SEARCH_RESULTS_FORMAT, SearchResultsFormat.METADATA_ONLY.getValue());
        runOnce(runner);
        testCounts(runner, isInput() ? 1 : 0, 10, 0, 0);
        runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).forEach(hit -> {
            hit.assertAttributeEquals("hit.count", "1");
            assertOutputContent(hit.getContent(), 1, false);
            final Map<String, Object> h = JsonUtils.readMap(hit.getContent());
            assertFalse(h.isEmpty());
            assertFalse(h.containsKey("_source"));
            assertTrue(h.containsKey("_index"));
            assertEquals(1L,
                    runner.getProvenanceEvents().stream()
                            .filter(pe ->
                                    ProvenanceEventType.RECEIVE.equals(pe.getEventType()) && pe.getAttribute("uuid").equals(hit.getAttribute("uuid")))
                            .count());
        });
    }

    @Test
    public void testNoHits() {
        // test no hits (no output)
        final TestRunner runner = createRunner(false);
        final TestElasticsearchClientService service = getService(runner);
        service.setMaxPages(0);
        runner.setProperty(AbstractJsonQueryElasticsearch.QUERY, matchAllQuery);
        runner.setProperty(AbstractJsonQueryElasticsearch.OUTPUT_NO_HITS, "false");
        runOnce(runner);
        testCounts(runner, isInput() ? 1 : 0, 0, 0, 0);
        assertEquals(0L, runner.getProvenanceEvents().stream()
                .filter(pe -> ProvenanceEventType.RECEIVE.equals(pe.getEventType()))
                .count());
        reset(runner);

        // test not hits (with output)
        runner.setProperty(AbstractJsonQueryElasticsearch.OUTPUT_NO_HITS, "true");
        runOnce(runner);
        testCounts(runner, isInput() ? 1 : 0, 1, 0, 0);

        runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).forEach(
                hit -> {
                    hit.assertAttributeEquals("hit.count", "0");
                    assertOutputContent(hit.getContent(), 0, false);
                    assertEquals(1L, runner.getProvenanceEvents().stream()
                            .filter(pe ->
                                    ProvenanceEventType.RECEIVE.equals(pe.getEventType()) && pe.getAttribute("uuid").equals(hit.getAttribute("uuid")))
                            .count());
                });
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testAggregationsFullFormat() {
        final TestRunner runner = createRunner(true);
        runner.setProperty(AbstractJsonQueryElasticsearch.QUERY, matchAllAggregationWithDefaultTermsQuery);
        runner.setProperty(AbstractJsonQueryElasticsearch.AGGREGATION_RESULTS_FORMAT, AggregationResultsFormat.FULL.getValue());
        runOnce(runner);
        testCounts(runner, isInput() ? 1 : 0, 1, 0, 1);
        runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).get(0).assertAttributeEquals("hit.count", "10");
        MockFlowFile aggregations = runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_AGGREGATIONS).get(0);
        aggregations.assertAttributeNotExists("aggregation.number");
        aggregations.assertAttributeNotExists("aggregation.name");
        // count == 1 because aggregations is a single Map rather than a List of Maps, even when there are multiple aggs
        assertOutputContent(aggregations.getContent(), 1, false);
        final Map<String, Object> agg = JsonUtils.readMap(aggregations.getContent());
        // agg Map of 2 Maps (buckets and metadata)
        assertEquals(2, agg.size());
        agg.keySet().forEach(aggName -> {
            final Map<String, Object> termAgg = (Map<String, Object>) agg.get(aggName);
            assertInstanceOf(List.class, termAgg.get("buckets"));
            assertTrue(termAgg.containsKey("doc_count_error_upper_bound"));
        });
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testWithQueryParameterNoIncomingConnectionAndBucketsAggregationFormat() {
        final TestRunner runner = createRunner(true);
        runner.setProperty(AbstractJsonQueryElasticsearch.QUERY, matchAllAggregationWithDefaultTermsQuery);
        runner.setIncomingConnection(false);
        runner.setProperty(AbstractJsonQueryElasticsearch.AGGREGATION_RESULTS_FORMAT, AggregationResultsFormat.BUCKETS_ONLY.getValue());
        runner.run(1, true, true);
        testCounts(runner, 0, 1, 0, 1);
        runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).get(0).assertAttributeEquals("hit.count", "10");
        final MockFlowFile singleAgg = runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_AGGREGATIONS).get(0);
        singleAgg.assertAttributeNotExists("aggregation.number");
        singleAgg.assertAttributeNotExists("aggregation.name");
        final Map<String, Object> agg2 = JsonUtils.readMap(singleAgg.getContent());
        // agg Map of 2 Lists (bucket contents only)
        assertEquals(2, agg2.size());
        agg2.keySet().forEach(aggName -> {
            final List<Map<String, Object>> termAgg = (List<Map<String, Object>>) agg2.get(aggName);
            assertEquals(5, termAgg.size());
            termAgg.forEach(a -> {
                assertTrue(a.containsKey("key"));
                assertTrue(a.containsKey("doc_count"));
            });
        });
    }

    @Test
    public void testSplittingAggregationsMetadataOnlyFormat() {
        final TestRunner runner = createRunner(true);
        runner.setProperty(AbstractJsonQueryElasticsearch.QUERY, matchAllAggregationWithDefaultTermsQuery);
        runner.setProperty(AbstractJsonQueryElasticsearch.AGGREGATION_RESULTS_SPLIT, ResultOutputStrategy.PER_HIT.getValue());
        runner.setProperty(AbstractJsonQueryElasticsearch.AGGREGATION_RESULTS_FORMAT, AggregationResultsFormat.METADATA_ONLY.getValue());
        runOnce(runner);
        testCounts(runner, isInput() ? 1 : 0, 1, 0, 2);
        runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).get(0).assertAttributeEquals("hit.count", "10");
        int a = 0;
        for (MockFlowFile termAgg : runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_AGGREGATIONS)) {
            termAgg.assertAttributeEquals("aggregation.name", a == 0 ? "term_agg" : "term_agg2");
            termAgg.assertAttributeEquals("aggregation.number", Integer.toString(++a));
            assertOutputContent(termAgg.getContent(), 1, false);

            Map<String, Object> aggContent = JsonUtils.readMap(termAgg.getContent());
            // agg Map (metadata, no buckets)
            assertTrue(aggContent.containsKey("doc_count_error_upper_bound"));
            assertFalse(aggContent.containsKey("buckets"));
        }
    }
    @Test
    public void testAggregationsUsingExpressionLanguage() throws Exception {
        final TestRunner runner = createRunner(true);
        String query = Files.readString(Paths.get(TEST_DIR, "matchAllAggregationWithDefaultTermsInExpressionLanguageQuery.json"));
        runner.setVariable("fieldValue", "msg");
        runner.setVariable("es.index", INDEX_NAME);
        runner.setVariable("es.type", "msg");
        runner.setProperty(AbstractJsonQueryElasticsearch.QUERY, query);
        runner.setProperty(AbstractJsonQueryElasticsearch.INDEX, "${es.index}");
        runner.setProperty(AbstractJsonQueryElasticsearch.TYPE, "${es.type}");
        runner.setProperty(AbstractJsonQueryElasticsearch.AGGREGATION_RESULTS_SPLIT, ResultOutputStrategy.PER_HIT.getValue());
        runner.setProperty(AbstractJsonQueryElasticsearch.AGGREGATION_RESULTS_FORMAT, AggregationResultsFormat.FULL.getValue());
        runOnce(runner);
        testCounts(runner, isInput() ? 1 : 0, 1, 0, 2);
        runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).get(0).assertAttributeEquals("hit.count", "10");
        int a = 0;
        for (MockFlowFile termAgg : runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_AGGREGATIONS)) {
            termAgg.assertAttributeEquals("aggregation.name", a == 0 ? "term_agg" : "term_agg2");
            termAgg.assertAttributeEquals("aggregation.number", Integer.toString(++a));
            assertOutputContent(termAgg.getContent(), 1, false);
        }
    }

    @Test
    public void testErrorDuringSearch() {
        final TestRunner runner = createRunner(true);
        getService(runner).setThrowErrorInSearch(true);
        runner.setProperty(AbstractJsonQueryElasticsearch.QUERY, matchAllAggregationWithDefaultTermsQuery);
        runOnce(runner);
        testCounts(runner, 0, 0, isInput() ? 1 : 0, 0);
    }

    @Test
    public void testQueryAttribute() {
        String query = matchAllAggregationWithDefaultTermsQuery;
        final String queryAttr = "es.query";

        final TestRunner runner = createRunner(true);
        runner.setProperty(AbstractJsonQueryElasticsearch.QUERY, query);
        runner.setProperty(AbstractJsonQueryElasticsearch.QUERY_ATTRIBUTE, queryAttr);
        runOnce(runner);
        testCounts(runner, isInput() ? 1 : 0, 1, 0, 1);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_AGGREGATIONS);
        flowFiles.addAll(runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS));

        for (final MockFlowFile mockFlowFile : flowFiles) {
            final String attr = mockFlowFile.getAttribute(queryAttr);
            assertNotNull(attr, "Missing query attribute");
            assertEquals(query, attr, "Query had wrong value.");
        }
    }

    @Test
    public void testInputHandling() {
        final TestRunner runner = createRunner(false);
        runner.setProperty(AbstractJsonQueryElasticsearch.QUERY, matchAllQuery);

        runner.setIncomingConnection(true);
        runner.run();
        testCounts(runner, 0, 0, 0, 0);
        reset(runner);

        runner.setIncomingConnection(false);
        runner.run();
        testCounts(runner, 0, 1, 0, 0);
    }

    @Test
    public void testRequestParameters() {
        final TestRunner runner = createRunner(false);
        runner.setProperty(AbstractJsonQueryElasticsearch.QUERY, matchAllQuery);
        runner.setProperty("refresh", "true");
        runner.setProperty("slices", "${slices}");
        runner.setVariable("slices", "auto");

        runOnce(runner);

        final TestElasticsearchClientService service = getService(runner);
        if (getProcessor() instanceof SearchElasticsearch || getProcessor() instanceof PaginatedJsonQueryElasticsearch) {
            assertEquals(3, service.getRequestParameters().size());
            assertEquals("600s", service.getRequestParameters().get("scroll"));
        } else {
            assertEquals(2, service.getRequestParameters().size());
        }

        assertEquals("true", service.getRequestParameters().get("refresh"));
        assertEquals("auto", service.getRequestParameters().get("slices"));
    }

    public static void testCounts(TestRunner runner, int original, int hits, int failure, int aggregations) {
        runner.assertTransferCount(AbstractJsonQueryElasticsearch.REL_ORIGINAL, original);
        runner.assertTransferCount(AbstractJsonQueryElasticsearch.REL_HITS, hits);
        runner.assertTransferCount(AbstractJsonQueryElasticsearch.REL_FAILURE, failure);
        runner.assertTransferCount(AbstractJsonQueryElasticsearch.REL_AGGREGATIONS, aggregations);
        runner.assertTransferCount(AbstractJsonQueryElasticsearch.REL_RETRY, 0);
    }

    public static void assertOutputContent(final String content, final int count, final boolean ndjson) {
        if (ndjson) {
            assertEquals(count, (content.split("\n").length));
        } else {
            if (count == 0) {
                assertEquals("", content);
            } else if (count == 1) {
                assertTrue(content.startsWith("{") && content.endsWith("}"));
            } else {
                assertTrue(content.startsWith("[") && content.endsWith("]"));
            }
        }
    }

    public TestRunner createRunner(final boolean returnAggs) {
        final P processor = getProcessor();
        final TestRunner runner = TestRunners.newTestRunner(processor);
        final TestElasticsearchClientService service = new TestElasticsearchClientService(returnAggs);
        try {
            runner.addControllerService("esService", service);
        } catch (InitializationException e) {
            throw new RuntimeException(e);
        }
        runner.enableControllerService(service);
        runner.setProperty(AbstractJsonQueryElasticsearch.CLIENT_SERVICE, "esService");
        runner.setProperty(AbstractJsonQueryElasticsearch.INDEX, INDEX_NAME);
        runner.setProperty(AbstractJsonQueryElasticsearch.TYPE, "message");
        runner.setValidateExpressionUsage(true);

        return runner;
    }

    public MockFlowFile runOnce(final TestRunner runner) {
        final MockFlowFile ff;
        if (isInput()) {
            runner.setIncomingConnection(true);
            ff = runner.enqueue("test");
        } else {
            runner.setIncomingConnection(false);
            ff = null;
        }

        runner.run(1, true, true);
        return ff;
    }

    public static TestElasticsearchClientService getService(final TestRunner runner) {
        return runner.getControllerService("esService", TestElasticsearchClientService.class);
    }

    public void reset(final TestRunner runner) {
        runner.clearProvenanceEvents();
        runner.clearTransferState();
        if (isStateUsed()) {
            try {
                runner.getStateManager().clear(Scope.LOCAL);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        getService(runner).resetPageCount();
    }
}
