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

import org.apache.nifi.processors.elasticsearch.api.PaginationType;
import org.apache.nifi.processors.elasticsearch.api.ResultOutputStrategy;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.temporal.ValueRange;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PaginatedJsonQueryElasticsearchTest extends AbstractPaginatedJsonQueryElasticsearchTest {
    public AbstractPaginatedJsonQueryElasticsearch getProcessor() {
        return new PaginatedJsonQueryElasticsearch();
    }

    public boolean isStateUsed() {
        return false;
    }

    public boolean isInput() {
        return true;
    }

    public static void validatePagination(final TestRunner runner, final ResultOutputStrategy resultOutputStrategy) {
        switch (resultOutputStrategy) {
            case PER_RESPONSE:
                AbstractJsonQueryElasticsearchTest.testCounts(runner, 1, 2, 0, 0);
                int page = 1;
                for (MockFlowFile hit : runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS)) {
                    hit.assertAttributeEquals("hit.count", "10");
                    hit.assertAttributeEquals("page.number", Integer.toString(page++));
                }
                break;
            case PER_QUERY:
                AbstractJsonQueryElasticsearchTest.testCounts(runner, 1, 1, 0, 0);
                runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).get(0).assertAttributeEquals("hit.count", "20");
                runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).get(0).assertAttributeEquals("page.number", "2");
                assertEquals(20, runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).get(0).getContent().split("\n").length);
                break;
            case PER_HIT:
                AbstractJsonQueryElasticsearchTest.testCounts(runner, 1, 20, 0, 0);
                long count = 1;
                ValueRange firstPage = ValueRange.of(1, 10);
                for (MockFlowFile hit : runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS)) {
                    hit.assertAttributeEquals("hit.count", "1");
                    // 10 hits per page, so first 10 flow files should be page.number 1, the rest page.number 2
                    hit.assertAttributeEquals("page.number", firstPage.isValidValue(count) ? "1" : "2");
                    count++;
                }
                break;
            default:
                throw new IllegalArgumentException("Unknown ResultOutputStrategy value: " + resultOutputStrategy);
        }
    }

    @Override
    public void testPagination(final PaginationType paginationType) {
        final TestRunner runner = createRunner(false);
        final TestElasticsearchClientService service = AbstractJsonQueryElasticsearchTest.getService(runner);
        service.setMaxPages(2);

        try {
            runner.setProperty(AbstractJsonQueryElasticsearch.QUERY,
                    Files.readString(Paths.get("src/test/resources/AbstractPaginatedJsonQueryElasticsearchTest/matchAllWithSortByMsgQueryWithSize.json")));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        for (final ResultOutputStrategy resultOutputStrategy : ResultOutputStrategy.values()) {
            runner.setProperty(AbstractPaginatedJsonQueryElasticsearch.SEARCH_RESULTS_SPLIT, resultOutputStrategy.getValue());

            runOnce(runner);
            validatePagination(runner, resultOutputStrategy);
            runner.getStateManager().assertStateNotSet();
            reset(runner);

            // Check that OUTPUT_NO_HITS true doesn't have any adverse effects on pagination
            runner.setProperty(AbstractJsonQueryElasticsearch.OUTPUT_NO_HITS, "true");
            runOnce(runner);
            validatePagination(runner, resultOutputStrategy);
            // Unset OUTPUT_NO_HITS
            runner.setProperty(AbstractJsonQueryElasticsearch.OUTPUT_NO_HITS, "false");
            reset(runner);
        }
    }
}
