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
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.BeforeAll;

import java.time.temporal.ValueRange;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PaginatedJsonQueryElasticsearchTest extends AbstractPaginatedJsonQueryElasticsearchTest {
    @BeforeAll
    public static void setUpBeforeClass() throws Exception {
        AbstractPaginatedJsonQueryElasticsearchTest.setUpBeforeClass();
    }
    AbstractPaginatedJsonQueryElasticsearch getProcessor() {
        return new PaginatedJsonQueryElasticsearch();
    }

    Scope getStateScope() {
        return null;
    }

    boolean isInput() {
        return true;
    }

    @Override
    void validatePagination(final TestRunner runner, final ResultOutputStrategy resultOutputStrategy, final PaginationType paginationType, final int iteration) {
        runner.getStateManager().assertStateNotSet();
        switch (resultOutputStrategy) {
            case PER_RESPONSE:
                AbstractJsonQueryElasticsearchTest.testCounts(runner, 1, 2, 0, 0);
                for(int page = 1; page <= 2; page++) {
                    final MockFlowFile hit = runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).get(page - 1);
                    hit.assertAttributeEquals("hit.count", "10");
                    hit.assertAttributeEquals("page.number", Integer.toString(page));
                }
                break;
            case PER_QUERY:
                final int expectedHits = 20;
                AbstractJsonQueryElasticsearchTest.testCounts(runner, 1, 1, 0, 0);
                runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).getFirst().assertAttributeEquals("hit.count", Integer.toString(expectedHits));
                runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).getFirst().assertAttributeEquals("page.number", "2");
                assertEquals(expectedHits, runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).getFirst().getContent().split("\n").length);
                break;
            case PER_HIT:
                AbstractJsonQueryElasticsearchTest.testCounts(runner, 1, 20, 0, 0);
                long count = 1;
                final ValueRange firstPage = ValueRange.of(1, 10);
                for (final MockFlowFile hit : runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS)) {
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
}
