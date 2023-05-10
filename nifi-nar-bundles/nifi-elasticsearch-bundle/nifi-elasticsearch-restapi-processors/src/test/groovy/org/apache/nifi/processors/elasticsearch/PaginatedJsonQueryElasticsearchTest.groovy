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

import org.apache.nifi.processors.elasticsearch.api.PaginationType
import org.apache.nifi.processors.elasticsearch.api.ResultOutputStrategy
import org.apache.nifi.util.TestRunner

import static groovy.json.JsonOutput.prettyPrint
import static groovy.json.JsonOutput.toJson
import static org.hamcrest.CoreMatchers.is
import static org.hamcrest.MatcherAssert.assertThat

class PaginatedJsonQueryElasticsearchTest extends AbstractPaginatedJsonQueryElasticsearchTest {
    AbstractPaginatedJsonQueryElasticsearch getProcessor() {
        return new PaginatedJsonQueryElasticsearch()
    }

    boolean isStateUsed() {
        return false
    }

    boolean isInput() {
        return true
    }

    static void validatePagination(final TestRunner runner, final ResultOutputStrategy resultOutputStrategy) {
        switch (resultOutputStrategy) {
            case ResultOutputStrategy.PER_RESPONSE:
                testCounts(runner, 1, 2, 0, 0)
                int page = 1
                runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).forEach(
                        { hit ->
                            hit.assertAttributeEquals("hit.count", "10")
                            hit.assertAttributeEquals("page.number", Integer.toString(page++))
                        }
                )
                break
            case ResultOutputStrategy.PER_QUERY:
                testCounts(runner, 1, 1, 0, 0)
                runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).get(0).assertAttributeEquals("hit.count", "20")
                // the "last" page.number is used, so 2 here because there were 2 pages of hits
                runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).get(0).assertAttributeEquals("page.number", "2")
                assertThat(
                        runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).get(0).getContent().split("\n").length,
                        is(20)
                )
                break
            case ResultOutputStrategy.PER_HIT:
                testCounts(runner, 1, 20, 0, 0)
                int count = 0
                runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).forEach(
                        { hit ->
                            hit.assertAttributeEquals("hit.count", "1")
                            // 10 hits per page, so first 10 flow files should be page.number 1, the rest page.number 2
                            hit.assertAttributeEquals("page.number", Integer.toString(Math.ceil(++count / 10) as int))
                        }
                )
                break
            default:
                throw new IllegalArgumentException("Unknown ResultOutputStrategy value: " + resultOutputStrategy)
        }
    }

    void testPagination(final PaginationType paginationType) {
        final TestRunner runner = createRunner(false)
        final TestElasticsearchClientService service = getService(runner)
        service.setMaxPages(2)
        runner.setProperty(AbstractJsonQueryElasticsearch.QUERY, prettyPrint(toJson([size: 10, sort: [msg: "desc"], query: [match_all: [:]]])))

        for (final ResultOutputStrategy resultOutputStrategy : ResultOutputStrategy.values()) {
            runner.setProperty(AbstractPaginatedJsonQueryElasticsearch.SEARCH_RESULTS_SPLIT, resultOutputStrategy.getValue())

            runOnce(runner)
            validatePagination(runner, resultOutputStrategy)
            runner.getStateManager().assertStateNotSet()
            reset(runner)

            // Check that OUTPUT_NO_HITS true doesn't have any adverse effects on pagination
            runner.setProperty(AbstractJsonQueryElasticsearch.OUTPUT_NO_HITS, "true")
            runOnce(runner)
            validatePagination(runner, resultOutputStrategy)
            // Unset OUTPUT_NO_HITS
            runner.setProperty(AbstractJsonQueryElasticsearch.OUTPUT_NO_HITS, "false")
            reset(runner)
        }
    }
}