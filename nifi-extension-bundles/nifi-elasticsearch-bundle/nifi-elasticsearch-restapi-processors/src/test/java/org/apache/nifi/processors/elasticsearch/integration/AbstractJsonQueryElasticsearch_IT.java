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
package org.apache.nifi.processors.elasticsearch.integration;

import org.apache.nifi.processors.elasticsearch.ElasticsearchRestProcessor;
import org.apache.nifi.processors.elasticsearch.api.QueryDefinitionType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public abstract class AbstractJsonQueryElasticsearch_IT extends AbstractElasticsearchRestProcessor_IT {
    @BeforeEach
    public void setUp() {
        // set Query Definition Style and default Query Clause for all tests, allowing for ConsumeElasticsearch test override
        runner.setProperty(ElasticsearchRestProcessor.QUERY_DEFINITION_STYLE, QueryDefinitionType.BUILD_QUERY);
        runner.setProperty(ElasticsearchRestProcessor.QUERY_CLAUSE, "{\"match_all\":{}}");
    }

    @Test
    void testVerifyBuildQueryValidAggregations() {
        runner.removeProperty(ElasticsearchRestProcessor.QUERY_CLAUSE);
        runner.setProperty(ElasticsearchRestProcessor.AGGREGATIONS, "{\"messages\":{\"terms\":{\"field\":\"msg\"}}}");

        assertQueryVerify(3, 1);
    }

    @Test
    void testVerifyBuildQueryValidQueryAndAggregations() {
        runner.setProperty(ElasticsearchRestProcessor.AGGREGATIONS, "{\"messages\":{\"terms\":{\"field\":\"msg\"}}}");

        assertQueryVerify(3, 1);
    }

    @Test
    void testVerifyBuildQueryValidAll() {
        runner.setProperty(ElasticsearchRestProcessor.AGGREGATIONS, "{\"messages\":{\"terms\":{\"field\":\"msg\"}}}");
        if (getElasticMajorVersion() > 6) {
            // "fields" didn't exist before Elasticsearch 7
            runner.setProperty(ElasticsearchRestProcessor.FIELDS, "[\"msg\", \"test*\"]");
        }
        runner.setProperty(ElasticsearchRestProcessor.SCRIPT_FIELDS, "{\"test1\": {\"script\": {\"lang\": \"painless\", \"source\": \"doc['num'].value * 2\"}}}");

        assertQueryVerify(3, 1);
    }

    @Test
    void testVerifyBuildQueryValidSize() {
        runner.setProperty(ElasticsearchRestProcessor.SIZE, "1");

        // looks a bit odd, but 3 documents match (hits.total); although only 1 is returned (size)
        assertQueryVerify(3, 0);
    }

    @Test
    void testVerifyBuildQueryValidSingleSort() {
        runner.setProperty(ElasticsearchRestProcessor.SORT, "{\"msg\":\"asc\"}");

        assertQueryVerify(3, 0);
    }

    @Test
    void testVerifyBuildQueryValidMultipleSorts() {
        runner.setProperty(ElasticsearchRestProcessor.SORT, "[{\"msg\":\"desc\"},{\"num\":\"asc\"}]");

        assertQueryVerify(3, 0);
    }
}
