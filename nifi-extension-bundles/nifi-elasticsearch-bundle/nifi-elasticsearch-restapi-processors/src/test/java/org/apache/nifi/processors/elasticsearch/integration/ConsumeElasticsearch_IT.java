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

import org.apache.nifi.processors.elasticsearch.ConsumeElasticsearch;
import org.apache.nifi.processors.elasticsearch.ElasticsearchRestProcessor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ConsumeElasticsearch_IT extends AbstractJsonQueryElasticsearch_IT {
    private static final String RANGE_FIELD = "num";
    private static final String RANGE_SORT_ORDER = "asc";

    ElasticsearchRestProcessor getProcessor() {
        return new ConsumeElasticsearch();
    }

    @BeforeEach
    public void setUp() {
        // Range Field is required; no Initial Value should result in a default "match_all" query being constructed
        runner.setProperty(ConsumeElasticsearch.RANGE_FIELD, RANGE_FIELD);
        runner.setProperty(ConsumeElasticsearch.RANGE_FIELD_SORT_ORDER, RANGE_SORT_ORDER);
        runner.removeProperty(ConsumeElasticsearch.RANGE_INITIAL_VALUE);
        runner.setValidateExpressionUsage(false);
    }

    @Test
    void testVerifyBuildQueryValidInitialValue() {
        runner.setProperty(ConsumeElasticsearch.RANGE_INITIAL_VALUE, "2");

        assertQueryVerify(1, 0);
    }

    @Test
    void testVerifyBuildQueryValidSortOrder() {
        runner.setProperty(ConsumeElasticsearch.RANGE_FIELD_SORT_ORDER, "desc");

        assertQueryVerify(3, 0);
    }

    @Test
    void testVerifyBuildQueryValidFormatAndTimeZone() {
        runner.setProperty(ConsumeElasticsearch.RANGE_INITIAL_VALUE, "2");
        runner.setProperty(ConsumeElasticsearch.RANGE_DATE_FORMAT, "dd");
        runner.setProperty(ConsumeElasticsearch.RANGE_TIME_ZONE, "Europe/London");

        assertQueryVerify(1, 0);
    }

    @Test
    void testVerifyBuildQueryValidSingleAdditionalFilter() {
        runner.setProperty(ConsumeElasticsearch.RANGE_INITIAL_VALUE, "1");
        runner.setProperty(ConsumeElasticsearch.ADDITIONAL_FILTERS, "{\"term\":{\"msg\":\"two\"}}");

        assertQueryVerify(1, 0);
    }

    @Test
    void testVerifyBuildQueryValidMultipleAdditionalFilters() {
        runner.setProperty(ConsumeElasticsearch.ADDITIONAL_FILTERS, "[{\"term\":{\"msg\":\"two\"}},{\"term\":{\"num\":2}}]");

        assertQueryVerify(1, 0);
    }
}
