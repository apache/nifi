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

import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.processor.VerifiableProcessor;
import org.apache.nifi.processors.elasticsearch.ConsumeElasticsearch;
import org.apache.nifi.processors.elasticsearch.ElasticsearchRestProcessor;
import org.apache.nifi.processors.elasticsearch.api.QueryDefinitionType;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;

public abstract class AbstractElasticsearchRestProcessor_IT extends AbstractElasticsearch_IT<ElasticsearchRestProcessor> {
    private boolean isConsumeElasticsearch() {
        return runner.getProcessor() instanceof ConsumeElasticsearch;
    }

    @Test
    void testVerifyFullQueryInvalidJson() {
        Assumptions.assumeFalse(isConsumeElasticsearch(), "ConsumeElasticsearch does not use the FULL_QUERY Definition Type");

        runner.setProperty(ElasticsearchRestProcessor.QUERY_DEFINITION_STYLE, QueryDefinitionType.FULL_QUERY);
        runner.setProperty(ElasticsearchRestProcessor.QUERY, "{\"query\":");

        final List<ConfigVerificationResult> results = ((VerifiableProcessor) runner.getProcessor()).verify(
                runner.getProcessContext(), runner.getLogger(), Collections.emptyMap()
        );
        assertEquals(3, results.size());
        assertEquals(1, results.stream().filter(result -> result.getOutcome() == ConfigVerificationResult.Outcome.SUCCESSFUL).count(), results.toString());
        // check Query JSON result, index covered in AbstractElasticsearch_IT#testVerifyIndexExists
        assertEquals(1, results.stream().filter(
                        result -> Objects.equals(result.getVerificationStepName(), ElasticsearchRestProcessor.VERIFICATION_STEP_QUERY_JSON_VALID)
                                && result.getExplanation().startsWith("Query cannot be parsed as valid JSON: Unexpected end-of-input within/between Object entries")
                                && result.getOutcome() == ConfigVerificationResult.Outcome.FAILED).count(),
                results.toString()
        );
        // check Query result
        assertEquals(1, results.stream().filter(
                        result -> Objects.equals(result.getVerificationStepName(), ElasticsearchRestProcessor.VERIFICATION_STEP_QUERY_VALID)
                                && Objects.equals(result.getExplanation(), "Query JSON could not be parsed")
                                && result.getOutcome() == ConfigVerificationResult.Outcome.SKIPPED).count(),
                results.toString()
        );
    }

    @Test
    void testVerifyFullQueryValid() {
        Assumptions.assumeFalse(isConsumeElasticsearch(), "ConsumeElasticsearch does not use the FULL_QUERY Definition Type");

        runner.setProperty(ElasticsearchRestProcessor.QUERY_DEFINITION_STYLE, QueryDefinitionType.FULL_QUERY);
        runner.setProperty(ElasticsearchRestProcessor.QUERY, "{\"query\":{\"term\":{\"msg\":\"one\"}}, \"aggs\":{\"messages\":{\"terms\":{\"field\":\"msg\"}}}}");

        assertQueryVerify(1, 1);
    }

    @Test
    void testVerifyFullQueryValidEmptyQuery() {
        Assumptions.assumeFalse(isConsumeElasticsearch(), "ConsumeElasticsearch does not use the FULL_QUERY Definition Type");

        runner.setProperty(ElasticsearchRestProcessor.QUERY_DEFINITION_STYLE, QueryDefinitionType.FULL_QUERY);
        runner.removeProperty(ElasticsearchRestProcessor.QUERY); // should run a default "match_all" query

        assertQueryVerify(3, 0);
    }

    @Test
    void testVerifyFullQueryInvalid() {
        Assumptions.assumeFalse(isConsumeElasticsearch(), "ConsumeElasticsearch does not use the FULL_QUERY Definition Type");

        runner.setProperty(ElasticsearchRestProcessor.QUERY_DEFINITION_STYLE, QueryDefinitionType.FULL_QUERY);
        runner.setProperty(ElasticsearchRestProcessor.QUERY, "{\"query\":{\"unknown\":{}}}");

        final List<ConfigVerificationResult> results = assertVerify(2);

        // check Query result
        assertEquals(1, results.stream().filter(
                        result -> Objects.equals(result.getVerificationStepName(), ElasticsearchRestProcessor.VERIFICATION_STEP_QUERY_VALID)
                                && result.getExplanation().startsWith("Query failed in Elasticsearch: ")
                                && result.getExplanation().contains("[unknown]")
                                && result.getOutcome() == ConfigVerificationResult.Outcome.FAILED).count(),
                results.toString()
        );
    }

    @Test
    void testVerifyBuildQueryValidQueryClause() {
        Assumptions.assumeFalse(isConsumeElasticsearch(), "ConsumeElasticsearch does not use the QUERY_CLAUSE");

        runner.setProperty(ElasticsearchRestProcessor.QUERY_DEFINITION_STYLE, QueryDefinitionType.BUILD_QUERY);
        runner.setProperty(ElasticsearchRestProcessor.QUERY_CLAUSE, "{\"term\":{\"msg\":\"one\"}}");

        assertQueryVerify(1, 0);
    }

    List<ConfigVerificationResult> assertVerify(final int numSuccess) {
        final List<ConfigVerificationResult> results = ((VerifiableProcessor) runner.getProcessor()).verify(
                runner.getProcessContext(), runner.getLogger(), Collections.emptyMap()
        );
        assertEquals(3, results.size());
        assertEquals(numSuccess, results.stream().filter(result -> result.getOutcome() == ConfigVerificationResult.Outcome.SUCCESSFUL).count(), results.toString());
        // check Query JSON result, index covered in AbstractElasticsearch_IT#testVerifyIndexExists
        assertEquals(1, results.stream().filter(
                        result -> Objects.equals(result.getVerificationStepName(), ElasticsearchRestProcessor.VERIFICATION_STEP_QUERY_JSON_VALID)
                                && Objects.equals(result.getExplanation(), "Query JSON successfully parsed")
                                && result.getOutcome() == ConfigVerificationResult.Outcome.SUCCESSFUL).count(),
                results.toString()
        );

        return results;
    }

    void assertQueryVerify(final int numHits, final int numAggs) {
        final List<ConfigVerificationResult> results = assertVerify(3);

        // check Query result
        assertEquals(1, results.stream().filter(
                        result -> Objects.equals(result.getVerificationStepName(), ElasticsearchRestProcessor.VERIFICATION_STEP_QUERY_VALID)
                                && result.getExplanation().matches(String.format("Query found %d hits and %d aggregations in \\d+ milliseconds, timed out: false", numHits, numAggs))
                                && result.getOutcome() == ConfigVerificationResult.Outcome.SUCCESSFUL).count(),
                results.toString()
        );
    }
}
