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
import org.apache.nifi.processors.elasticsearch.ElasticsearchRestProcessor;
import org.apache.nifi.processors.elasticsearch.GetElasticsearch;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;

class GetElasticsearch_IT extends AbstractElasticsearch_IT {
    ElasticsearchRestProcessor getProcessor() {
        return new GetElasticsearch();
    }

    @BeforeEach
    void before() throws Exception {
        super.before();

        runner.setProperty(GetElasticsearch.ID, ID);
    }

    @Test
    void testVerifyIndexAndDocumentExist() {
        final List<ConfigVerificationResult> results = ((VerifiableProcessor) runner.getProcessor()).verify(
                runner.getProcessContext(), runner.getLogger(), Collections.emptyMap()
        );
        assertEquals(2, results.size());
        assertEquals(2, results.stream().filter(result -> result.getOutcome() == ConfigVerificationResult.Outcome.SUCCESSFUL).count(), results.toString());
        // check Document result, index covered in AbstractElasticsearch_IT#testVerifyIndexExists
        assertEquals(1, results.stream().filter(
                        result -> Objects.equals(result.getVerificationStepName(), GetElasticsearch.VERIFICATION_STEP_DOCUMENT_EXISTS)
                                && Objects.equals(result.getExplanation(), String.format("Document [%s] exists in index [%s]", ID, INDEX))
                                && result.getOutcome() == ConfigVerificationResult.Outcome.SUCCESSFUL).count(),
                results.toString()
        );
    }

    @Test
    void testVerifyIndexNotExists() {
        final String notExists = "not-exists";
        runner.setProperty(ElasticsearchRestProcessor.INDEX, notExists);

        final List<ConfigVerificationResult> results = ((VerifiableProcessor) runner.getProcessor()).verify(
                runner.getProcessContext(), runner.getLogger(), Collections.emptyMap()
        );
        assertEquals(2, results.size());
        assertEquals(1, results.stream().filter(
                        result -> Objects.equals(result.getVerificationStepName(), ElasticsearchRestProcessor.VERIFICATION_STEP_INDEX_EXISTS)
                                && Objects.equals(result.getExplanation(), String.format("Index [%s] does not exist", notExists))
                                && result.getOutcome() == ConfigVerificationResult.Outcome.FAILED).count(),
                results.toString()
        );
        assertEquals(1, results.stream().filter(
                        result -> Objects.equals(result.getVerificationStepName(), GetElasticsearch.VERIFICATION_STEP_DOCUMENT_EXISTS)
                                && Objects.equals(result.getExplanation(), String.format("Index %s does not exist for document existence check", notExists))
                                && result.getOutcome() == ConfigVerificationResult.Outcome.SKIPPED).count(),
                results.toString()
        );
    }

    @Test
    void testVerifyIndexExistsDocumentNotExists() {
        final String notExists = "not-exists";
        runner.setProperty(GetElasticsearch.ID, notExists);

        final List<ConfigVerificationResult> results = ((VerifiableProcessor) runner.getProcessor()).verify(
                runner.getProcessContext(), runner.getLogger(), Collections.emptyMap()
        );
        assertEquals(2, results.size());
        assertEquals(1, results.stream().filter(
                        result -> Objects.equals(result.getVerificationStepName(), ElasticsearchRestProcessor.VERIFICATION_STEP_INDEX_EXISTS)
                                && Objects.equals(result.getExplanation(), String.format("Index [%s] exists", INDEX))
                                && result.getOutcome() == ConfigVerificationResult.Outcome.SUCCESSFUL).count(),
                results.toString()
        );
        assertEquals(1, results.stream().filter(
                        result -> Objects.equals(result.getVerificationStepName(), GetElasticsearch.VERIFICATION_STEP_DOCUMENT_EXISTS)
                                && Objects.equals(result.getExplanation(), String.format("Document [%s] does not exist in index [%s]", notExists, INDEX))
                                && result.getOutcome() == ConfigVerificationResult.Outcome.SUCCESSFUL).count(),
                results.toString()
        );
    }
}
