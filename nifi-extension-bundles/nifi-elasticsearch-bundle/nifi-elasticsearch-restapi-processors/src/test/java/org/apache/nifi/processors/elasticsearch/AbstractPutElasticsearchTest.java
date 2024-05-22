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

import org.apache.nifi.elasticsearch.IndexOperationRequest;
import org.apache.nifi.elasticsearch.IndexOperationResponse;
import org.apache.nifi.processors.elasticsearch.mock.MockBulkLoadClientService;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class AbstractPutElasticsearchTest<P extends AbstractPutElasticsearch> {
    static final String TEST_COMMON_DIR = "src/test/resources/common";

    public abstract Class<? extends AbstractPutElasticsearch> getTestProcessor();

    MockBulkLoadClientService clientService;
    TestRunner runner;

    @BeforeEach
    public void setup() throws Exception {
        runner = TestRunners.newTestRunner(getTestProcessor());

        clientService = new MockBulkLoadClientService();
        clientService.setResponse(new IndexOperationResponse(1500));
        runner.addControllerService("clientService", clientService);
        runner.setProperty(AbstractPutElasticsearch.CLIENT_SERVICE, "clientService");
        runner.enableControllerService(clientService);

        runner.setProperty(AbstractPutElasticsearch.INDEX_OP, IndexOperationRequest.Operation.Index.getValue());
        runner.setProperty(AbstractPutElasticsearch.INDEX, "test_index");
        runner.setProperty(AbstractPutElasticsearch.TYPE, "test_type");

        runner.setProperty(AbstractPutElasticsearch.LOG_ERROR_RESPONSES, "false");
        runner.setProperty(AbstractPutElasticsearch.NOT_FOUND_IS_SUCCESSFUL, "true");
        runner.setProperty(AbstractPutElasticsearch.OUTPUT_ERROR_RESPONSES, "false");
    }

    @Test
    public void testOutputErrorResponsesRelationship() {
        final TestRunner runner = TestRunners.newTestRunner(getTestProcessor());

        assertFalse(runner.getProcessor().getRelationships().contains(AbstractPutElasticsearch.REL_ERROR_RESPONSES));

        runner.setProperty(AbstractPutElasticsearch.OUTPUT_ERROR_RESPONSES, "true");
        assertTrue(runner.getProcessor().getRelationships().contains(AbstractPutElasticsearch.REL_ERROR_RESPONSES));

        runner.setProperty(AbstractPutElasticsearch.OUTPUT_ERROR_RESPONSES, "false");
        assertFalse(runner.getProcessor().getRelationships().contains(AbstractPutElasticsearch.REL_ERROR_RESPONSES));
    }

    protected String getUnexpectedCountMsg(final String countName) {
        return "Did not get expected " + countName + " count";
    }
}
