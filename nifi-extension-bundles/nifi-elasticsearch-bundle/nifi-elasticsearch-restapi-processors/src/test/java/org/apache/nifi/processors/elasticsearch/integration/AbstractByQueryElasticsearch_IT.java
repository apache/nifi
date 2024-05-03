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

import org.apache.nifi.processors.elasticsearch.DeleteByQueryElasticsearch;
import org.apache.nifi.processors.elasticsearch.ElasticsearchRestProcessor;
import org.apache.nifi.processors.elasticsearch.api.QueryDefinitionType;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

public abstract class AbstractByQueryElasticsearch_IT extends AbstractElasticsearchRestProcessor_IT {
    @Test
    void testVerifyBuildQueryValidScript() {
        Assumptions.assumeFalse(runner.getProcessor() instanceof DeleteByQueryElasticsearch,
                "DeleteByQueryElasticsearch does not use the SCRIPT property");

        runner.setProperty(ElasticsearchRestProcessor.QUERY_DEFINITION_STYLE, QueryDefinitionType.BUILD_QUERY);
        runner.setProperty(ElasticsearchRestProcessor.SCRIPT, "{\"source\": \"ctx._source.num++\", \"lang\": \"painless\"}");

        assertQueryVerify(3, 0);
    }
}
