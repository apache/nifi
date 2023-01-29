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

import org.apache.nifi.util.TestRunner
import org.apache.nifi.util.TestRunners
import org.junit.jupiter.api.Test

import static org.hamcrest.CoreMatchers.hasItem
import static org.hamcrest.CoreMatchers.not
import static org.hamcrest.MatcherAssert.assertThat

abstract class AbstractPutElasticsearchTest<P extends AbstractPutElasticsearch> {
    abstract P getProcessor()

    @Test
    void testOutputErrorResponsesRelationship() {
        final TestRunner runner = createRunner()

        assertThat(runner.getProcessor().getRelationships(), not(hasItem(AbstractPutElasticsearch.REL_ERROR_RESPONSES)))

        runner.setProperty(AbstractPutElasticsearch.OUTPUT_ERROR_RESPONSES, "true")
        assertThat(runner.getProcessor().getRelationships(), hasItem(AbstractPutElasticsearch.REL_ERROR_RESPONSES))

        runner.setProperty(AbstractPutElasticsearch.OUTPUT_ERROR_RESPONSES, "false")
        assertThat(runner.getProcessor().getRelationships(), not(hasItem(AbstractPutElasticsearch.REL_ERROR_RESPONSES)))
    }

    TestRunner createRunner() {
        final P processor = getProcessor()
        final TestRunner runner = TestRunners.newTestRunner(processor)

        return runner
    }
}
