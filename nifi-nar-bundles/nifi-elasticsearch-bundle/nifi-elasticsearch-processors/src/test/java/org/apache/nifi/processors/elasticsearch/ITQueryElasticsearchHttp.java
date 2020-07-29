/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
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

import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Test;

public class ITQueryElasticsearchHttp {

    private TestRunner runner;

    @After
    public void teardown() {
        runner = null;
    }

    @Test
    public void testFetchElasticsearchOnTrigger() throws IOException {
        runner = TestRunners.newTestRunner(QueryElasticsearchHttp.class); // all docs are found
        runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL,
                "http://localhost.internal:9200");

        runner.setProperty(QueryElasticsearchHttp.INDEX, "prod-accounting");
        runner.assertNotValid();
        runner.setProperty(QueryElasticsearchHttp.TYPE, "provenance");
        runner.assertNotValid();
        runner.setProperty(QueryElasticsearchHttp.QUERY,
                "identifier:2f79eba8839f5976cd0b1e16a0e7fe8d7dd0ceca");
        runner.setProperty(QueryElasticsearchHttp.SORT, "timestamp:asc");
        runner.setProperty(QueryElasticsearchHttp.FIELDS, "transit_uri,version");
        runner.setProperty(QueryElasticsearchHttp.PAGE_SIZE, "1");
        runner.assertValid();

        runner.setIncomingConnection(false);
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(QueryElasticsearchHttp.REL_SUCCESS, 3);
        final MockFlowFile out = runner.getFlowFilesForRelationship(
                QueryElasticsearchHttp.REL_SUCCESS).get(0);
        assertNotNull(out);
    }

    @Test
    public void testFetchElasticsearchOnTrigger_IncomingFile() throws IOException {
        runner = TestRunners.newTestRunner(QueryElasticsearchHttp.class); // all docs are found
        runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL,
                "http://localhost.internal:9200");

        runner.setProperty(QueryElasticsearchHttp.INDEX, "prod-accounting");
        runner.assertNotValid();
        runner.setProperty(QueryElasticsearchHttp.TYPE, "provenance");
        runner.assertNotValid();
        runner.setProperty(QueryElasticsearchHttp.QUERY, "${query}");
        runner.setProperty(QueryElasticsearchHttp.SORT, "timestamp:asc");
        runner.setProperty(QueryElasticsearchHttp.FIELDS, "transit_uri,version");
        runner.setProperty(QueryElasticsearchHttp.PAGE_SIZE, "1");
        runner.assertValid();

        Map<String, String> attributes = new HashMap<>();
        attributes.put("query", "identifier:2f79eba8839f5976cd0b1e16a0e7fe8d7dd0ceca");
        runner.enqueue("".getBytes(), attributes);
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(QueryElasticsearchHttp.REL_SUCCESS, 3);
        final MockFlowFile out = runner.getFlowFilesForRelationship(
                QueryElasticsearchHttp.REL_SUCCESS).get(0);
        assertNotNull(out);
    }
}
