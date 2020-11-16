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

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Test;

public class ITScrollElasticsearchHttp {

    private TestRunner runner;

    @After
    public void teardown() {
        runner = null;
    }

    @Test
    public void testFetchElasticsearchOnTrigger() throws IOException {
        runner = TestRunners.newTestRunner(ScrollElasticsearchHttp.class); // all docs are found
        runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL,
                "http://ip-172-31-49-152.ec2.internal:9200");

        runner.setProperty(ScrollElasticsearchHttp.INDEX, "prod-accounting");
        runner.assertNotValid();
        runner.setProperty(ScrollElasticsearchHttp.TYPE, "provenance");
        runner.assertNotValid();
        runner.setProperty(ScrollElasticsearchHttp.QUERY,
                "identifier:2f79eba8839f5976cd0b1e16a0e7fe8d7dd0ceca");
        runner.setProperty(ScrollElasticsearchHttp.SORT, "timestamp:asc");
        runner.setProperty(ScrollElasticsearchHttp.FIELDS, "transit_uri,version");
        runner.setProperty(ScrollElasticsearchHttp.PAGE_SIZE, "1");
        runner.assertValid();

        runner.setIncomingConnection(false);
        runner.run(4, true, true);

        runner.assertAllFlowFilesTransferred(ScrollElasticsearchHttp.REL_SUCCESS, 3);
        final MockFlowFile out = runner.getFlowFilesForRelationship(
                ScrollElasticsearchHttp.REL_SUCCESS).get(0);
        assertNotNull(out);
    }
}
