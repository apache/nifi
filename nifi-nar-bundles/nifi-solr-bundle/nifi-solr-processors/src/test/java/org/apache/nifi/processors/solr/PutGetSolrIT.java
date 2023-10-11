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

package org.apache.nifi.processors.solr;

import java.io.File;
import java.io.IOException;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.SolrContainer;
import org.testcontainers.utility.DockerImageName;

public class PutGetSolrIT {
    private static final String SOLR_IMAGE = "solr:latest";
    private static SolrContainer container;
    private static String solrLocation;

    @BeforeAll
    public static void setup() {
        container = new SolrContainer(DockerImageName.parse(SOLR_IMAGE));
        container.withCollection("nifi").start();

        solrLocation = "http://" + container.getHost() + ":" + container.getSolrPort() + "/solr";
    }

    @AfterAll
    public static void shutdown() {
        if (container != null) {
            container.stop();
        }
    }

    @Test
    public void testPutSimpleRecord() throws InitializationException, IOException {
        // Put a record to Solr
        final TestRunner runner = TestRunners.newTestRunner(PutSolrRecord.class);

        final RecordReaderFactory reader = new JsonTreeReader();
        runner.addControllerService("reader", reader);
        runner.enableControllerService(reader);
        runner.setProperty(PutSolrRecord.RECORD_READER, "reader");
        runner.setProperty(SolrUtils.SOLR_LOCATION, solrLocation);
        runner.setProperty(SolrUtils.COLLECTION, "nifi");

        final File inputFile = new File("src/test/resources/testdata/test-solr-json-multiple-docs.json");
        runner.enqueue(inputFile.toPath());
        runner.run();

        runner.assertAllFlowFilesTransferred(PutSolrRecord.REL_SUCCESS, 1);

        // Get the record back out
        final TestRunner getRunner = TestRunners.newTestRunner(GetSolr.class);
        final RecordSetWriterFactory writer = new JsonRecordSetWriter();
        getRunner.addControllerService("writer", writer);
        getRunner.enableControllerService(writer);
        getRunner.setProperty(SolrUtils.RECORD_WRITER, "writer");
        runner.setProperty(SolrUtils.SOLR_LOCATION, solrLocation);
        runner.setProperty(SolrUtils.COLLECTION, "nifi");
        runner.setProperty(GetSolr.RETURN_TYPE, GetSolr.MODE_REC);
        runner.setProperty(GetSolr.SOLR_QUERY, "subject:Math");

        runner.run();
        runner.assertAllFlowFilesTransferred(GetSolr.REL_SUCCESS, 1);

        final MockFlowFile outFlowFile = runner.getFlowFilesForRelationship(GetSolr.REL_SUCCESS).get(0);
        outFlowFile.assertContentEquals(inputFile);
    }
}
