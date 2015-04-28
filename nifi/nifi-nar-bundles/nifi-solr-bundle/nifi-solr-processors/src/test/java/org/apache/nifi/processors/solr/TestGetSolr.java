/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nifi.processors.solr;

import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.Properties;
import java.util.TimeZone;

import static org.junit.Assert.assertTrue;

public class TestGetSolr {

    static final String DEFAULT_SOLR_CORE = "testCollection";

    private SolrClient solrClient;

    @Before
    public void setup() {
        // create the conf dir if it doesn't exist
        File confDir = new File("conf");
        if (!confDir.exists()) {
            confDir.mkdir();
        }

        try {
            // create an EmbeddedSolrServer for the processor to use
            String relPath = getClass().getProtectionDomain().getCodeSource()
                    .getLocation().getFile() + "../../target";

            solrClient = EmbeddedSolrServerFactory.create(EmbeddedSolrServerFactory.DEFAULT_SOLR_HOME,
                    EmbeddedSolrServerFactory.DEFAULT_CORE_HOME, DEFAULT_SOLR_CORE, relPath);

            // create some test documents
            SolrInputDocument doc1 = new SolrInputDocument();
            doc1.addField("first", "bob");
            doc1.addField("last", "smith");
            doc1.addField("created", new Date());

            SolrInputDocument doc2 = new SolrInputDocument();
            doc2.addField("first", "alice");
            doc2.addField("last", "smith");
            doc2.addField("created", new Date());

            SolrInputDocument doc3 = new SolrInputDocument();
            doc3.addField("first", "mike");
            doc3.addField("last", "smith");
            doc3.addField("created", new Date());

            SolrInputDocument doc4 = new SolrInputDocument();
            doc4.addField("first", "john");
            doc4.addField("last", "smith");
            doc4.addField("created", new Date());

            SolrInputDocument doc5 = new SolrInputDocument();
            doc5.addField("first", "joan");
            doc5.addField("last", "smith");
            doc5.addField("created", new Date());

            // add the test data to the index
            solrClient.add(Arrays.asList(doc1, doc2, doc3, doc4, doc5));
            solrClient.commit();
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @After
    public void teardown() {
        File confDir = new File("conf");
        assertTrue(confDir.exists());
        File[] files = confDir.listFiles();
        assertTrue(files.length > 0);
        for (File file : files) {
            assertTrue("Failed to delete " + file.getName(), file.delete());
        }
        assertTrue(confDir.delete());

        try {
            solrClient.shutdown();
        } catch (Exception e) {
        }
    }

    @Test
    public void testMoreThanBatchSizeShouldProduceMultipleFlowFiles() throws IOException, SolrServerException {
        final TestableProcessor proc = new TestableProcessor(solrClient);
        final TestRunner runner = TestRunners.newTestRunner(proc);

        // setup a lastEndDate file to simulate picking up from a previous end date
        SimpleDateFormat sdf = new SimpleDateFormat(GetSolr.LAST_END_DATE_PATTERN, Locale.US);
        sdf.setTimeZone(TimeZone.getTimeZone("GMT"));

        Calendar cal = new GregorianCalendar();
        cal.add(Calendar.MINUTE, -30);
        final String lastEndDate = sdf.format(cal.getTime());

        File lastEndDateCache = new File(GetSolr.FILE_PREFIX + proc.getIdentifier());
        try (FileOutputStream fos = new FileOutputStream(lastEndDateCache)) {
            Properties props = new Properties();
            props.setProperty(GetSolr.LAST_END_DATE, lastEndDate);
            props.store(fos, "GetSolr LastEndDate value");
        } catch (IOException e) {
            Assert.fail("Failed to setup last end date value: " + e.getMessage());
        }

        runner.setProperty(GetSolr.SOLR_TYPE, PutSolrContentStream.SOLR_TYPE_STANDARD.getValue());
        runner.setProperty(GetSolr.SOLR_LOCATION, "http://localhost:8443/solr");
        runner.setProperty(GetSolr.SOLR_QUERY, "last:smith");
        runner.setProperty(GetSolr.RETURN_FIELDS, "first, last, created");
        runner.setProperty(GetSolr.SORT_CLAUSE, "created desc, first asc");
        runner.setProperty(GetSolr.DATE_FIELD, "created");
        runner.setProperty(GetSolr.BATCH_SIZE, "2");

        runner.run();
        runner.assertAllFlowFilesTransferred(GetSolr.REL_SUCCESS, 3);
    }

    @Test
    public void testLessThanBatchSizeShouldProduceOneFlowFile() throws IOException, SolrServerException {
        final TestableProcessor proc = new TestableProcessor(solrClient);

        TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(GetSolr.SOLR_TYPE, PutSolrContentStream.SOLR_TYPE_STANDARD.getValue());
        runner.setProperty(GetSolr.SOLR_LOCATION, "http://localhost:8443/solr");
        runner.setProperty(GetSolr.SOLR_QUERY, "last:smith");
        runner.setProperty(GetSolr.RETURN_FIELDS, "created");
        runner.setProperty(GetSolr.SORT_CLAUSE, "created desc");
        runner.setProperty(GetSolr.DATE_FIELD, "created");
        runner.setProperty(GetSolr.BATCH_SIZE, "10");

        runner.run();
        runner.assertAllFlowFilesTransferred(GetSolr.REL_SUCCESS, 1);
    }

    @Test
    public void testNoResultsShouldProduceNoOutput() throws IOException, SolrServerException {
        final TestableProcessor proc = new TestableProcessor(solrClient);

        TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(GetSolr.SOLR_TYPE, PutSolrContentStream.SOLR_TYPE_STANDARD.getValue());
        runner.setProperty(GetSolr.SOLR_LOCATION, "http://localhost:8443/solr");
        runner.setProperty(GetSolr.SOLR_QUERY, "last:xyz");
        runner.setProperty(GetSolr.RETURN_FIELDS, "created");
        runner.setProperty(GetSolr.SORT_CLAUSE, "created desc");
        runner.setProperty(GetSolr.DATE_FIELD, "created");
        runner.setProperty(GetSolr.BATCH_SIZE, "10");

        runner.run();
        runner.assertAllFlowFilesTransferred(GetSolr.REL_SUCCESS, 0);
    }


    // Override createSolrClient and return the passed in SolrClient
    private class TestableProcessor extends GetSolr {
        private SolrClient solrClient;

        public TestableProcessor(SolrClient solrClient) {
            this.solrClient = solrClient;
        }
        @Override
        protected SolrClient createSolrClient(ProcessContext context) {
            return solrClient;
        }
    }
}
