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

import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.solr.client.solrj.*;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.mockito.Mockito.*;

/**
 * Test for PutSolr processor.
 */
public class TestPutSolrContentStream {

    static final String DEFAULT_SOLR_CORE = "testCollection";

    static final String CUSTOM_JSON_SINGLE_DOC_FILE = "src/test/resources/testdata/test-custom-json-single-doc.json";
    static final String SOLR_JSON_MULTIPLE_DOCS_FILE = "src/test/resources/testdata/test-solr-json-multiple-docs.json";
    static final String CSV_MULTIPLE_DOCS_FILE = "src/test/resources/testdata/test-csv-multiple-docs.csv";
    static final String XML_MULTIPLE_DOCS_FILE = "src/test/resources/testdata/test-xml-multiple-docs.xml";

    static final SolrDocument expectedDoc1 = new SolrDocument();
    static {
        expectedDoc1.addField("first", "John");
        expectedDoc1.addField("last", "Doe");
        expectedDoc1.addField("grade", 8);
        expectedDoc1.addField("subject", "Math");
        expectedDoc1.addField("test", "term1");
        expectedDoc1.addField("marks", 90);
    }

    static final SolrDocument expectedDoc2 = new SolrDocument();
    static {
        expectedDoc2.addField("first", "John");
        expectedDoc2.addField("last", "Doe");
        expectedDoc2.addField("grade", 8);
        expectedDoc2.addField("subject", "Biology");
        expectedDoc2.addField("test", "term1");
        expectedDoc2.addField("marks", 86);
    }

    /**
     * Creates a base TestRunner with Solr Type of standard and json update path.
     */
    private static TestRunner createDefaultTestRunner(PutSolrContentStream processor) {
        TestRunner runner = TestRunners.newTestRunner(processor);
        runner.setProperty(PutSolrContentStream.SOLR_TYPE, PutSolrContentStream.SOLR_TYPE_STANDARD.getValue());
        runner.setProperty(PutSolrContentStream.SOLR_LOCATION, "http://localhost:8443/solr");
        return runner;
    }

    @Test
    public void testUpdateWithSolrJson() throws IOException, SolrServerException {
        final EmbeddedSolrServerProcessor proc = new EmbeddedSolrServerProcessor(DEFAULT_SOLR_CORE);

        final TestRunner runner = createDefaultTestRunner(proc);
        runner.setProperty(PutSolrContentStream.CONTENT_STREAM_PATH, "/update/json/docs");
        runner.setProperty(PutSolrContentStream.REQUEST_PARAMS,"json.command=false");

        try (FileInputStream fileIn = new FileInputStream(SOLR_JSON_MULTIPLE_DOCS_FILE)) {
            runner.enqueue(fileIn);

            runner.run();
            runner.assertTransferCount(PutSolrContentStream.REL_FAILURE, 0);
            runner.assertTransferCount(PutSolrContentStream.REL_CONNECTION_FAILURE, 0);
            runner.assertTransferCount(PutSolrContentStream.REL_SUCCESS, 1);

            verifySolrDocuments(proc.getSolrClient(), Arrays.asList(expectedDoc1, expectedDoc2));
        } finally {
            try { proc.getSolrClient().shutdown(); } catch (Exception e) { }
        }
    }

    @Test
    public void testUpdateWithCustomJson() throws IOException, SolrServerException {
        final EmbeddedSolrServerProcessor proc = new EmbeddedSolrServerProcessor(DEFAULT_SOLR_CORE);

        final TestRunner runner = createDefaultTestRunner(proc);
        runner.setProperty(PutSolrContentStream.CONTENT_STREAM_PATH, "/update/json/docs");
        runner.setProperty(PutSolrContentStream.REQUEST_PARAMS,
                "split=/exams" +
                "&f=first:/first" +
                "&f=last:/last" +
                "&f=grade:/grade" +
                "&f=subject:/exams/subject" +
                "&f=test:/exams/test" +
                "&f=marks:/exams/marks");

        try (FileInputStream fileIn = new FileInputStream(CUSTOM_JSON_SINGLE_DOC_FILE)) {
            runner.enqueue(fileIn);

            runner.run();
            runner.assertTransferCount(PutSolrContentStream.REL_FAILURE, 0);
            runner.assertTransferCount(PutSolrContentStream.REL_CONNECTION_FAILURE, 0);
            runner.assertTransferCount(PutSolrContentStream.REL_SUCCESS, 1);

            verifySolrDocuments(proc.getSolrClient(), Arrays.asList(expectedDoc1, expectedDoc2));
        } finally {
            try { proc.getSolrClient().shutdown(); } catch (Exception e) { }
        }
    }

    @Test
    public void testUpdateWithCsv() throws IOException, SolrServerException {
        final EmbeddedSolrServerProcessor proc = new EmbeddedSolrServerProcessor(DEFAULT_SOLR_CORE);

        final TestRunner runner = createDefaultTestRunner(proc);
        runner.setProperty(PutSolrContentStream.CONTENT_STREAM_PATH, "/update/csv");
        runner.setProperty(PutSolrContentStream.REQUEST_PARAMS,
                "fieldnames=first,last,grade,subject,test,marks");

        try (FileInputStream fileIn = new FileInputStream(CSV_MULTIPLE_DOCS_FILE)) {
            runner.enqueue(fileIn);

            runner.run();
            runner.assertTransferCount(PutSolrContentStream.REL_FAILURE, 0);
            runner.assertTransferCount(PutSolrContentStream.REL_CONNECTION_FAILURE, 0);
            runner.assertTransferCount(PutSolrContentStream.REL_SUCCESS, 1);

            verifySolrDocuments(proc.getSolrClient(), Arrays.asList(expectedDoc1, expectedDoc2));
        } finally {
            try { proc.getSolrClient().shutdown(); } catch (Exception e) { }
        }
    }

    @Test
    public void testUpdateWithXml() throws IOException, SolrServerException {
        final EmbeddedSolrServerProcessor proc = new EmbeddedSolrServerProcessor(DEFAULT_SOLR_CORE);

        final TestRunner runner = createDefaultTestRunner(proc);
        runner.setProperty(PutSolrContentStream.CONTENT_STREAM_PATH, "/update");
        runner.setProperty(PutSolrContentStream.CONTENT_TYPE, "application/xml");

        try (FileInputStream fileIn = new FileInputStream(XML_MULTIPLE_DOCS_FILE)) {
            runner.enqueue(fileIn);

            runner.run();
            runner.assertTransferCount(PutSolrContentStream.REL_FAILURE, 0);
            runner.assertTransferCount(PutSolrContentStream.REL_CONNECTION_FAILURE, 0);
            runner.assertTransferCount(PutSolrContentStream.REL_SUCCESS, 1);

            verifySolrDocuments(proc.getSolrClient(), Arrays.asList(expectedDoc1, expectedDoc2));
        } finally {
            try { proc.getSolrClient().shutdown(); } catch (Exception e) { }
        }
    }

    @Test
    public void testSolrServerExceptionShouldRouteToConnectionFailure() throws IOException, SolrServerException {
        final Throwable throwable = new SolrServerException("Error communicating with Solr");
        final ExceptionThrowingProcessor proc = new ExceptionThrowingProcessor(throwable);

        final TestRunner runner = createDefaultTestRunner(proc);

        try (FileInputStream fileIn = new FileInputStream(CUSTOM_JSON_SINGLE_DOC_FILE)) {
            runner.enqueue(fileIn);
            runner.run();

            runner.assertAllFlowFilesTransferred(PutSolrContentStream.REL_CONNECTION_FAILURE, 1);
            verify(proc.getSolrClient(), times(1)).request(any(SolrRequest.class));
        }
    }

    @Test
    public void testSolrExceptionShouldRouteToFailure() throws IOException, SolrServerException {
        final Throwable throwable = new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Error");
        final ExceptionThrowingProcessor proc = new ExceptionThrowingProcessor(throwable);

        final TestRunner runner = createDefaultTestRunner(proc);

        try (FileInputStream fileIn = new FileInputStream(CUSTOM_JSON_SINGLE_DOC_FILE)) {
            runner.enqueue(fileIn);
            runner.run();

            runner.assertAllFlowFilesTransferred(PutSolrContentStream.REL_FAILURE, 1);
            verify(proc.getSolrClient(), times(1)).request(any(SolrRequest.class));
        }
    }

    @Test
    public void testRemoteSolrExceptionShouldRouteToFailure() throws IOException, SolrServerException {
        final Throwable throwable = new HttpSolrClient.RemoteSolrException(
                "host", 401, "error", new NumberFormatException());
        final ExceptionThrowingProcessor proc = new ExceptionThrowingProcessor(throwable);

        final TestRunner runner = createDefaultTestRunner(proc);

        try (FileInputStream fileIn = new FileInputStream(CUSTOM_JSON_SINGLE_DOC_FILE)) {
            runner.enqueue(fileIn);
            runner.run();

            runner.assertAllFlowFilesTransferred(PutSolrContentStream.REL_FAILURE, 1);
            verify(proc.getSolrClient(), times(1)).request(any(SolrRequest.class));
        }
    }


    @Test
    public void testSolrTypeCloudShouldRequireCollection() {
        final TestRunner runner = TestRunners.newTestRunner(PutSolrContentStream.class);
        runner.setProperty(PutSolrContentStream.SOLR_TYPE, PutSolrContentStream.SOLR_TYPE_CLOUD.getValue());
        runner.setProperty(PutSolrContentStream.SOLR_LOCATION, "http://localhost:8443/solr");
        runner.assertNotValid();

        runner.setProperty(PutSolrContentStream.COLLECTION, "someCollection1");
        runner.assertValid();
    }

    @Test
    public void testSolrTypeStandardShouldNotRequireCollection() {
        final TestRunner runner = TestRunners.newTestRunner(PutSolrContentStream.class);
        runner.setProperty(PutSolrContentStream.SOLR_TYPE, PutSolrContentStream.SOLR_TYPE_STANDARD.getValue());
        runner.setProperty(PutSolrContentStream.SOLR_LOCATION, "http://localhost:8443/solr");
        runner.assertValid();
    }

    @Test
    public void testRequestParamsShouldBeInvalid() {
        final TestRunner runner = TestRunners.newTestRunner(PutSolrContentStream.class);
        runner.setProperty(PutSolrContentStream.SOLR_TYPE, PutSolrContentStream.SOLR_TYPE_STANDARD.getValue());
        runner.setProperty(PutSolrContentStream.SOLR_LOCATION, "http://localhost:8443/solr");
        runner.setProperty(PutSolrContentStream.REQUEST_PARAMS, "a=1&b");
        runner.assertNotValid();
    }

    /**
     * Override the creatrSolrServer method to inject a Mock.
     */
    private class ExceptionThrowingProcessor extends PutSolrContentStream {

        private SolrClient mockSolrServer;
        private Throwable throwable;

        public ExceptionThrowingProcessor(Throwable throwable) {
            this.throwable = throwable;
        }

        @Override
        protected SolrClient createSolrClient(ProcessContext context) {
            mockSolrServer = Mockito.mock(SolrClient.class);
            try {
                when(mockSolrServer.request(any(SolrRequest.class))).thenThrow(throwable);
            } catch (SolrServerException e) {
                Assert.fail(e.getMessage());
            } catch (IOException e) {
                Assert.fail(e.getMessage());
            }
            return mockSolrServer;
        }

    }

    /**
     * Override the createSolrClient method and create and EmbeddedSolrServer.
     */
    private class EmbeddedSolrServerProcessor extends PutSolrContentStream {

        private String coreName;
        private SolrClient embeddedSolrServer;

        public EmbeddedSolrServerProcessor(String coreName) {
            this.coreName = coreName;
        }

        @Override
        protected SolrClient createSolrClient(ProcessContext context) {
            try {
                String relPath = getClass().getProtectionDomain()
                        .getCodeSource().getLocation().getFile()
                        + "../../target";

                embeddedSolrServer = EmbeddedSolrServerFactory.create(
                        EmbeddedSolrServerFactory.DEFAULT_SOLR_HOME,
                        EmbeddedSolrServerFactory.DEFAULT_CORE_HOME,
                        coreName, relPath);
            } catch (IOException e) {
                Assert.fail(e.getMessage());
            }
            return embeddedSolrServer;
        }

    }

    /**
     * Verify that given SolrServer contains the expected SolrDocuments.
     */
    private static void verifySolrDocuments(SolrClient solrServer, Collection<SolrDocument> expectedDocuments)
            throws IOException, SolrServerException {

        solrServer.commit();

        SolrQuery query = new SolrQuery("*:*");
        QueryResponse qResponse = solrServer.query(query);
        Assert.assertEquals(expectedDocuments.size(), qResponse.getResults().getNumFound());

        // verify documents have expected fields and values
        for (SolrDocument expectedDoc : expectedDocuments) {
            boolean found = false;
            for (SolrDocument solrDocument : qResponse.getResults()) {
                boolean foundAllFields = true;
                for (String expectedField : expectedDoc.getFieldNames()) {
                    Object expectedVal = expectedDoc.getFirstValue(expectedField);
                    Object actualVal = solrDocument.getFirstValue(expectedField);
                    foundAllFields = expectedVal.equals(actualVal);
                }

                if (foundAllFields) {
                    found = true;
                    break;
                }
            }
            Assert.assertTrue("Could not find " + expectedDoc, found);
        }
    }

}
