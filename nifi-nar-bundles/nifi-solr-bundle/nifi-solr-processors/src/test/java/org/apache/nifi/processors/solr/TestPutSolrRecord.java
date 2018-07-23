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

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test for PutSolrRecord Processor
 */
public class TestPutSolrRecord {

    private static final String DEFAULT_SOLR_CORE = "testCollection";

    @Test
    public void testPutSolrOnTriggerIndex() throws IOException, InitializationException, SolrServerException {
        final SolrClient solrClient = createEmbeddedSolrClient(DEFAULT_SOLR_CORE);
        TestableProcessor proc = new TestableProcessor(solrClient);

        TestRunner runner= createDefaultTestRunner(proc);
        MockRecordParser recordParser = new MockRecordParser();
        runner.addControllerService("parser", recordParser);

        runner.enableControllerService(recordParser);
        runner.setProperty(PutSolrRecord.RECORD_READER, "parser");

        runner.setProperty(PutSolrRecord.UPDATE_PATH, "/update");

        recordParser.addSchemaField("id", RecordFieldType.INT);
        recordParser.addSchemaField("first", RecordFieldType.STRING);
        recordParser.addSchemaField("last", RecordFieldType.STRING);
        recordParser.addSchemaField("grade", RecordFieldType.INT);
        recordParser.addSchemaField("subject", RecordFieldType.STRING);
        recordParser.addSchemaField("test", RecordFieldType.STRING);
        recordParser.addSchemaField("marks", RecordFieldType.INT);

        SolrDocument solrDocument = new SolrDocument();
        solrDocument.put("id",1);
        solrDocument.put("first","Abhinav");
        solrDocument.put("last","R");
        solrDocument.put("grade",8);
        solrDocument.put("subject","Chemistry");
        solrDocument.put("test","term1");
        solrDocument.put("marks",98);

        recordParser.addRecord(1, "Abhinav","R",8,"Chemistry","term1", 98);

        try {
            runner.enqueue(new byte[0], new HashMap<String, String>() {{
                put("id", "1");
            }});
            runner.run(1, false);
            verifySolrDocuments(proc.getSolrClient(), Collections.singletonList(solrDocument));
            runner.assertTransferCount(PutSolrRecord.REL_FAILURE, 0);
            runner.assertTransferCount(PutSolrRecord.REL_CONNECTION_FAILURE, 0);
            runner.assertTransferCount(PutSolrRecord.REL_SUCCESS, 1);
        } finally {
            try {
                proc.getSolrClient().close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testPutSolrOnTriggerIndexForANestedRecord() throws IOException, InitializationException, SolrServerException {
        final SolrClient solrClient = createEmbeddedSolrClient(DEFAULT_SOLR_CORE);
        TestableProcessor proc = new TestableProcessor(solrClient);

        TestRunner runner= createDefaultTestRunner(proc);
        MockRecordParser recordParser = new MockRecordParser();
        runner.addControllerService("parser", recordParser);

        runner.enableControllerService(recordParser);
        runner.setProperty(PutSolrRecord.RECORD_READER, "parser");

        runner.setProperty(PutSolrRecord.UPDATE_PATH, "/update");

        recordParser.addSchemaField("id", RecordFieldType.INT);
        recordParser.addSchemaField("first", RecordFieldType.STRING);
        recordParser.addSchemaField("last", RecordFieldType.STRING);
        recordParser.addSchemaField("grade", RecordFieldType.INT);
        recordParser.addSchemaField("exam", RecordFieldType.RECORD);

        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("subject", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("test", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("marks", RecordFieldType.INT.getDataType()));
        RecordSchema schema = new SimpleRecordSchema(fields);

        Map<String,Object> values = new HashMap<>();
        values.put("subject","Chemistry");
        values.put("test","term1");
        values.put("marks",98);
        final Record record = new MapRecord(schema,values);

        recordParser.addRecord(1, "Abhinav","R",8,record);


        SolrDocument solrDocument = new SolrDocument();
        solrDocument.put("id",1);
        solrDocument.put("first","Abhinav");
        solrDocument.put("last","R");
        solrDocument.put("grade",8);
        solrDocument.put("exam_subject","Chemistry");
        solrDocument.put("exam_test","term1");
        solrDocument.put("exam_marks",98);

        try {
            runner.enqueue(new byte[0], new HashMap<String, String>() {{
                put("id", "1");
            }});
            runner.run(1, false);
            runner.assertTransferCount(PutSolrRecord.REL_FAILURE, 0);
            runner.assertTransferCount(PutSolrRecord.REL_CONNECTION_FAILURE, 0);
            runner.assertTransferCount(PutSolrRecord.REL_SUCCESS, 1);
            verifySolrDocuments(proc.getSolrClient(), Collections.singletonList(solrDocument));
        } finally {
            try {
                proc.getSolrClient().close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    @Test
    public void testRecordParserExceptionShouldRoutToFailure() throws IOException, InitializationException, SolrServerException {
        final SolrClient solrClient = createEmbeddedSolrClient(DEFAULT_SOLR_CORE);
        TestableProcessor proc = new TestableProcessor(solrClient);

        TestRunner runner= createDefaultTestRunner(proc);
        MockRecordParser recordParser = new MockRecordParser();
        runner.addControllerService("parser", recordParser);

        runner.enableControllerService(recordParser);
        runner.setProperty(PutSolrRecord.RECORD_READER, "parser");

        runner.setProperty(PutSolrRecord.UPDATE_PATH, "/update");

        recordParser.addSchemaField("id", RecordFieldType.INT);
        recordParser.addSchemaField("first", RecordFieldType.STRING);
        recordParser.addSchemaField("last", RecordFieldType.STRING);
        recordParser.addSchemaField("grade", RecordFieldType.INT);

        recordParser.failAfter(0);

        try {
            runner.enqueue(new byte[0], new HashMap<String, String>() {{
                put("id", "1");
            }});
            runner.run(1, false);
            runner.assertTransferCount(PutSolrRecord.REL_FAILURE, 1);
            runner.assertTransferCount(PutSolrRecord.REL_CONNECTION_FAILURE, 0);
            runner.assertTransferCount(PutSolrRecord.REL_SUCCESS, 0);
            verifySolrDocuments(proc.getSolrClient(),Collections.EMPTY_LIST);
        } finally {
            try {
                proc.getSolrClient().close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testPutSolrOnTriggerIndexForAnArrayOfNestedRecord() throws IOException, InitializationException, SolrServerException {
        final SolrClient solrClient = createEmbeddedSolrClient(DEFAULT_SOLR_CORE);
        TestableProcessor proc = new TestableProcessor(solrClient);

        TestRunner runner= createDefaultTestRunner(proc);
        MockRecordParser recordParser = new MockRecordParser();
        runner.addControllerService("parser", recordParser);

        runner.enableControllerService(recordParser);
        runner.setProperty(PutSolrRecord.RECORD_READER, "parser");

        runner.setProperty(PutSolrRecord.UPDATE_PATH, "/update");

        recordParser.addSchemaField("id", RecordFieldType.INT);
        recordParser.addSchemaField("first", RecordFieldType.STRING);
        recordParser.addSchemaField("last", RecordFieldType.STRING);
        recordParser.addSchemaField("grade", RecordFieldType.INT);
        recordParser.addSchemaField("exams", RecordFieldType.ARRAY);

        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("subject", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("test", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("marks", RecordFieldType.INT.getDataType()));
        RecordSchema schema = new SimpleRecordSchema(fields);

        Map<String,Object> values1 = new HashMap<>();
        values1.put("subject","Chemistry");
        values1.put("test","term1");
        values1.put("marks",98);
        final Record record1 = new MapRecord(schema,values1);

        Map<String,Object> values2 = new HashMap<>();
        values2.put("subject","Maths");
        values2.put("test","term1");
        values2.put("marks",98);
        final Record record2 = new MapRecord(schema,values2);

        recordParser.addRecord(1, "Abhinav","R",8,new Record[]{record1,record2});

        SolrDocument solrDocument = new SolrDocument();
        solrDocument.put("id",1);
        solrDocument.put("first","Abhinav");
        solrDocument.put("last","R");
        solrDocument.put("grade",8);
        solrDocument.put("exams_subject", Stream.of("Chemistry","Maths").collect(Collectors.toList()));
        solrDocument.put("exams_test",Stream.of("term1","term1").collect(Collectors.toList()));
        solrDocument.put("exams_marks",Stream.of(98,98).collect(Collectors.toList()));

        try {
            runner.enqueue(new byte[0], new HashMap<String, String>() {{
                put("id", "1");
            }});
            runner.run(1, false);
            verifySolrDocuments(solrClient,Collections.singletonList(solrDocument));
            runner.assertTransferCount(PutSolrRecord.REL_FAILURE, 0);
            runner.assertTransferCount(PutSolrRecord.REL_CONNECTION_FAILURE, 0);
            runner.assertTransferCount(PutSolrRecord.REL_SUCCESS, 1);

        }catch (Exception e){
            e.printStackTrace();
        }finally {
            try {
                proc.getSolrClient().close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testCollectionExpressionLanguage() throws IOException, SolrServerException, InitializationException {
        final String collection = "collection1";
        final CollectionVerifyingProcessor proc = new CollectionVerifyingProcessor(collection);

        TestRunner runner = TestRunners.newTestRunner(proc);

        MockRecordParser recordParser = new MockRecordParser();
        runner.addControllerService("parser", recordParser);

        runner.enableControllerService(recordParser);
        runner.setProperty(PutSolrRecord.RECORD_READER, "parser");

        recordParser.addSchemaField("id", RecordFieldType.INT);
        recordParser.addSchemaField("first", RecordFieldType.STRING);
        recordParser.addSchemaField("last", RecordFieldType.STRING);
        recordParser.addSchemaField("grade", RecordFieldType.INT);
        recordParser.addSchemaField("subject", RecordFieldType.STRING);
        recordParser.addSchemaField("test", RecordFieldType.STRING);
        recordParser.addSchemaField("marks", RecordFieldType.INT);


        recordParser.addRecord(1, "Abhinav","R",8,"Chemistry","term1", 98);

        runner.setProperty(SolrUtils.SOLR_TYPE, SolrUtils.SOLR_TYPE_CLOUD.getValue());
        runner.setProperty(SolrUtils.SOLR_LOCATION, "localhost:9983");
        runner.setProperty(SolrUtils.COLLECTION, "${solr.collection}");

        final Map<String,String> attributes = new HashMap<>();
        attributes.put("solr.collection", collection);
        attributes.put("id","1");
        try {
            runner.enqueue(new byte[0], attributes);
            runner.run(1, false);
            runner.assertTransferCount(PutSolrRecord.REL_FAILURE, 0);
            runner.assertTransferCount(PutSolrRecord.REL_CONNECTION_FAILURE, 0);
            runner.assertTransferCount(PutSolrRecord.REL_SUCCESS, 1);

        } finally {
            try {
                proc.getSolrClient().close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testSolrServerExceptionShouldRouteToFailure() throws IOException, SolrServerException, InitializationException {
        final Throwable throwable = new SolrServerException("Invalid Document");
        final ExceptionThrowingProcessor proc = new ExceptionThrowingProcessor(throwable);

        final TestRunner runner = createDefaultTestRunner(proc);
        runner.setProperty(PutSolrRecord.UPDATE_PATH, "/update");

        MockRecordParser recordParser = new MockRecordParser();

        recordParser.addRecord(1, "Abhinav","R",8,"Chemistry","term1", 98);
        runner.addControllerService("parser", recordParser);

        runner.enableControllerService(recordParser);
        runner.setProperty(PutSolrRecord.RECORD_READER, "parser");


        try {
            runner.enqueue(new byte[0], new HashMap<String, String>() {{
                put("id", "1");
            }});
            runner.run(1,false);

            runner.assertAllFlowFilesTransferred(PutSolrRecord.REL_FAILURE, 1);
            verify(proc.getSolrClient(), times(1)).request(any(SolrRequest.class), eq((String)null));
        }finally {
            try {
                proc.getSolrClient().close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testSolrServerExceptionCausedByIOExceptionShouldRouteToConnectionFailure() throws IOException, SolrServerException, InitializationException {
        final Throwable throwable = new SolrServerException(new IOException("Error communicating with Solr"));
        final ExceptionThrowingProcessor proc = new ExceptionThrowingProcessor(throwable);

        final TestRunner runner = createDefaultTestRunner(proc);

        runner.setProperty(PutSolrRecord.UPDATE_PATH, "/update");

        MockRecordParser recordParser = new MockRecordParser();
        recordParser.addRecord(1, "Abhinav","R",8,"Chemistry","term1", 98);
        runner.addControllerService("parser", recordParser);

        runner.enableControllerService(recordParser);
        runner.setProperty(PutSolrRecord.RECORD_READER, "parser");

        try {
            runner.enqueue(new byte[0], new HashMap<String, String>() {{
                put("id", "1");
            }});
            runner.run();

            runner.assertAllFlowFilesTransferred(PutSolrRecord.REL_CONNECTION_FAILURE, 1);
            verify(proc.getSolrClient(), times(1)).request(any(SolrRequest.class), eq((String)null));
        }finally {
            try {
                proc.getSolrClient().close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testSolrExceptionShouldRouteToFailure() throws IOException, SolrServerException, InitializationException {
        final Throwable throwable = new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Error");
        final ExceptionThrowingProcessor proc = new ExceptionThrowingProcessor(throwable);

        final TestRunner runner = createDefaultTestRunner(proc);
        runner.setProperty(PutSolrRecord.UPDATE_PATH, "/update");

        MockRecordParser recordParser = new MockRecordParser();
        recordParser.addRecord(1, "Abhinav","R",8,"Chemistry","term1", 98);
        runner.addControllerService("parser", recordParser);

        runner.enableControllerService(recordParser);
        runner.setProperty(PutSolrRecord.RECORD_READER, "parser");

        try {
            runner.enqueue(new byte[0], new HashMap<String, String>() {{
                put("id", "1");
            }});
            runner.run();

            runner.assertAllFlowFilesTransferred(PutSolrRecord.REL_FAILURE, 1);
            verify(proc.getSolrClient(), times(1)).request(any(SolrRequest.class), eq((String)null));
        }finally {
            try {
                proc.getSolrClient().close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testRemoteSolrExceptionShouldRouteToFailure() throws IOException, SolrServerException, InitializationException {
        final Throwable throwable = new HttpSolrClient.RemoteSolrException(
                "host", 401, "error", new NumberFormatException());
        final ExceptionThrowingProcessor proc = new ExceptionThrowingProcessor(throwable);

        final TestRunner runner = createDefaultTestRunner(proc);
        runner.setProperty(PutSolrRecord.UPDATE_PATH, "/update");

        MockRecordParser recordParser = new MockRecordParser();
        recordParser.addRecord(1, "Abhinav","R",8,"Chemistry","term1", 98);
        runner.addControllerService("parser", recordParser);

        runner.enableControllerService(recordParser);
        runner.setProperty(PutSolrRecord.RECORD_READER, "parser");

        try {
            runner.enqueue(new byte[0], new HashMap<String, String>() {{
                put("id", "1");
            }});
            runner.run();

            runner.assertAllFlowFilesTransferred(PutSolrRecord.REL_FAILURE, 1);
            verify(proc.getSolrClient(), times(1)).request(any(SolrRequest.class), eq((String)null));
        }finally {
            try {
                proc.getSolrClient().close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testIOExceptionShouldRouteToConnectionFailure() throws IOException, SolrServerException, InitializationException {
        final Throwable throwable = new IOException("Error communicating with Solr");
        final ExceptionThrowingProcessor proc = new ExceptionThrowingProcessor(throwable);

        final TestRunner runner = createDefaultTestRunner(proc);
        runner.setProperty(PutSolrRecord.UPDATE_PATH, "/update");

        MockRecordParser recordParser = new MockRecordParser();
        recordParser.addRecord(1, "Abhinav","R",8,"Chemistry","term1", 98);
        runner.addControllerService("parser", recordParser);
        runner.enableControllerService(recordParser);
        runner.setProperty(PutSolrRecord.RECORD_READER, "parser");

        try {
            runner.enqueue(new byte[0], new HashMap<String, String>() {{
                put("id", "1");
            }});
            runner.run();

            runner.assertAllFlowFilesTransferred(PutSolrRecord.REL_CONNECTION_FAILURE, 1);
            verify(proc.getSolrClient(), times(1)).request(any(SolrRequest.class), eq((String)null));
        }finally {
            try {
                proc.getSolrClient().close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testSolrTypeCloudShouldRequireCollection() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(PutSolrRecord.class);
        MockRecordParser recordParser = new MockRecordParser();
        recordParser.addRecord(1, "Abhinav","R",8,"Chemistry","term1", 98);
        runner.addControllerService("parser", recordParser);
        runner.enableControllerService(recordParser);
        runner.setProperty(PutSolrRecord.RECORD_READER, "parser");
        runner.setProperty(SolrUtils.SOLR_TYPE, SolrUtils.SOLR_TYPE_CLOUD.getValue());
        runner.setProperty(SolrUtils.SOLR_LOCATION, "http://localhost:8443/solr");
        runner.assertNotValid();

        runner.setProperty(SolrUtils.COLLECTION, "someCollection1");
        runner.assertValid();
    }


    @Test
    public void testSolrTypeStandardShouldNotRequireCollection() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(PutSolrRecord.class);
        MockRecordParser recordParser = new MockRecordParser();
        recordParser.addRecord(1, "Abhinav","R",8,"Chemistry","term1", 98);
        runner.addControllerService("parser", recordParser);
        runner.enableControllerService(recordParser);
        runner.setProperty(PutSolrRecord.RECORD_READER, "parser");
        runner.setProperty(SolrUtils.SOLR_TYPE, SolrUtils.SOLR_TYPE_STANDARD.getValue());
        runner.setProperty(SolrUtils.SOLR_LOCATION, "http://localhost:8443/solr");
        runner.assertValid();
    }

    @Test
    public void testHttpsUrlShouldRequireSSLContext() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(PutSolrRecord.class);
        MockRecordParser recordParser = new MockRecordParser();
        recordParser.addRecord(1, "Abhinav","R",8,"Chemistry","term1", 98);
        runner.addControllerService("parser", recordParser);
        runner.enableControllerService(recordParser);
        runner.setProperty(PutSolrRecord.RECORD_READER, "parser");

        runner.setProperty(SolrUtils.SOLR_TYPE, SolrUtils.SOLR_TYPE_STANDARD.getValue());
        runner.setProperty(SolrUtils.SOLR_LOCATION, "https://localhost:8443/solr");
        runner.assertNotValid();

        final SSLContextService sslContextService = new MockSSLContextService();
        runner.addControllerService("ssl-context", sslContextService);
        runner.enableControllerService(sslContextService);

        runner.setProperty(SolrUtils.SSL_CONTEXT_SERVICE, "ssl-context");
        runner.assertValid();
    }

    @Test
    public void testHttpUrlShouldNotAllowSSLContext() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(PutSolrRecord.class);
        MockRecordParser recordParser = new MockRecordParser();
        recordParser.addRecord(1, "Abhinav","R",8,"Chemistry","term1", 98);
        runner.addControllerService("parser", recordParser);
        runner.enableControllerService(recordParser);
        runner.setProperty(PutSolrRecord.RECORD_READER, "parser");

        runner.setProperty(SolrUtils.SOLR_TYPE, SolrUtils.SOLR_TYPE_STANDARD.getValue());
        runner.setProperty(SolrUtils.SOLR_LOCATION, "http://localhost:8443/solr");
        runner.assertValid();

        final SSLContextService sslContextService = new MockSSLContextService();
        runner.addControllerService("ssl-context", sslContextService);
        runner.enableControllerService(sslContextService);

        runner.setProperty(SolrUtils.SSL_CONTEXT_SERVICE, "ssl-context");
        runner.assertNotValid();
    }

    @Test
    public void testUsernamePasswordValidation() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(PutSolrRecord.class);
        MockRecordParser recordParser = new MockRecordParser();
        recordParser.addRecord(1, "Abhinav","R",8,"Chemistry","term1", 98);
        runner.addControllerService("parser", recordParser);
        runner.enableControllerService(recordParser);
        runner.setProperty(PutSolrRecord.RECORD_READER, "parser");

        runner.setProperty(SolrUtils.SOLR_TYPE, SolrUtils.SOLR_TYPE_STANDARD.getValue());
        runner.setProperty(SolrUtils.SOLR_LOCATION, "http://localhost:8443/solr");
        runner.assertValid();

        runner.setProperty(SolrUtils.BASIC_USERNAME, "user1");
        runner.assertNotValid();

        runner.setProperty(SolrUtils.BASIC_PASSWORD, "password");
        runner.assertValid();

        runner.setProperty(SolrUtils.BASIC_USERNAME, "");
        runner.assertNotValid();

        runner.setProperty(SolrUtils.BASIC_USERNAME, "${solr.user}");
        runner.assertNotValid();

        runner.setVariable("solr.user", "solrRocks");
        runner.assertValid();

        runner.setProperty(SolrUtils.BASIC_PASSWORD, "${solr.password}");
        runner.assertNotValid();

        runner.setVariable("solr.password", "solrRocksPassword");
        runner.assertValid();
    }

    /**
     * Creates a base TestRunner with Solr Type of standard.
     */
    private static TestRunner createDefaultTestRunner(PutSolrRecord processor) {
        TestRunner runner = TestRunners.newTestRunner(processor);
        runner.setProperty(SolrUtils.SOLR_TYPE, SolrUtils.SOLR_TYPE_STANDARD.getValue());
        runner.setProperty(SolrUtils.SOLR_LOCATION, "http://localhost:8443/solr");
        return runner;
    }

    // Override createSolrClient and return the passed in SolrClient
    private class TestableProcessor extends PutSolrRecord {
        private SolrClient solrClient;

        public TestableProcessor(SolrClient solrClient) {
            this.solrClient = solrClient;
        }
        @Override
        protected SolrClient createSolrClient(ProcessContext context, String solrLocation) {
            return solrClient;
        }
    }

    // Create an EmbeddedSolrClient with the given core name.
    private static SolrClient createEmbeddedSolrClient(String coreName) throws IOException {
        String relPath = TestPutSolrRecord.class.getProtectionDomain()
                .getCodeSource().getLocation().getFile()
                + "../../target";

        return EmbeddedSolrServerFactory.create(
                EmbeddedSolrServerFactory.DEFAULT_SOLR_HOME,
                coreName, relPath);
    }

    // Override the createSolrClient method to inject a custom SolrClient.
    private class CollectionVerifyingProcessor extends PutSolrRecord {

        private SolrClient mockSolrClient;

        private final String expectedCollection;

        public CollectionVerifyingProcessor(final String expectedCollection) {
            this.expectedCollection = expectedCollection;
        }

        @Override
        protected SolrClient createSolrClient(ProcessContext context, String solrLocation) {
            mockSolrClient = new SolrClient() {
                @Override
                public NamedList<Object> request(SolrRequest solrRequest, String s) throws SolrServerException, IOException {
                    Assert.assertEquals(expectedCollection, solrRequest.getParams().get(PutSolrRecord.COLLECTION_PARAM_NAME));
                    return new NamedList<>();
                }

                @Override
                public void close() {

                }

            };
            return mockSolrClient;
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

    // Override the createSolrClient method to inject a Mock.
    private class ExceptionThrowingProcessor extends PutSolrRecord {

        private SolrClient mockSolrClient;
        private Throwable throwable;

        public ExceptionThrowingProcessor(Throwable throwable) {
            this.throwable = throwable;
        }

        @Override
        protected SolrClient createSolrClient(ProcessContext context, String solrLocation) {
            mockSolrClient = Mockito.mock(SolrClient.class);
            try {
                when(mockSolrClient.request(any(SolrRequest.class),
                        eq((String)null))).thenThrow(throwable);
            } catch (SolrServerException|IOException e) {
                Assert.fail(e.getMessage());
            }
            return mockSolrClient;
        }

    }


    /**
     * Mock implementation so we don't need to have a real keystore/truststore available for testing.
     */
    private class MockSSLContextService extends AbstractControllerService implements SSLContextService {

        @Override
        public SSLContext createSSLContext(ClientAuth clientAuth) throws ProcessException {
            return null;
        }

        @Override
        public String getTrustStoreFile() {
            return null;
        }

        @Override
        public String getTrustStoreType() {
            return null;
        }

        @Override
        public String getTrustStorePassword() {
            return null;
        }

        @Override
        public boolean isTrustStoreConfigured() {
            return false;
        }

        @Override
        public String getKeyStoreFile() {
            return null;
        }

        @Override
        public String getKeyStoreType() {
            return null;
        }

        @Override
        public String getKeyStorePassword() {
            return null;
        }

        @Override
        public String getKeyPassword() {
            return null;
        }

        @Override
        public boolean isKeyStoreConfigured() {
            return false;
        }

        @Override
        public String getSslAlgorithm() {
            return null;
        }
    }


}
