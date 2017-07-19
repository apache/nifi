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

package org.apache.nifi.processors.kudu;

import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.OperationResponse;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.mockito.Mockito.mock;

public class TestPutKudu {

    public static final String DEFAULT_TABLE_NAME = "Nifi-Kudu-Table";
    public static final String DEFAULT_MASTERS = "localhost:7051";
    public static final String DEFAULT_BATCH_SIZE = "25";

    private TestRunner testRunner;
    private MockPutKudu processor;

    @Before
    public void setUp() {
        processor = new MockPutKudu();
        testRunner = TestRunners.newTestRunner(processor);

        testRunner.setProperty(PutKudu.TABLE_NAME, DEFAULT_TABLE_NAME);
        testRunner.setProperty(PutKudu.KUDU_MASTERS, DEFAULT_MASTERS);
        testRunner.setProperty(PutKudu.BATCH_SIZE, DEFAULT_BATCH_SIZE);
    }

    @After
    public void close() {
        testRunner = null;
    }

    @Test
    public void testInsertOne() throws IOException {
        final String content = "{ \"field1\" : \"value1\", \"field2\" : \"value2\" }";
        byte[] bytes = content.getBytes("UTF-8");
        testRunner.enqueue(bytes);
        testRunner.run(1, true, true);
        testRunner.assertAllFlowFilesTransferred(PutKudu.REL_SUCCESS);

        MockFlowFile out = testRunner.getFlowFilesForRelationship(PutKudu.REL_SUCCESS).get(0);
        out.assertContentEquals(bytes);
    }

    @Test
    public void testInsertMany() throws Exception {
        final String content1 = "{ \"field1\" : \"value1\", \"field2\" : \"valu11\" }";
        final String content2 = "{ \"field1\" : \"value1\", \"field2\" : \"value11\" }";
        final String content3 = "{ \"field1\" : \"value3\", \"field2\" : \"value33\" }";

        testRunner.enqueue(content1.getBytes());
        testRunner.enqueue(content2.getBytes());
        testRunner.enqueue(content3.getBytes());

        testRunner.run(3);

        testRunner.assertAllFlowFilesTransferred(PutKudu.REL_SUCCESS, 3);
        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(PutKudu.REL_SUCCESS);

        flowFiles.get(0).assertContentEquals(content1.getBytes());
        flowFiles.get(1).assertContentEquals(content2.getBytes());
        flowFiles.get(2).assertContentEquals(content3.getBytes());
    }

    @Test
    public void testFailMany() throws Exception {
        final String content1 = "TestContent1";
        final String content2 = "TestContent2";

        //for testing failure case only
        MockFlowFile mff = new MockFlowFile(1234567890);

        testRunner.enqueue(content1.getBytes());
        testRunner.enqueue(content2.getBytes());
        testRunner.enqueue(mff);

        testRunner.run(3);

        testRunner.assertTransferCount(PutKudu.REL_SUCCESS, 2);
        testRunner.assertTransferCount(PutKudu.REL_FAILURE, 1);

        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(PutKudu.REL_SUCCESS);

        flowFiles.get(0).assertContentEquals(content1.getBytes());
        flowFiles.get(1).assertContentEquals(content2.getBytes());
    }

    private static class MockPutKudu extends PutKudu {

        @Override
        protected OperationResponse createPut(ProcessSession session, ProcessContext context, FlowFile flowFile) {
            //for testing failure case only
            if (flowFile.getId() == 1234567890)
                return null;

            return mock(OperationResponse.class);
        }

        @Override
        protected KuduClient getKuduConnection(String masters) {
            return mock(KuduClient.class);
        }
    }
}
