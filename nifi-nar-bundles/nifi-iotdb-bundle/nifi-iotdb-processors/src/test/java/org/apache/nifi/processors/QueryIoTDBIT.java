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
package org.apache.nifi.processors;

import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.util.MockFlowFile;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class QueryIoTDBIT {
    private static TestRunner testRunner;
    private static MockRecordWriter recordWriter;
    private static Session session;

    @BeforeEach
    public void init() throws IoTDBConnectionException, IoTDBConnectionException, StatementExecutionException {
        testRunner = TestRunners.newTestRunner(QueryIoTDBRecord.class);
        recordWriter = new MockRecordWriter("header", true);
        testRunner.setProperty(QueryIoTDBRecord.RECORD_WRITER_FACTORY, "writer");
        testRunner.setProperty("Host", "127.0.0.1");
        testRunner.setProperty("Username", "root");
        testRunner.setProperty("Password", "root");
        session = new Session.Builder().build();
        session.open();

        String deviceId = "root.sg7.d1";
        List<String> measurements = new ArrayList<>(2);
        measurements.add("s0");
        measurements.add("s1");

        List<String> values = new ArrayList<>(2);
        values.add("5.0");
        values.add("6.0");
        session.insertRecord(deviceId,1L,measurements,values);
    }

    @AfterEach
    public void release() throws Exception {
        testRunner.shutdown();
        recordWriter.disabled();
        session.close();
        EnvironmentUtils.cleanEnv();
        EnvironmentUtils.shutdownDaemon();
    }

    private void setUpStandardTestConfig() throws InitializationException {
        testRunner.addControllerService("writer", recordWriter);
        testRunner.enableControllerService(recordWriter);
    }

    @Test
    public void testQueryIoTDBbyProperty()
            throws  InitializationException {
        setUpStandardTestConfig();

        // call the QueryIoTDBProcessor
        testRunner.setProperty("iotdb-query", "select s0,s1 from root.sg7.d1");
        testRunner.enqueue("");
        testRunner.run();

        // test whether transferred successfully?
        testRunner.assertAllFlowFilesTransferred(PutIoTDBRecord.REL_SUCCESS, 1);

        final MockFlowFile out = testRunner.getFlowFilesForRelationship(PutIoTDBRecord.REL_SUCCESS).get(0);
        out.assertContentEquals("header\n\"1\",\"5.0\",\"6.0\"\n");
    }

    @Test
    public void testQueryIoTDBbyEnqueue()
            throws  InitializationException {
        setUpStandardTestConfig();

        // call the QueryIoTDBProcessor
        testRunner.enqueue("select s1 from root.sg7.d1");
        testRunner.run();

        // test whether transferred successfully?
        testRunner.assertAllFlowFilesTransferred(PutIoTDBRecord.REL_SUCCESS, 1);

        final MockFlowFile out = testRunner.getFlowFilesForRelationship(PutIoTDBRecord.REL_SUCCESS).get(0);
        out.assertContentEquals("header\n\"1\",\"6.0\"\n");
    }
}
