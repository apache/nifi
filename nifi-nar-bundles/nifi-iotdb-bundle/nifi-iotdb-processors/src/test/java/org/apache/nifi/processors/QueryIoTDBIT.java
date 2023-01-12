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
import org.apache.nifi.flowfile.attributes.CoreAttributes;
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
    private static final String WRITER_SERVICE_ID = "writer";

    private static final String DEVICE_ID = "root.sg7.d1";

    private static final String FIRST_MEASUREMENT = "s0";

    private static final String SECOND_MEASUREMENT = "s1";

    private static final long TIMESTAMP = 1;

    private TestRunner testRunner;
    private MockRecordWriter recordWriter;
    private Session session;

    @BeforeEach
    public void setRunner() throws IoTDBConnectionException, StatementExecutionException {
        testRunner = TestRunners.newTestRunner(QueryIoTDBRecord.class);
        recordWriter = new MockRecordWriter("header", true);
        testRunner.setProperty(QueryIoTDBRecord.RECORD_WRITER_FACTORY, WRITER_SERVICE_ID);
        testRunner.setProperty(QueryIoTDBRecord.IOTDB_HOST, "127.0.0.1");
        testRunner.setProperty(QueryIoTDBRecord.USERNAME, "root");
        testRunner.setProperty(QueryIoTDBRecord.PASSWORD, "root");
        session = new Session.Builder().build();
        session.open();

        List<String> measurements = new ArrayList<>(2);
        measurements.add(FIRST_MEASUREMENT);
        measurements.add(SECOND_MEASUREMENT);

        List<String> values = new ArrayList<>(2);
        values.add("5.0");
        values.add("6.0");
        session.insertRecord(DEVICE_ID, TIMESTAMP, measurements, values);
    }

    @AfterEach
    public void shutdown() throws Exception {
        testRunner.shutdown();
        recordWriter.disabled();
        session.close();
        EnvironmentUtils.cleanEnv();
        EnvironmentUtils.shutdownDaemon();
    }

    @Test
    public void testQueryIoTDBbyProperty() throws InitializationException {
        setUpStandardTestConfig();

        final String query = String.format("SELECT %s, %s FROM %s", FIRST_MEASUREMENT, SECOND_MEASUREMENT, DEVICE_ID);
        testRunner.setProperty(QueryIoTDBRecord.QUERY, query);
        testRunner.enqueue(new byte[]{});
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutIoTDBRecord.REL_SUCCESS, 1);

        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(PutIoTDBRecord.REL_SUCCESS).get(0);
        flowFile.assertContentEquals("header\n\"1\",\"5.0\",\"6.0\"\n");
        flowFile.assertAttributeExists(CoreAttributes.MIME_TYPE.key());
    }

    private void setUpStandardTestConfig() throws InitializationException {
        testRunner.addControllerService(WRITER_SERVICE_ID, recordWriter);
        testRunner.enableControllerService(recordWriter);
    }
}
