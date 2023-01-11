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
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.sql.Timestamp;

public class PutIoTDBRecordIT {
    private TestRunner testRunner;
    private MockRecordParser recordReader;

    @BeforeEach
    public void setRunner() {
        testRunner = TestRunners.newTestRunner(PutIoTDBRecord.class);
        recordReader = new MockRecordParser();
        testRunner.setProperty(PutIoTDBRecord.RECORD_READER_FACTORY, "reader");
        testRunner.setProperty(PutIoTDBRecord.IOTDB_HOST, "127.0.0.1");
        testRunner.setProperty(PutIoTDBRecord.USERNAME, "root");
        testRunner.setProperty(PutIoTDBRecord.PASSWORD, "root");
        testRunner.setProperty(PutIoTDBRecord.MAX_ROW_NUMBER, "1024");
        EnvironmentUtils.envSetUp();
    }

    @AfterEach
    public void shutdown() throws Exception {
        testRunner.shutdown();
        recordReader.disabled();
        EnvironmentUtils.cleanEnv();
        EnvironmentUtils.shutdownDaemon();
    }

    private void setUpStandardTestConfig() throws InitializationException {
        testRunner.addControllerService("reader", recordReader);
        testRunner.enableControllerService(recordReader);
    }

    @Test
    public void testInsertByNativeSchemaWithSingleDevice() throws InitializationException {
        setUpStandardTestConfig();

        recordReader.addSchemaField("TIME", RecordFieldType.LONG);
        recordReader.addSchemaField("s1", RecordFieldType.INT);
        recordReader.addSchemaField("s2", RecordFieldType.LONG);
        recordReader.addSchemaField("s3", RecordFieldType.FLOAT);
        recordReader.addSchemaField("s4", RecordFieldType.DOUBLE);
        recordReader.addSchemaField("s5", RecordFieldType.BOOLEAN);
        recordReader.addSchemaField("s6", RecordFieldType.STRING);

        recordReader.addRecord(1L, 1, 2L, 3.0F, 4.0D, true, "abc");
        recordReader.addRecord(2L, 1, 2L, 3.0F, 4.0D, true, "abc");
        recordReader.addRecord(3L, 1, 2L, 3.0F, 4.0D, true, "abc");
        recordReader.addRecord(4L, 1, 2L, 3.0F, 4.0D, true, "abc");
        recordReader.addRecord(5L, 1, 2L, 3.0F, 4.0D, true, "abc");
        recordReader.addRecord(6L, 1, 2L, 3.0F, 4.0D, true, "abc");
        recordReader.addRecord(7L, 1, 2L, 3.0F, 4.0D, true, "abc");

        testRunner.setProperty(PutIoTDBRecord.TIME_FIELD, "TIME");
        testRunner.setProperty(PutIoTDBRecord.PREFIX, "root.sg0.d1.");
        testRunner.enqueue("");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutIoTDBRecord.REL_SUCCESS, 1);
    }

    @Test
    public void testInsertByNativeSchemaWithTimeStamp() throws InitializationException {
        setUpStandardTestConfig();

        recordReader.addSchemaField("Time", RecordFieldType.TIMESTAMP);
        recordReader.addSchemaField("s1", RecordFieldType.INT);
        recordReader.addSchemaField("s2", RecordFieldType.LONG);
        recordReader.addSchemaField("s3", RecordFieldType.FLOAT);
        recordReader.addSchemaField("s4", RecordFieldType.DOUBLE);
        recordReader.addSchemaField("s5", RecordFieldType.BOOLEAN);
        recordReader.addSchemaField("s6", RecordFieldType.STRING);

        recordReader.addRecord(new Timestamp(1L), 1, 2L, 3.0F, 4.0D, true, "abc");
        recordReader.addRecord(new Timestamp(2L), 1, 2L, 3.0F, 4.0D, true, "abc");
        recordReader.addRecord(new Timestamp(3L), 1, 2L, 3.0F, 4.0D, true, "abc");
        recordReader.addRecord(new Timestamp(4L), 1, 2L, 3.0F, 4.0D, true, "abc");
        recordReader.addRecord(new Timestamp(5L), 1, 2L, 3.0F, 4.0D, true, "abc");
        recordReader.addRecord(new Timestamp(6L), 1, 2L, 3.0F, 4.0D, true, "abc");
        recordReader.addRecord(new Timestamp(7L), 1, 2L, 3.0F, 4.0D, true, "abc");

        testRunner.setProperty(PutIoTDBRecord.PREFIX, "root.sg1.d1.");
        testRunner.enqueue("");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutIoTDBRecord.REL_SUCCESS, 1);
    }

    @Test
    public void testInsertByNativeSchemaWithNullValue() throws InitializationException {
        setUpStandardTestConfig();

        recordReader.addSchemaField("Time", RecordFieldType.LONG);
        recordReader.addSchemaField("s1", RecordFieldType.INT);
        recordReader.addSchemaField("s2", RecordFieldType.LONG);

        recordReader.addRecord(1L, 1, 2L);
        recordReader.addRecord(2L, 1, 2L);
        recordReader.addRecord(3L, 1, null);

        testRunner.setProperty(PutIoTDBRecord.PREFIX, "root.sg2.d1.");
        testRunner.enqueue("");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutIoTDBRecord.REL_SUCCESS, 1);
    }

    @Test
    public void testInsertByNativeSchemaWithEmptyValue() throws InitializationException {
        setUpStandardTestConfig();

        recordReader.addSchemaField("Time", RecordFieldType.LONG);
        recordReader.addSchemaField("s1", RecordFieldType.INT);
        recordReader.addSchemaField("s2", RecordFieldType.LONG);

        testRunner.setProperty(PutIoTDBRecord.PREFIX, "root.sg3.d1.");
        testRunner.enqueue("");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutIoTDBRecord.REL_SUCCESS, 1);
    }

    @Test
    public void testInsertByNativeSchemaWithUnsupportedDataType() throws InitializationException {
        setUpStandardTestConfig();

        recordReader.addSchemaField("Time", RecordFieldType.LONG);
        recordReader.addSchemaField("s1", RecordFieldType.ARRAY);

        recordReader.addRecord(1L, new String[]{"1"});

        testRunner.setProperty(PutIoTDBRecord.PREFIX, "root.sg4.d1.");
        testRunner.enqueue("");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutIoTDBRecord.REL_FAILURE, 1);
    }

    @Test
    public void testInsertByNativeSchemaWithoutTimeField() throws InitializationException {
        setUpStandardTestConfig();

        recordReader.addSchemaField("s1", RecordFieldType.INT);
        recordReader.addSchemaField("s2", RecordFieldType.INT);
        recordReader.addRecord(1, 1);

        testRunner.setProperty(PutIoTDBRecord.PREFIX, "root.sg5.d1.");
        testRunner.enqueue("");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutIoTDBRecord.REL_FAILURE, 1);
    }

    @Test
    public void testInsertByNativeSchemaWithWrongTimeType() throws InitializationException {
        setUpStandardTestConfig();

        recordReader.addSchemaField("Time", RecordFieldType.INT);
        recordReader.addSchemaField("s1", RecordFieldType.INT);

        recordReader.addRecord(1, 1);

        testRunner.setProperty(PutIoTDBRecord.PREFIX, "root.sg5.d1.");
        testRunner.enqueue(new byte[]{});
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutIoTDBRecord.REL_FAILURE, 1);
    }

    @Test
    public void testInsertByNativeSchemaNotStartWithRoot() throws InitializationException {
        setUpStandardTestConfig();

        recordReader.addSchemaField("Time", RecordFieldType.LONG);
        recordReader.addSchemaField("s1", RecordFieldType.INT);

        recordReader.addRecord(1L, 1);

        testRunner.setProperty(PutIoTDBRecord.PREFIX, "sg6.d1.");
        testRunner.enqueue(new byte[]{});
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutIoTDBRecord.REL_FAILURE, 1);
    }
}
