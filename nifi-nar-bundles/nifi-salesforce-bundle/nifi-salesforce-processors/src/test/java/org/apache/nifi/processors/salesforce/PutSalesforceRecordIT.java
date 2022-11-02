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
package org.apache.nifi.processors.salesforce;

import org.apache.nifi.oauth2.StandardOauth2AccessTokenProvider;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processors.salesforce.util.SalesforceConfigAware;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.MockComponentLog;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class PutSalesforceRecordIT implements SalesforceConfigAware {

    private TestRunner runner;

    @BeforeEach
    void setUp() throws Exception {
        int maxRecordCount = 2;
        Processor putSalesforceRecord = new CustomPutSalesforceRecord(maxRecordCount);

        runner = TestRunners.newTestRunner(putSalesforceRecord);

        StandardOauth2AccessTokenProvider oauth2AccessTokenProvider = initOAuth2AccessTokenProvider(runner);
        runner.setProperty(QuerySalesforceObject.TOKEN_PROVIDER, oauth2AccessTokenProvider.getIdentifier());
    }

    @Test
    void testPutSalesforceRecord() throws Exception {
        MockComponentLog mockComponentLog = new MockComponentLog("id1", "testPutSalesforceRecord");
        InputStream in = readFile("src/test/resources/json/put_records.json");

        MockFlowFile flowFile = new MockFlowFile(1L);
        byte[] fileContent = Files.readAllBytes(Paths.get("src/test/resources/json/put_records.json"));
        flowFile.setData(fileContent);
        flowFile.putAttributes(Collections.singletonMap("objectType", "Account"));

        MockRecordParser reader = new MockRecordParser();
        reader.addSchemaField("name", RecordFieldType.STRING);
        reader.addSchemaField("phone", RecordFieldType.STRING);
        reader.addSchemaField("website", RecordFieldType.STRING);
        reader.addSchemaField("numberOfEmployees", RecordFieldType.STRING);
        reader.addSchemaField("industry", RecordFieldType.STRING);

        reader.addRecord("SampleAccount1", "111111", "www.salesforce1.com", "100", "Banking");
        reader.addRecord("SampleAccount2", "222222", "www.salesforce2.com", "200", "Banking");
        reader.addRecord("SampleAccount3", "333333", "www.salesforce3.com", "300", "Banking");
        reader.addRecord("SampleAccount4", "444444", "www.salesforce4.com", "400", "Banking");
        reader.addRecord("SampleAccount5", "555555", "www.salesforce5.com", "500", "Banking");

        reader.createRecordReader(flowFile, in, mockComponentLog);

        runner.addControllerService("reader", reader);
        runner.enableControllerService(reader);

        runner.setProperty(PutSalesforceRecord.API_VERSION, VERSION);
        runner.setProperty(PutSalesforceRecord.API_URL, BASE_URL);
        runner.setProperty(PutSalesforceRecord.RECORD_READER_FACTORY, reader.getIdentifier());


        runner.enqueue(flowFile);
        runner.run();

        List<MockFlowFile> results = runner.getFlowFilesForRelationship(QuerySalesforceObject.REL_SUCCESS);

        assertNotNull(results);
    }

    static class CustomPutSalesforceRecord extends PutSalesforceRecord {
        private final int maxRecordCount;

        public CustomPutSalesforceRecord(int maxRecordCount) {
            this.maxRecordCount = maxRecordCount;
        }

        @Override
        int getMaxRecordCount() {
            return maxRecordCount;
        }
    }

    private InputStream readFile(final String path) throws IOException {
        return new FileInputStream(path);
    }
}
