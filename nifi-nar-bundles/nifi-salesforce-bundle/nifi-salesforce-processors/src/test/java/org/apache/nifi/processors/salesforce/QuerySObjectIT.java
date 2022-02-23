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
import org.apache.nifi.serialization.DateTimeUtils;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Set constants in {@link SalesforceConfigAware}
 */
class QuerySObjectIT implements SalesforceConfigAware {
    private TestRunner runner;

    @BeforeEach
    void setUp() throws Exception {
        Processor querySObject = new QuerySObject();

        runner = TestRunners.newTestRunner(querySObject);

        StandardOauth2AccessTokenProvider oauth2AccessTokenProvider = initOAuth2AccessTokenProvider(runner);
        runner.setProperty(QuerySObject.AUTH_SERVICE, oauth2AccessTokenProvider.getIdentifier());
    }

    @AfterEach
    void tearDown() {
        runner.shutdown();
    }

    @Test
    void retrievesAndWritesRecords() throws Exception {
        String sObjectName = "Account";
        String fieldNames = "Id,name,CreatedDate";

        RecordSetWriterFactory writer = new MockRecordWriter();
        runner.addControllerService("writer", writer);
        runner.enableControllerService(writer);

        runner.setProperty(QuerySObject.SOBJECT_NAME, sObjectName);
        runner.setProperty(QuerySObject.FIELD_NAMES, fieldNames);
        runner.setProperty(QuerySObject.API_VERSION, VERSION);
        runner.setProperty(QuerySObject.BASE_URL, BASE_URL);
        runner.setProperty(DateTimeUtils.TIMESTAMP_FORMAT, "yyyy-MM-dd'T'HH:mm:ss.SSSZZZZ");
        runner.setProperty(QuerySObject.RECORD_WRITER, writer.getIdentifier());
        runner.setProperty(QuerySObject.AGE_FIELD, "CreatedDate");
        runner.setProperty(QuerySObject.INITIAL_AGE_FILTER, "2022-01-06T08:43:24.000+0000");

        runner.run();

        List<MockFlowFile> results = runner.getFlowFilesForRelationship(QuerySObject.REL_SUCCESS);

        assertNotNull(results.get(0).getContent());
    }
}
