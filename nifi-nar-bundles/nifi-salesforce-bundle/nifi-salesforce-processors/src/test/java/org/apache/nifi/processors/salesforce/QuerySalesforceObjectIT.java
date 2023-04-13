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
import org.apache.nifi.processors.salesforce.util.CommonSalesforceProperties;
import org.apache.nifi.processors.salesforce.util.SalesforceConfigAware;
import org.apache.nifi.provenance.ProvenanceEventType;
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
class QuerySalesforceObjectIT implements SalesforceConfigAware {
    private TestRunner runner;

    @BeforeEach
    void setUp() throws Exception {
        Processor querySObject = new QuerySalesforceObject();

        runner = TestRunners.newTestRunner(querySObject);

        StandardOauth2AccessTokenProvider oauth2AccessTokenProvider = initOAuth2AccessTokenProvider(runner);
        runner.setProperty(CommonSalesforceProperties.TOKEN_PROVIDER, oauth2AccessTokenProvider.getIdentifier());
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

        runner.setProperty(QuerySalesforceObject.QUERY_TYPE, QuerySalesforceObject.PROPERTY_BASED_QUERY);
        runner.setProperty(QuerySalesforceObject.SOBJECT_NAME, sObjectName);
        runner.setProperty(QuerySalesforceObject.FIELD_NAMES, fieldNames);
        runner.setProperty(CommonSalesforceProperties.API_VERSION, VERSION);
        runner.setProperty(CommonSalesforceProperties.SALESFORCE_INSTANCE_URL, INSTANCE_URL);
        runner.setProperty(QuerySalesforceObject.RECORD_WRITER, writer.getIdentifier());
        runner.setProperty(QuerySalesforceObject.AGE_FIELD, "CreatedDate");
        runner.setProperty(QuerySalesforceObject.INITIAL_AGE_FILTER, "2022-01-06T08:43:24.000+0000");

        runner.run();

        List<MockFlowFile> results = runner.getFlowFilesForRelationship(QuerySalesforceObject.REL_SUCCESS);

        assertNotNull(results.get(0).getContent());
        runner.assertProvenanceEvent(ProvenanceEventType.RECEIVE);
    }

    @Test
    void runCustomQuery() {
        String customQuery = "SELECT Id, Name FROM Account";

        runner.setProperty(QuerySalesforceObject.QUERY_TYPE, QuerySalesforceObject.CUSTOM_QUERY);
        runner.setProperty(QuerySalesforceObject.CUSTOM_SOQL_QUERY, customQuery);
        runner.setProperty(CommonSalesforceProperties.API_VERSION, VERSION);
        runner.setProperty(CommonSalesforceProperties.SALESFORCE_INSTANCE_URL, INSTANCE_URL);

        runner.run();

        List<MockFlowFile> results = runner.getFlowFilesForRelationship(QuerySalesforceObject.REL_SUCCESS);

        assertNotNull(results.get(0).getContent());
    }
}
