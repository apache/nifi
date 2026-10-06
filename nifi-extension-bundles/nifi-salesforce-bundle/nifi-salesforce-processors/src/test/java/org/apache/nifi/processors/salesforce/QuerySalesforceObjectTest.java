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

import org.apache.nifi.components.state.Scope;
import org.apache.nifi.oauth2.OAuth2AccessTokenProvider;
import org.apache.nifi.processors.salesforce.rest.SalesforceConfiguration;
import org.apache.nifi.processors.salesforce.rest.SalesforceRestClient;
import org.apache.nifi.processors.salesforce.util.CommonSalesforceProperties;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.MockCsvRecordWriter;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.InputStream;
import java.time.LocalDate;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class QuerySalesforceObjectTest {

    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSX");
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss.SSSX");
    private static final String AGE_FIELD = "LastModifiedDate";
    private static final String ACCOUNT_STATE_KEY = QuerySalesforceObject.LAST_AGE_FILTER + ".account";

    private TestRunner testRunner;

    @Mock
    private SalesforceRestClient mockSalesforceRestClient;

    @BeforeEach
    void beforeEach() throws Exception {
        QuerySalesforceObject querySalesforceObject = new QuerySalesforceObject() {
            @Override
            SalesforceRestClient getSalesforceRestClient(SalesforceConfiguration salesforceConfiguration) {
                return mockSalesforceRestClient;
            }
        };

        testRunner = TestRunners.newTestRunner(querySalesforceObject);

        testRunner.setProperty(CommonSalesforceProperties.SALESFORCE_INSTANCE_URL, "http://localhost");

        String accessTokenServiceId = "access_token_service";
        OAuth2AccessTokenProvider accessTokenService = mock(OAuth2AccessTokenProvider.class);
        when(accessTokenService.getIdentifier()).thenReturn(accessTokenServiceId);
        testRunner.addControllerService(accessTokenServiceId, accessTokenService);
        testRunner.enableControllerService(accessTokenService);
        testRunner.setProperty(CommonSalesforceProperties.TOKEN_PROVIDER, accessTokenServiceId);
    }

    @Test
    void testDateTimeFields() throws Exception {
        // values from query_sobject.json
        String date = "2025-12-16";
        String dateTime = "2025-12-16T13:05:30.000+0000";
        String time = "13:05:30.000Z";

        String sObjectName = "TestObject";
        testRunner.setProperty(QuerySalesforceObject.SOBJECT_NAME, sObjectName);

        setUpRecordWriter();

        when(mockSalesforceRestClient.describeSObject(sObjectName)).thenReturn(getResourceAsStream("query/describe_sobject.json"));
        when(mockSalesforceRestClient.query(any())).thenReturn(getResourceAsStream("query/query_sobject.json"));

        testRunner.run();

        testRunner.assertTransferCount(QuerySalesforceObject.REL_SUCCESS, 1);

        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(QuerySalesforceObject.REL_SUCCESS).getFirst();
        assertNotNull(flowFile);

        String content = flowFile.getContent();
        assertNotNull(content);

        String[] fields = content.split(",");
        assertEquals(4, fields.length);

        assertEquals(date, fields[1].trim());

        String expectedDateTime = ZonedDateTime.parse(dateTime, DATE_TIME_FORMATTER)
                .withZoneSameInstant(ZoneId.systemDefault())
                .format(DATE_TIME_FORMATTER);
        assertEquals(expectedDateTime, fields[2].trim());

        String expectedTime = OffsetTime.parse(time, TIME_FORMATTER)
                .atDate(LocalDate.EPOCH)
                .toZonedDateTime()
                .withZoneSameInstant(ZoneId.systemDefault())
                .format(TIME_FORMATTER);
        assertEquals(expectedTime, fields[3].trim());
    }

    @Test
    void testSObjectNameEvaluatedWithFlowFileAttributes() throws Exception {
        String sObjectName = "TestObject";
        testRunner.setProperty(QuerySalesforceObject.SOBJECT_NAME, "${object}");
        setUpRecordWriter();

        when(mockSalesforceRestClient.describeSObject(sObjectName)).thenReturn(getResourceAsStream("query/describe_sobject.json"));
        when(mockSalesforceRestClient.query(any())).thenReturn(getResourceAsStream("query/query_sobject.json"));

        testRunner.enqueue("", Map.of("object", sObjectName));
        testRunner.run();

        testRunner.assertTransferCount(QuerySalesforceObject.REL_SUCCESS, 1);
        testRunner.assertTransferCount(QuerySalesforceObject.REL_ORIGINAL, 1);
        testRunner.assertTransferCount(QuerySalesforceObject.REL_FAILURE, 0);
        verify(mockSalesforceRestClient).describeSObject(sObjectName);
    }

    @Test
    void testFieldNamesEvaluatedWithFlowFileAttributes() throws Exception {
        testRunner.setProperty(QuerySalesforceObject.SOBJECT_NAME, "TestObject");
        testRunner.setProperty(QuerySalesforceObject.FIELD_NAMES, "${fields}");
        setUpRecordWriter();

        when(mockSalesforceRestClient.describeSObject("TestObject")).thenReturn(getResourceAsStream("query/describe_sobject.json"));
        when(mockSalesforceRestClient.query(any())).thenReturn(getResourceAsStream("query/query_sobject.json"));

        testRunner.enqueue("", Map.of("fields", "id__c,date__c"));
        testRunner.run();

        ArgumentCaptor<String> query = ArgumentCaptor.forClass(String.class);
        verify(mockSalesforceRestClient).query(query.capture());
        assertEquals("SELECT id__c,date__c FROM TestObject", query.getValue());
        testRunner.assertTransferCount(QuerySalesforceObject.REL_SUCCESS, 1);
    }

    @Test
    void testAgeFilterStoredSeparatelyForEachSObject() throws Exception {
        testRunner.setProperty(QuerySalesforceObject.SOBJECT_NAME, "${object}");
        testRunner.setProperty(QuerySalesforceObject.AGE_FIELD, AGE_FIELD);
        setUpRecordWriter();

        when(mockSalesforceRestClient.describeSObject(any())).thenAnswer(invocation -> getResourceAsStream("query/describe_sobject.json"));
        when(mockSalesforceRestClient.query(any())).thenAnswer(invocation -> getResourceAsStream("query/query_sobject.json"));

        testRunner.enqueue("", Map.of("object", "Account"));
        testRunner.run();
        String accountAgeFilter = testRunner.getStateManager().getState(Scope.CLUSTER).get(ACCOUNT_STATE_KEY);
        assertNotNull(accountAgeFilter);

        testRunner.enqueue("", Map.of("object", "Contact"));
        testRunner.run();

        testRunner.enqueue("", Map.of("object", "Account"));
        testRunner.run();

        ArgumentCaptor<String> queries = ArgumentCaptor.forClass(String.class);
        verify(mockSalesforceRestClient, times(3)).query(queries.capture());
        List<String> executedQueries = queries.getAllValues();

        assertFalse(executedQueries.get(0).contains(AGE_FIELD + " >= "));
        assertFalse(executedQueries.get(1).contains(AGE_FIELD + " >= "), "Contact must not use the age filter stored for Account");
        assertTrue(executedQueries.get(2).contains(AGE_FIELD + " >= " + accountAgeFilter));

        Map<String, String> state = testRunner.getStateManager().getState(Scope.CLUSTER).toMap();
        assertNotNull(state.get(ACCOUNT_STATE_KEY));
        assertNotNull(state.get(QuerySalesforceObject.LAST_AGE_FILTER + ".contact"));
        assertNull(state.get(QuerySalesforceObject.LAST_AGE_FILTER));
    }

    @Test
    void testAgeFilterStoredWithoutSObjectIsMigrated() throws Exception {
        String previousAgeFilter = "2020-01-01T00:00:00.000+0000";
        testRunner.setProperty(QuerySalesforceObject.SOBJECT_NAME, "Account");
        testRunner.setProperty(QuerySalesforceObject.AGE_FIELD, AGE_FIELD);
        setUpRecordWriter();
        testRunner.getStateManager().setState(Map.of(QuerySalesforceObject.LAST_AGE_FILTER, previousAgeFilter), Scope.CLUSTER);

        when(mockSalesforceRestClient.describeSObject("Account")).thenReturn(getResourceAsStream("query/describe_sobject.json"));
        when(mockSalesforceRestClient.query(any())).thenReturn(getResourceAsStream("query/query_sobject.json"));

        testRunner.run();

        ArgumentCaptor<String> query = ArgumentCaptor.forClass(String.class);
        verify(mockSalesforceRestClient).query(query.capture());
        assertTrue(query.getValue().contains(AGE_FIELD + " >= " + previousAgeFilter));

        Map<String, String> state = testRunner.getStateManager().getState(Scope.CLUSTER).toMap();
        assertNotNull(state.get(ACCOUNT_STATE_KEY));
        assertNull(state.get(QuerySalesforceObject.LAST_AGE_FILTER));
    }

    private void setUpRecordWriter() throws Exception {
        String recordWriterServiceId = "record_writer_service";
        RecordSetWriterFactory recordWriterService = MockCsvRecordWriter.builder()
                .quoteValues(false)
                .build();
        testRunner.addControllerService(recordWriterServiceId, recordWriterService);
        testRunner.enableControllerService(recordWriterService);
        testRunner.setProperty(QuerySalesforceObject.RECORD_WRITER, recordWriterServiceId);
    }

    private InputStream getResourceAsStream(String resourceName) {
        return getClass().getClassLoader().getResourceAsStream(resourceName);
    }
}
