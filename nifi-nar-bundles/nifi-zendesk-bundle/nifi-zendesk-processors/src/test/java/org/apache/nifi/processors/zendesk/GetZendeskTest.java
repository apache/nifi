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

package org.apache.nifi.processors.zendesk;

import okhttp3.HttpUrl;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.apache.nifi.common.zendesk.ZendeskAuthenticationType;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.web.client.StandardHttpUriBuilder;
import org.apache.nifi.web.client.api.HttpUriBuilder;
import org.apache.nifi.web.client.provider.service.StandardWebClientServiceProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opentest4j.AssertionFailedError;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;
import static org.apache.nifi.common.zendesk.ZendeskProperties.WEB_CLIENT_SERVICE_PROVIDER;
import static org.apache.nifi.common.zendesk.ZendeskProperties.ZENDESK_AUTHENTICATION_CREDENTIAL;
import static org.apache.nifi.common.zendesk.ZendeskProperties.ZENDESK_AUTHENTICATION_TYPE;
import static org.apache.nifi.common.zendesk.ZendeskProperties.ZENDESK_SUBDOMAIN;
import static org.apache.nifi.common.zendesk.ZendeskProperties.ZENDESK_USER;
import static org.apache.nifi.components.state.Scope.CLUSTER;
import static org.apache.nifi.processors.zendesk.GetZendesk.HTTP_TOO_MANY_REQUESTS;
import static org.apache.nifi.processors.zendesk.GetZendesk.RECORD_COUNT_ATTRIBUTE_NAME;
import static org.apache.nifi.processors.zendesk.GetZendesk.REL_SUCCESS;
import static org.apache.nifi.processors.zendesk.GetZendesk.ZENDESK_EXPORT_METHOD;
import static org.apache.nifi.processors.zendesk.GetZendesk.ZENDESK_QUERY_START_TIMESTAMP;
import static org.apache.nifi.processors.zendesk.GetZendesk.ZENDESK_RESOURCE;
import static org.apache.nifi.processors.zendesk.ZendeskExportMethod.CURSOR;
import static org.apache.nifi.processors.zendesk.ZendeskResource.TICKETS;
import static org.apache.nifi.util.TestRunners.newTestRunner;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class GetZendeskTest {

    private static final int HTTP_OK = 200;
    private static final int HTTP_BAD_REQUEST = 400;

    private static final String DEFAULT_QUERY_START_TIMESTAMP = "1640995200";
    private static final String DEFAULT_CURSOR_VALUE = "123456789";
    private static final String EMPTY_RESPONSE = "{}";
    private static final String RESPONSE_WITH_CURSOR_FIELD_TEMPLATE = "{ \"%s\": " + DEFAULT_CURSOR_VALUE + " }";
    private static final String THREE_RECORDS = "[{\"id\":1},{\"id\":2},{\"id\":3}]";
    private static final String RESPONSE_WITH_THREE_RECORDS_TEMPLATE = "{ \"%s\": " + THREE_RECORDS + " }";
    private static final String RESPONSE_WITH_ZERO_RECORDS_TEMPLATE = "{ \"%s\": [] }";

    private MockWebServer server;
    private TestRunner testRunner;

    private static Stream<Arguments> supportedZendeskResourcesExportMethodCombinations() {
        return Stream.of(ZendeskResource.values())
            .flatMap(zendeskResource -> zendeskResource.getSupportedExportMethods()
                .stream()
                .map(exportMethod -> Arguments.of(zendeskResource, exportMethod)));
    }

    private static Stream<Arguments> unsupportedZendeskResourcesExportMethodCombinations() {
        return Stream.of(ZendeskResource.values())
            .flatMap(zendeskResource -> {
                List<ZendeskExportMethod> zendeskExportMethods = new ArrayList<>(asList(ZendeskExportMethod.values()));
                zendeskExportMethods.removeAll(zendeskResource.getSupportedExportMethods());
                return zendeskExportMethods.stream()
                    .map(exportMethod -> Arguments.of(zendeskResource, exportMethod));
            });
    }

    @BeforeEach
    public void init() throws IOException, InitializationException {
        server = new MockWebServer();
        server.start();

        testRunner = newTestRunner(new TestGetZendesk());

        StandardWebClientServiceProvider standardWebClientServiceProvider = new StandardWebClientServiceProvider();
        String standardWebClientServiceProviderId = "standardWebClientServiceProvider";
        testRunner.addControllerService(standardWebClientServiceProviderId, standardWebClientServiceProvider);
        testRunner.enableControllerService(standardWebClientServiceProvider);

        testRunner.setProperty(WEB_CLIENT_SERVICE_PROVIDER, standardWebClientServiceProviderId);
        testRunner.setProperty(ZENDESK_SUBDOMAIN, "default-zendesk-subdomain");
        testRunner.setProperty(ZENDESK_USER, "default-zendesk-user-name");
        testRunner.setProperty(ZENDESK_AUTHENTICATION_TYPE, ZendeskAuthenticationType.PASSWORD);
        testRunner.setProperty(ZENDESK_AUTHENTICATION_CREDENTIAL, "default-zendesk-password");
        testRunner.setProperty(ZENDESK_QUERY_START_TIMESTAMP, DEFAULT_QUERY_START_TIMESTAMP);
    }

    @AfterEach
    void tearDown() throws IOException {
        server.shutdown();
    }

    @ParameterizedTest
    @MethodSource("supportedZendeskResourcesExportMethodCombinations")
    public void testQueryStartTimestampIsUsedWhenNoStateIsAvailable(ZendeskResource zendeskResource, ZendeskExportMethod exportMethod) throws InterruptedException {
        // given
        server.enqueue(new MockResponse().setResponseCode(HTTP_OK).setBody(EMPTY_RESPONSE));
        testRunner.setProperty(ZENDESK_RESOURCE, zendeskResource);
        testRunner.setProperty(ZENDESK_EXPORT_METHOD, exportMethod);

        // when
        testRunner.run(1);

        // then
        RecordedRequest request = server.takeRequest();
        assertEquals(
            zendeskResource.apiPath(exportMethod) + "?" + exportMethod.getInitialCursorQueryParameterName() + "=" + DEFAULT_QUERY_START_TIMESTAMP,
            request.getPath());
    }

    @ParameterizedTest
    @MethodSource("supportedZendeskResourcesExportMethodCombinations")
    public void testCursorFromStateIsUsedWhenStateIsAvailable(ZendeskResource zendeskResource, ZendeskExportMethod exportMethod) throws InterruptedException, IOException {
        // given
        server.enqueue(new MockResponse().setResponseCode(HTTP_OK).setBody(EMPTY_RESPONSE));
        testRunner.setProperty(ZENDESK_RESOURCE, zendeskResource);
        testRunner.setProperty(ZENDESK_EXPORT_METHOD, exportMethod);
        testRunner.getStateManager().setState(singletonMap(zendeskResource.getValue() + exportMethod.getValue(), DEFAULT_CURSOR_VALUE), CLUSTER);

        // when
        testRunner.run(1);

        // then
        RecordedRequest request = server.takeRequest();
        assertEquals(
            zendeskResource.apiPath(exportMethod) + "?" + exportMethod.getCursorQueryParameterName() + "=" + DEFAULT_CURSOR_VALUE,
            request.getPath());
    }

    @ParameterizedTest
    @MethodSource("supportedZendeskResourcesExportMethodCombinations")
    public void testCursorPositionIsStoredInState(ZendeskResource zendeskResource, ZendeskExportMethod exportMethod) throws IOException {
        // given
        server.enqueue(new MockResponse().setResponseCode(HTTP_OK)
            .setBody(format(RESPONSE_WITH_CURSOR_FIELD_TEMPLATE, exportMethod.getCursorJsonFieldName())));
        testRunner.setProperty(ZENDESK_RESOURCE, zendeskResource);
        testRunner.setProperty(ZENDESK_EXPORT_METHOD, exportMethod);
        String stateKey = zendeskResource.getValue() + exportMethod.getValue();
        assertNull(testRunner.getStateManager().getState(CLUSTER).get(stateKey));

        // when
        testRunner.run(1);

        // then
        assertEquals(DEFAULT_CURSOR_VALUE, testRunner.getStateManager().getState(CLUSTER).get(stateKey));
    }

    @ParameterizedTest
    @MethodSource("supportedZendeskResourcesExportMethodCombinations")
    public void testFlowFileIsCreatedAndContentIsAddedAndFlowFileAttributeIsSet(ZendeskResource zendeskResource, ZendeskExportMethod exportMethod) throws InterruptedException {
        // given
        server.enqueue(new MockResponse().setResponseCode(HTTP_OK)
            .setBody(format(RESPONSE_WITH_THREE_RECORDS_TEMPLATE, zendeskResource.getResponseFieldName())));
        testRunner.setProperty(ZENDESK_RESOURCE, zendeskResource);
        testRunner.setProperty(ZENDESK_EXPORT_METHOD, exportMethod);

        // when
        testRunner.run(1);

        // then
        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(REL_SUCCESS);
        assertEquals(1, flowFiles.size());
        MockFlowFile resultFlowFile = flowFiles.getFirst();
        assertEquals("3", resultFlowFile.getAttribute(RECORD_COUNT_ATTRIBUTE_NAME));
        assertEquals(THREE_RECORDS, resultFlowFile.getContent());
    }

    @ParameterizedTest
    @MethodSource("supportedZendeskResourcesExportMethodCombinations")
    public void testNoFlowFileIsEmittedWhenZeroRecordsAreSent(ZendeskResource zendeskResource, ZendeskExportMethod exportMethod) throws InterruptedException {
        // given
        server.enqueue(new MockResponse().setResponseCode(HTTP_OK)
            .setBody(format(RESPONSE_WITH_ZERO_RECORDS_TEMPLATE, zendeskResource.getResponseFieldName())));
        testRunner.setProperty(ZENDESK_RESOURCE, zendeskResource);
        testRunner.setProperty(ZENDESK_EXPORT_METHOD, exportMethod);

        // when
        testRunner.run(1);

        // then
        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(REL_SUCCESS);
        assertEquals(0, flowFiles.size());
    }

    @Test
    public void testNoFlowFileIsEmittedWhenTooManyRequestResponseCodeReceived() {
        // given
        server.enqueue(new MockResponse().setResponseCode(HTTP_TOO_MANY_REQUESTS));
        testRunner.setProperty(ZENDESK_RESOURCE, TICKETS);
        testRunner.setProperty(ZENDESK_EXPORT_METHOD, CURSOR);

        // when
        testRunner.run(1);

        // then
        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(REL_SUCCESS);
        assertEquals(0, flowFiles.size());
    }

    @Test
    public void testNoFlowFileIsEmittedWhenNonOkHttpResponseIsSent() {
        // given
        server.enqueue(new MockResponse().setResponseCode(HTTP_BAD_REQUEST)
            .setBody(format(RESPONSE_WITH_ZERO_RECORDS_TEMPLATE, TICKETS.getResponseFieldName())));
        testRunner.setProperty(ZENDESK_RESOURCE, TICKETS);
        testRunner.setProperty(ZENDESK_EXPORT_METHOD, CURSOR);

        // when
        testRunner.run(1);

        // then
        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(REL_SUCCESS);
        assertEquals(0, flowFiles.size());
    }

    @ParameterizedTest
    @MethodSource("unsupportedZendeskResourcesExportMethodCombinations")
    public void testUnsupportedZendeskResourceAndExportMethodsPairsShouldFailOnValidation(ZendeskResource zendeskResource, ZendeskExportMethod exportMethod) {
        // given
        testRunner.setProperty(ZENDESK_RESOURCE, zendeskResource);
        testRunner.setProperty(ZENDESK_EXPORT_METHOD, exportMethod);

        // when + then
        assertThrows(AssertionFailedError.class, () -> testRunner.run(1));
    }

    class TestGetZendesk extends GetZendesk {
        @Override
        HttpUriBuilder uriBuilder(String resourcePath) {
            HttpUrl url = server.url(resourcePath);
            return new StandardHttpUriBuilder()
                .scheme(url.scheme())
                .host(url.host())
                .port(url.port())
                .encodedPath(url.encodedPath());
        }
    }
}
