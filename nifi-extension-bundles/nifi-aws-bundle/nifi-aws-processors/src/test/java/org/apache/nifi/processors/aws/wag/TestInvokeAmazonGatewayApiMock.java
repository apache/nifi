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
package org.apache.nifi.processors.aws.wag;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.http.AmazonHttpClient;
import com.amazonaws.http.apache.client.impl.SdkHttpClient;
import com.amazonaws.internal.TokenBucket;
import com.amazonaws.metrics.RequestMetricCollector;
import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.message.BasicStatusLine;
import org.apache.http.protocol.HttpContext;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processors.aws.testutil.AuthUtils;
import org.apache.nifi.processors.aws.util.RegionUtilV1;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.times;

public class TestInvokeAmazonGatewayApiMock {

    private TestRunner runner = null;
    private SdkHttpClient mockSdkClient = null;

    @BeforeEach
    public void setUp() throws Exception {
        mockSdkClient = Mockito.mock(SdkHttpClient.class);
        ClientConfiguration clientConfig = new ClientConfiguration();

        InvokeAWSGatewayApi mockGetApi = new InvokeAWSGatewayApi(
            new AmazonHttpClient(clientConfig, mockSdkClient, RequestMetricCollector.NONE, new TokenBucket()));
        runner = TestRunners.newTestRunner(mockGetApi);
        runner.setValidateExpressionUsage(false);

        AuthUtils.enableAccessKey(runner, "awsAccessKey", "awsSecretKey");

        runner.setProperty(RegionUtilV1.REGION, "us-east-1");
        runner.setProperty(InvokeAWSGatewayApi.PROP_AWS_API_KEY, "abcd");
        runner.setProperty(InvokeAWSGatewayApi.PROP_RESOURCE_NAME, "/TEST");
        runner.setProperty(InvokeAWSGatewayApi.PROP_AWS_GATEWAY_API_ENDPOINT, "https://foobar.execute-api.us-east-1.amazonaws.com");
    }

    @Test
    public void testGetApiSimple() throws Exception {

        HttpResponse resp = new BasicHttpResponse(
            new BasicStatusLine(HttpVersion.HTTP_1_1, 200, "OK"));
        BasicHttpEntity entity = new BasicHttpEntity();
        entity.setContent(new ByteArrayInputStream("test payload".getBytes()));
        resp.setEntity(entity);
        Mockito.doReturn(resp).when(mockSdkClient)
               .execute(any(HttpUriRequest.class), any(HttpContext.class));

        // execute
        runner.assertValid();
        runner.run(1);

        // check
        Mockito.verify(mockSdkClient, times(1))
                .execute(argThat(argument -> argument.getMethod().equals("GET")
                                && argument.getFirstHeader("x-api-key").getValue().equals("abcd")
                                && argument.getFirstHeader("Authorization").getValue().startsWith("AWS4")
                                && argument.getURI().toString().equals("https://foobar.execute-api.us-east-1.amazonaws.com/TEST")),
                        any(HttpContext.class));

        runner.assertTransferCount(InvokeAWSGatewayApi.REL_SUCCESS_REQ, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RESPONSE, 1);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RETRY, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_NO_RETRY, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_FAILURE, 0);

        final List<MockFlowFile> flowFiles = runner
            .getFlowFilesForRelationship(InvokeAWSGatewayApi.REL_RESPONSE);
        final MockFlowFile ff0 = flowFiles.get(0);

        ff0.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_CODE, "200");
        ff0.assertContentEquals("test payload");
        ff0.assertAttributeExists(InvokeAWSGatewayApi.TRANSACTION_ID);
        ff0.assertAttributeEquals(InvokeAWSGatewayApi.RESOURCE_NAME_ATTR, "/TEST");
    }

    @Test
    public void testSendAttributes() throws Exception {

        HttpResponse resp = new BasicHttpResponse(
            new BasicStatusLine(HttpVersion.HTTP_1_1, 200, "OK"));
        BasicHttpEntity entity = new BasicHttpEntity();
        entity.setContent(new ByteArrayInputStream("test payload".getBytes()));
        resp.setEntity(entity);
        Mockito.doReturn(resp).when(mockSdkClient)
               .execute(any(HttpUriRequest.class), any(HttpContext.class));

        // add dynamic property
        runner.setProperty("dynamicHeader", "yes!");
        // set the regex
        runner.setProperty(InvokeAWSGatewayApi.PROP_ATTRIBUTES_TO_SEND, "F.*");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.MIME_TYPE.key(), "application/plain-text");
        attributes.put("Foo", "Bar");
        runner.enqueue("Hello".getBytes(StandardCharsets.UTF_8), attributes);
        // execute
        runner.assertValid();
        runner.run(1);

        Mockito.verify(mockSdkClient, times(1))
                .execute(argThat(argument -> argument.getMethod().equals("GET")
                                && argument.getFirstHeader("x-api-key").getValue().equals("abcd")
                                && argument.getFirstHeader("Authorization").getValue().startsWith("AWS4")
                                && argument.getFirstHeader("dynamicHeader").getValue().equals("yes!")
                                && argument.getFirstHeader("Foo").getValue().equals("Bar")
                                && argument.getURI().toString().equals("https://foobar.execute-api.us-east-1.amazonaws.com/TEST")),
                        any(HttpContext.class));
        // check
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_SUCCESS_REQ, 1);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RESPONSE, 1);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RETRY, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_NO_RETRY, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_FAILURE, 0);

        final List<MockFlowFile> flowFiles = runner
            .getFlowFilesForRelationship(InvokeAWSGatewayApi.REL_RESPONSE);
        final MockFlowFile ff0 = flowFiles.get(0);

        ff0.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_CODE, "200");
        ff0.assertContentEquals("test payload");
        ff0.assertAttributeExists(InvokeAWSGatewayApi.TRANSACTION_ID);
        ff0.assertAttributeEquals(InvokeAWSGatewayApi.RESOURCE_NAME_ATTR, "/TEST");
    }

    @Test
    public void testSendQueryParams() throws Exception {

        HttpResponse resp = new BasicHttpResponse(
            new BasicStatusLine(HttpVersion.HTTP_1_1, 200, "OK"));
        BasicHttpEntity entity = new BasicHttpEntity();
        entity.setContent(new ByteArrayInputStream("test payload".getBytes()));
        resp.setEntity(entity);
        Mockito.doReturn(resp).when(mockSdkClient)
               .execute(any(HttpUriRequest.class), any(HttpContext.class));

        // add dynamic property
        runner.setProperty("dynamicHeader", "yes!");
        runner.setProperty(InvokeAWSGatewayApi.PROP_QUERY_PARAMS, "apples=oranges&dogs=cats&filename=${filename}");

        // set the regex
        runner.setProperty(InvokeAWSGatewayApi.PROP_ATTRIBUTES_TO_SEND, "F.*");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.MIME_TYPE.key(), "application/plain-text");
        attributes.put("Foo", "Bar");
        attributes.put("filename", "testfile");
        runner.enqueue("Hello".getBytes(StandardCharsets.UTF_8), attributes);
        // execute
        runner.assertValid();
        runner.run(1);

        Mockito.verify(mockSdkClient, times(1))
                .execute(argThat(argument -> argument.getMethod().equals("GET")
                                && argument.getFirstHeader("x-api-key").getValue().equals("abcd")
                                && argument.getFirstHeader("Authorization").getValue().startsWith("AWS4")
                                && argument.getFirstHeader("dynamicHeader").getValue().equals("yes!")
                                && argument.getFirstHeader("Foo").getValue().equals("Bar")
                                && argument.getURI().toString().equals("https://foobar.execute-api.us-east-1.amazonaws.com/TEST?filename=testfile&dogs=cats&apples=oranges")),
                        any(HttpContext.class));
        // check
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_SUCCESS_REQ, 1);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RESPONSE, 1);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RETRY, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_NO_RETRY, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_FAILURE, 0);

        final List<MockFlowFile> flowFiles = runner
            .getFlowFilesForRelationship(InvokeAWSGatewayApi.REL_RESPONSE);
        final MockFlowFile ff0 = flowFiles.get(0);

        ff0.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_CODE, "200");
        ff0.assertContentEquals("test payload");
        ff0.assertAttributeExists(InvokeAWSGatewayApi.TRANSACTION_ID);
        ff0.assertAttributeEquals(InvokeAWSGatewayApi.RESOURCE_NAME_ATTR, "/TEST");
    }

}