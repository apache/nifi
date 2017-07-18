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
package org.apache.nifi.processors.grpc;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.ssl.StandardSSLContextService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.grpc.stub.StreamObserver;
import io.netty.handler.ssl.ClientAuth;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class TestInvokeGRPC {
    // ids placed on flowfiles and used to dictate response codes in the DummyFlowFileService below
    private static final long ERROR = 500;
    private static final long SUCCESS = 501;
    private static final long RETRY = 502;

    @Test
    public void testSuccess() throws Exception {
        final TestGRPCServer<DummyFlowFileService> server = new TestGRPCServer<>(DummyFlowFileService.class);

        try {
            final int port = TestGRPCServer.randomPort();
            server.start(port);
            final TestRunner runner = TestRunners.newTestRunner(InvokeGRPC.class);
            runner.setProperty(InvokeGRPC.PROP_SERVICE_HOST, TestGRPCServer.HOST);
            runner.setProperty(InvokeGRPC.PROP_SERVICE_PORT, String.valueOf(port));

            final MockFlowFile mockFlowFile = new MockFlowFile(SUCCESS);
            runner.enqueue(mockFlowFile);
            runner.run();
            runner.assertTransferCount(InvokeGRPC.REL_RESPONSE, 1);
            runner.assertTransferCount(InvokeGRPC.REL_SUCCESS_REQ, 1);
            runner.assertTransferCount(InvokeGRPC.REL_RETRY, 0);
            runner.assertTransferCount(InvokeGRPC.REL_NO_RETRY, 0);
            runner.assertTransferCount(InvokeGRPC.REL_FAILURE, 0);

            final List<MockFlowFile> responseFiles = runner.getFlowFilesForRelationship(InvokeGRPC.REL_RESPONSE);
            assertThat(responseFiles.size(), equalTo(1));
            final MockFlowFile response = responseFiles.get(0);
            response.assertAttributeEquals(InvokeGRPC.RESPONSE_CODE, String.valueOf(FlowFileReply.ResponseCode.SUCCESS));
            response.assertAttributeEquals(InvokeGRPC.RESPONSE_BODY, "success");
            response.assertAttributeEquals(InvokeGRPC.SERVICE_HOST, TestGRPCServer.HOST);
            response.assertAttributeEquals(InvokeGRPC.SERVICE_PORT, String.valueOf(port));

            final List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(InvokeGRPC.REL_SUCCESS_REQ);
            assertThat(successFiles.size(), equalTo(1));
            final MockFlowFile successFile = successFiles.get(0);
            successFile.assertAttributeEquals(InvokeGRPC.RESPONSE_CODE, String.valueOf(FlowFileReply.ResponseCode.SUCCESS));
            successFile.assertAttributeEquals(InvokeGRPC.RESPONSE_BODY, "success");
            successFile.assertAttributeEquals(InvokeGRPC.SERVICE_HOST, TestGRPCServer.HOST);
            successFile.assertAttributeEquals(InvokeGRPC.SERVICE_PORT, String.valueOf(port));
        } finally {
            server.stop();
        }
    }

    @Test
    public void testSuccessWithFlowFileContent() throws Exception {
        final TestGRPCServer<DummyFlowFileService> server = new TestGRPCServer<>(DummyFlowFileService.class);

        try {
            final int port = TestGRPCServer.randomPort();
            server.start(port);

            final TestRunner runner = TestRunners.newTestRunner(InvokeGRPC.class);
            runner.setProperty(InvokeGRPC.PROP_SERVICE_HOST, TestGRPCServer.HOST);
            runner.setProperty(InvokeGRPC.PROP_SERVICE_PORT, String.valueOf(port));

            runner.enqueue("content");
            runner.run();
            runner.assertTransferCount(InvokeGRPC.REL_RESPONSE, 1);
            runner.assertTransferCount(InvokeGRPC.REL_SUCCESS_REQ, 1);
            runner.assertTransferCount(InvokeGRPC.REL_RETRY, 0);
            runner.assertTransferCount(InvokeGRPC.REL_NO_RETRY, 0);
            runner.assertTransferCount(InvokeGRPC.REL_FAILURE, 0);

            final List<MockFlowFile> responseFiles = runner.getFlowFilesForRelationship(InvokeGRPC.REL_RESPONSE);
            assertThat(responseFiles.size(), equalTo(1));
            final MockFlowFile response = responseFiles.get(0);
            response.assertAttributeEquals(InvokeGRPC.RESPONSE_CODE, String.valueOf(FlowFileReply.ResponseCode.SUCCESS));
            response.assertAttributeEquals(InvokeGRPC.RESPONSE_BODY, "content");
            response.assertAttributeEquals(InvokeGRPC.SERVICE_HOST, TestGRPCServer.HOST);
            response.assertAttributeEquals(InvokeGRPC.SERVICE_PORT, String.valueOf(port));

            final List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(InvokeGRPC.REL_SUCCESS_REQ);
            assertThat(successFiles.size(), equalTo(1));
            final MockFlowFile successFile = successFiles.get(0);
            successFile.assertAttributeEquals(InvokeGRPC.RESPONSE_CODE, String.valueOf(FlowFileReply.ResponseCode.SUCCESS));
            successFile.assertAttributeEquals(InvokeGRPC.RESPONSE_BODY, "content");
            successFile.assertAttributeEquals(InvokeGRPC.SERVICE_HOST, TestGRPCServer.HOST);
            successFile.assertAttributeEquals(InvokeGRPC.SERVICE_PORT, String.valueOf(port));
        } finally {
            server.stop();
        }
    }

    @Test
    public void testSuccessAlwaysOutputResponse() throws Exception {
        final TestGRPCServer<DummyFlowFileService> server = new TestGRPCServer<>(DummyFlowFileService.class);

        try {
            final int port = TestGRPCServer.randomPort();
            server.start(port);

            final TestRunner runner = TestRunners.newTestRunner(InvokeGRPC.class);
            runner.setProperty(InvokeGRPC.PROP_SERVICE_HOST, TestGRPCServer.HOST);
            runner.setProperty(InvokeGRPC.PROP_SERVICE_PORT, String.valueOf(port));
            runner.setProperty(InvokeGRPC.PROP_OUTPUT_RESPONSE_REGARDLESS, "true");

            final MockFlowFile mockFlowFile = new MockFlowFile(SUCCESS);
            runner.enqueue(mockFlowFile);
            runner.run();
            runner.assertTransferCount(InvokeGRPC.REL_RESPONSE, 1);
            runner.assertTransferCount(InvokeGRPC.REL_SUCCESS_REQ, 1);
            runner.assertTransferCount(InvokeGRPC.REL_RETRY, 0);
            runner.assertTransferCount(InvokeGRPC.REL_NO_RETRY, 0);
            runner.assertTransferCount(InvokeGRPC.REL_FAILURE, 0);

            final List<MockFlowFile> responseFiles = runner.getFlowFilesForRelationship(InvokeGRPC.REL_RESPONSE);
            assertThat(responseFiles.size(), equalTo(1));
            final MockFlowFile response = responseFiles.get(0);
            response.assertAttributeEquals(InvokeGRPC.RESPONSE_CODE, String.valueOf(FlowFileReply.ResponseCode.SUCCESS));
            response.assertAttributeEquals(InvokeGRPC.RESPONSE_BODY, "success");
            response.assertAttributeEquals(InvokeGRPC.SERVICE_HOST, TestGRPCServer.HOST);
            response.assertAttributeEquals(InvokeGRPC.SERVICE_PORT, String.valueOf(port));

            final List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(InvokeGRPC.REL_SUCCESS_REQ);
            assertThat(successFiles.size(), equalTo(1));
            final MockFlowFile successFile = successFiles.get(0);
            successFile.assertAttributeEquals(InvokeGRPC.RESPONSE_CODE, String.valueOf(FlowFileReply.ResponseCode.SUCCESS));
            successFile.assertAttributeEquals(InvokeGRPC.RESPONSE_BODY, "success");
            successFile.assertAttributeEquals(InvokeGRPC.SERVICE_HOST, TestGRPCServer.HOST);
            successFile.assertAttributeEquals(InvokeGRPC.SERVICE_PORT, String.valueOf(port));
        } finally {
            server.stop();
        }
    }

    @Test
    public void testExceedMaxMessageSize() throws Exception {
        final TestGRPCServer<DummyFlowFileService> server = new TestGRPCServer<>(DummyFlowFileService.class);

        try {
            final int port = TestGRPCServer.randomPort();
            server.start(port);
            final TestRunner runner = TestRunners.newTestRunner(InvokeGRPC.class);
            runner.setProperty(InvokeGRPC.PROP_SERVICE_HOST, TestGRPCServer.HOST);
            runner.setProperty(InvokeGRPC.PROP_SERVICE_PORT, String.valueOf(port));
            // set max message size to 1B to force error
            runner.setProperty(InvokeGRPC.PROP_MAX_MESSAGE_SIZE, "1B");

            final MockFlowFile mockFlowFile = new MockFlowFile(SUCCESS);
            runner.enqueue(mockFlowFile);
            runner.run();
            runner.assertTransferCount(InvokeGRPC.REL_RESPONSE, 0);
            runner.assertTransferCount(InvokeGRPC.REL_SUCCESS_REQ, 0);
            runner.assertTransferCount(InvokeGRPC.REL_RETRY, 0);
            runner.assertTransferCount(InvokeGRPC.REL_NO_RETRY, 0);
            runner.assertTransferCount(InvokeGRPC.REL_FAILURE, 1);

            final List<MockFlowFile> responseFiles = runner.getFlowFilesForRelationship(InvokeGRPC.REL_FAILURE);
            assertThat(responseFiles.size(), equalTo(1));
            final MockFlowFile response = responseFiles.get(0);
            response.assertAttributeEquals(InvokeGRPC.SERVICE_HOST, TestGRPCServer.HOST);
            response.assertAttributeEquals(InvokeGRPC.SERVICE_PORT, String.valueOf(port));
            // an exception should be thrown indicating that the max message size was exceeded.
            response.assertAttributeEquals(InvokeGRPC.EXCEPTION_CLASS, "io.grpc.StatusRuntimeException");
        } finally {
            server.stop();
        }
    }

    @Test
    public void testRetry() throws Exception {
        final TestGRPCServer<DummyFlowFileService> server = new TestGRPCServer<>(DummyFlowFileService.class);

        try {
            final int port = TestGRPCServer.randomPort();
            server.start(port);

            final TestRunner runner = TestRunners.newTestRunner(InvokeGRPC.class);
            runner.setProperty(InvokeGRPC.PROP_SERVICE_HOST, TestGRPCServer.HOST);
            runner.setProperty(InvokeGRPC.PROP_SERVICE_PORT, String.valueOf(port));

            final MockFlowFile mockFlowFile = new MockFlowFile(RETRY);
            runner.enqueue(mockFlowFile);
            runner.run();
            runner.assertTransferCount(InvokeGRPC.REL_RESPONSE, 0);
            runner.assertTransferCount(InvokeGRPC.REL_SUCCESS_REQ, 0);
            runner.assertTransferCount(InvokeGRPC.REL_RETRY, 1);
            runner.assertTransferCount(InvokeGRPC.REL_NO_RETRY, 0);
            runner.assertTransferCount(InvokeGRPC.REL_FAILURE, 0);
            runner.assertPenalizeCount(1);

            final List<MockFlowFile> responseFiles = runner.getFlowFilesForRelationship(InvokeGRPC.REL_RETRY);
            assertThat(responseFiles.size(), equalTo(1));
            final MockFlowFile response = responseFiles.get(0);
            response.assertAttributeEquals(InvokeGRPC.RESPONSE_CODE, String.valueOf(FlowFileReply.ResponseCode.RETRY));
            response.assertAttributeEquals(InvokeGRPC.RESPONSE_BODY, "retry");
            response.assertAttributeEquals(InvokeGRPC.SERVICE_HOST, TestGRPCServer.HOST);
            response.assertAttributeEquals(InvokeGRPC.SERVICE_PORT, String.valueOf(port));
        } finally {
            server.stop();
        }
    }

    @Test
    public void testRetryAlwaysOutputResponse() throws Exception {
        final TestGRPCServer<DummyFlowFileService> server = new TestGRPCServer<>(DummyFlowFileService.class);

        try {
            final int port = TestGRPCServer.randomPort();
            server.start(port);

            final TestRunner runner = TestRunners.newTestRunner(InvokeGRPC.class);
            runner.setProperty(InvokeGRPC.PROP_SERVICE_HOST, TestGRPCServer.HOST);
            runner.setProperty(InvokeGRPC.PROP_SERVICE_PORT, String.valueOf(port));
            runner.setProperty(InvokeGRPC.PROP_OUTPUT_RESPONSE_REGARDLESS, "true");

            final MockFlowFile mockFlowFile = new MockFlowFile(RETRY);
            runner.enqueue(mockFlowFile);
            runner.run();
            runner.assertTransferCount(InvokeGRPC.REL_RESPONSE, 1);
            runner.assertTransferCount(InvokeGRPC.REL_SUCCESS_REQ, 0);
            runner.assertTransferCount(InvokeGRPC.REL_RETRY, 1);
            runner.assertTransferCount(InvokeGRPC.REL_NO_RETRY, 0);
            runner.assertTransferCount(InvokeGRPC.REL_FAILURE, 0);
            runner.assertPenalizeCount(1);

            final List<MockFlowFile> retryFiles = runner.getFlowFilesForRelationship(InvokeGRPC.REL_RETRY);
            assertThat(retryFiles.size(), equalTo(1));
            final MockFlowFile retry = retryFiles.get(0);
            retry.assertAttributeEquals(InvokeGRPC.RESPONSE_CODE, String.valueOf(FlowFileReply.ResponseCode.RETRY));
            retry.assertAttributeEquals(InvokeGRPC.RESPONSE_BODY, "retry");
            retry.assertAttributeEquals(InvokeGRPC.SERVICE_HOST, TestGRPCServer.HOST);
            retry.assertAttributeEquals(InvokeGRPC.SERVICE_PORT, String.valueOf(port));

            final List<MockFlowFile> responseFiles = runner.getFlowFilesForRelationship(InvokeGRPC.REL_RESPONSE);
            assertThat(responseFiles.size(), equalTo(1));
            final MockFlowFile response = responseFiles.get(0);
            response.assertAttributeEquals(InvokeGRPC.RESPONSE_CODE, String.valueOf(FlowFileReply.ResponseCode.RETRY));
            response.assertAttributeEquals(InvokeGRPC.RESPONSE_BODY, "retry");
            response.assertAttributeEquals(InvokeGRPC.SERVICE_HOST, TestGRPCServer.HOST);
            response.assertAttributeEquals(InvokeGRPC.SERVICE_PORT, String.valueOf(port));
        } finally {
            server.stop();
        }
    }

    @Test
    public void testNoRetryOnError() throws Exception {
        final TestGRPCServer<DummyFlowFileService> server = new TestGRPCServer<>(DummyFlowFileService.class);

        try {
            final int port = TestGRPCServer.randomPort();
            server.start(port);

            final TestRunner runner = TestRunners.newTestRunner(InvokeGRPC.class);
            runner.setProperty(InvokeGRPC.PROP_SERVICE_HOST, TestGRPCServer.HOST);
            runner.setProperty(InvokeGRPC.PROP_SERVICE_PORT, String.valueOf(port));

            final MockFlowFile mockFlowFile = new MockFlowFile(ERROR);
            runner.enqueue(mockFlowFile);
            runner.run();
            runner.assertTransferCount(InvokeGRPC.REL_RESPONSE, 0);
            runner.assertTransferCount(InvokeGRPC.REL_SUCCESS_REQ, 0);
            runner.assertTransferCount(InvokeGRPC.REL_RETRY, 0);
            runner.assertTransferCount(InvokeGRPC.REL_NO_RETRY, 1);
            runner.assertTransferCount(InvokeGRPC.REL_FAILURE, 0);

            final List<MockFlowFile> responseFiles = runner.getFlowFilesForRelationship(InvokeGRPC.REL_NO_RETRY);
            assertThat(responseFiles.size(), equalTo(1));
            final MockFlowFile response = responseFiles.get(0);
            response.assertAttributeEquals(InvokeGRPC.RESPONSE_CODE, String.valueOf(FlowFileReply.ResponseCode.ERROR));
            response.assertAttributeEquals(InvokeGRPC.RESPONSE_BODY, "error");
            response.assertAttributeEquals(InvokeGRPC.SERVICE_HOST, TestGRPCServer.HOST);
            response.assertAttributeEquals(InvokeGRPC.SERVICE_PORT, String.valueOf(port));
        } finally {
            server.stop();
        }
    }

    @Test
    public void testNoRetryOnErrorAlwaysOutputResponseAndPenalize() throws Exception {
        final TestGRPCServer<DummyFlowFileService> server = new TestGRPCServer<>(DummyFlowFileService.class);

        try {
            final int port = TestGRPCServer.randomPort();
            server.start(port);

            final TestRunner runner = TestRunners.newTestRunner(InvokeGRPC.class);
            runner.setProperty(InvokeGRPC.PROP_SERVICE_HOST, TestGRPCServer.HOST);
            runner.setProperty(InvokeGRPC.PROP_SERVICE_PORT, String.valueOf(port));
            runner.setProperty(InvokeGRPC.PROP_OUTPUT_RESPONSE_REGARDLESS, "true");
            runner.setProperty(InvokeGRPC.PROP_PENALIZE_NO_RETRY, "true");

            final MockFlowFile mockFlowFile = new MockFlowFile(ERROR);
            runner.enqueue(mockFlowFile);
            runner.run();
            runner.assertTransferCount(InvokeGRPC.REL_RESPONSE, 1);
            runner.assertTransferCount(InvokeGRPC.REL_SUCCESS_REQ, 0);
            runner.assertTransferCount(InvokeGRPC.REL_RETRY, 0);
            runner.assertTransferCount(InvokeGRPC.REL_NO_RETRY, 1);
            runner.assertTransferCount(InvokeGRPC.REL_FAILURE, 0);
            runner.assertPenalizeCount(1);

            final List<MockFlowFile> noRetryFiles = runner.getFlowFilesForRelationship(InvokeGRPC.REL_NO_RETRY);
            assertThat(noRetryFiles.size(), equalTo(1));
            final MockFlowFile noRetry = noRetryFiles.get(0);
            noRetry.assertAttributeEquals(InvokeGRPC.RESPONSE_CODE, String.valueOf(FlowFileReply.ResponseCode.ERROR));
            noRetry.assertAttributeEquals(InvokeGRPC.RESPONSE_BODY, "error");
            noRetry.assertAttributeEquals(InvokeGRPC.SERVICE_HOST, TestGRPCServer.HOST);
            noRetry.assertAttributeEquals(InvokeGRPC.SERVICE_PORT, String.valueOf(port));

            final List<MockFlowFile> responseFiles = runner.getFlowFilesForRelationship(InvokeGRPC.REL_RESPONSE);
            assertThat(responseFiles.size(), equalTo(1));
            final MockFlowFile response = responseFiles.get(0);
            response.assertAttributeEquals(InvokeGRPC.RESPONSE_CODE, String.valueOf(FlowFileReply.ResponseCode.ERROR));
            response.assertAttributeEquals(InvokeGRPC.RESPONSE_BODY, "error");
            response.assertAttributeEquals(InvokeGRPC.SERVICE_HOST, TestGRPCServer.HOST);
            response.assertAttributeEquals(InvokeGRPC.SERVICE_PORT, String.valueOf(port));
        } finally {
            server.stop();
        }
    }

    @Test
    public void testNoInput() throws Exception {
        final TestGRPCServer<DummyFlowFileService> server = new TestGRPCServer<>(DummyFlowFileService.class);

        try {
            final int port = TestGRPCServer.randomPort();
            server.start(port);

            final TestRunner runner = TestRunners.newTestRunner(InvokeGRPC.class);
            runner.setProperty(InvokeGRPC.PROP_SERVICE_HOST, TestGRPCServer.HOST);
            runner.setProperty(InvokeGRPC.PROP_SERVICE_PORT, String.valueOf(port));

            runner.run();
            runner.assertTransferCount(InvokeGRPC.REL_RESPONSE, 0);
            runner.assertTransferCount(InvokeGRPC.REL_SUCCESS_REQ, 0);
            runner.assertTransferCount(InvokeGRPC.REL_RETRY, 0);
            runner.assertTransferCount(InvokeGRPC.REL_NO_RETRY, 0);
            runner.assertTransferCount(InvokeGRPC.REL_FAILURE, 0);
            runner.assertPenalizeCount(0);
        } finally {
            server.stop();
        }
    }

    @Test
    public void testServerConnectionFail() throws Exception {

        final int port = TestGRPCServer.randomPort();

        // should be no gRPC server running @ that port, so processor will fail
        final TestRunner runner = TestRunners.newTestRunner(InvokeGRPC.class);
        runner.setProperty(InvokeGRPC.PROP_SERVICE_HOST, TestGRPCServer.HOST);
        runner.setProperty(InvokeGRPC.PROP_SERVICE_PORT, Integer.toString(port));

        final MockFlowFile mockFlowFile = new MockFlowFile(SUCCESS);
        runner.enqueue(mockFlowFile);
        runner.run();

        runner.assertTransferCount(InvokeGRPC.REL_RESPONSE, 0);
        runner.assertTransferCount(InvokeGRPC.REL_SUCCESS_REQ, 0);
        runner.assertTransferCount(InvokeGRPC.REL_RETRY, 0);
        runner.assertTransferCount(InvokeGRPC.REL_NO_RETRY, 0);
        runner.assertTransferCount(InvokeGRPC.REL_FAILURE, 1);

        final List<MockFlowFile> responseFiles = runner.getFlowFilesForRelationship(InvokeGRPC.REL_FAILURE);
        assertThat(responseFiles.size(), equalTo(1));
        final MockFlowFile response = responseFiles.get(0);
        response.assertAttributeEquals(InvokeGRPC.SERVICE_HOST, TestGRPCServer.HOST);
        response.assertAttributeEquals(InvokeGRPC.SERVICE_PORT, Integer.toString(port));
        response.assertAttributeEquals(InvokeGRPC.EXCEPTION_CLASS, "io.grpc.StatusRuntimeException");

    }

    @Test
    public void testSecureTwoWaySsl() throws Exception {
        final Map<String, String> sslProperties = getKeystoreProperties();
        sslProperties.putAll(getTruststoreProperties());
        final TestGRPCServer<DummyFlowFileService> server = new TestGRPCServer<>(DummyFlowFileService.class, sslProperties);

        try {
            final int port = TestGRPCServer.randomPort();
            final TestRunner runner = TestRunners.newTestRunner(InvokeGRPC.class);
            runner.setProperty(InvokeGRPC.PROP_SERVICE_HOST, TestGRPCServer.HOST);
            runner.setProperty(InvokeGRPC.PROP_SERVICE_PORT, String.valueOf(port));
            runner.setProperty(InvokeGRPC.PROP_USE_SECURE, "true");
            useSSLContextService(runner, sslProperties);
            server.start(port);

            final MockFlowFile mockFlowFile = new MockFlowFile(SUCCESS);
            runner.enqueue(mockFlowFile);
            runner.run();
            runner.assertTransferCount(InvokeGRPC.REL_RESPONSE, 1);
            runner.assertTransferCount(InvokeGRPC.REL_SUCCESS_REQ, 1);
            runner.assertTransferCount(InvokeGRPC.REL_RETRY, 0);
            runner.assertTransferCount(InvokeGRPC.REL_NO_RETRY, 0);
            runner.assertTransferCount(InvokeGRPC.REL_FAILURE, 0);

            final List<MockFlowFile> responseFiles = runner.getFlowFilesForRelationship(InvokeGRPC.REL_RESPONSE);
            assertThat(responseFiles.size(), equalTo(1));
            final MockFlowFile response = responseFiles.get(0);
            response.assertAttributeEquals(InvokeGRPC.RESPONSE_CODE, String.valueOf(FlowFileReply.ResponseCode.SUCCESS));
            response.assertAttributeEquals(InvokeGRPC.RESPONSE_BODY, "success");
            response.assertAttributeEquals(InvokeGRPC.SERVICE_HOST, TestGRPCServer.HOST);
            response.assertAttributeEquals(InvokeGRPC.SERVICE_PORT, String.valueOf(port));

            final List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(InvokeGRPC.REL_SUCCESS_REQ);
            assertThat(successFiles.size(), equalTo(1));
            final MockFlowFile successFile = successFiles.get(0);
            successFile.assertAttributeEquals(InvokeGRPC.RESPONSE_CODE, String.valueOf(FlowFileReply.ResponseCode.SUCCESS));
            successFile.assertAttributeEquals(InvokeGRPC.RESPONSE_BODY, "success");
            successFile.assertAttributeEquals(InvokeGRPC.SERVICE_HOST, TestGRPCServer.HOST);
            successFile.assertAttributeEquals(InvokeGRPC.SERVICE_PORT, String.valueOf(port));
        } finally {
            server.stop();
        }
    }

    @Test
    public void testSecureOneWaySsl() throws Exception {
        final Map<String, String> sslProperties = getKeystoreProperties();
        sslProperties.put(TestGRPCServer.NEED_CLIENT_AUTH, ClientAuth.NONE.name());
        final TestGRPCServer<DummyFlowFileService> server = new TestGRPCServer<>(DummyFlowFileService.class, sslProperties);

        try {
            final int port = TestGRPCServer.randomPort();
            final TestRunner runner = TestRunners.newTestRunner(InvokeGRPC.class);
            runner.setProperty(InvokeGRPC.PROP_SERVICE_HOST, TestGRPCServer.HOST);
            runner.setProperty(InvokeGRPC.PROP_SERVICE_PORT, String.valueOf(port));
            runner.setProperty(InvokeGRPC.PROP_USE_SECURE, "true");
            useSSLContextService(runner, getTruststoreProperties());
            server.start(port);

            final MockFlowFile mockFlowFile = new MockFlowFile(SUCCESS);
            runner.enqueue(mockFlowFile);
            runner.run();
            runner.assertTransferCount(InvokeGRPC.REL_RESPONSE, 1);
            runner.assertTransferCount(InvokeGRPC.REL_SUCCESS_REQ, 1);
            runner.assertTransferCount(InvokeGRPC.REL_RETRY, 0);
            runner.assertTransferCount(InvokeGRPC.REL_NO_RETRY, 0);
            runner.assertTransferCount(InvokeGRPC.REL_FAILURE, 0);

            final List<MockFlowFile> responseFiles = runner.getFlowFilesForRelationship(InvokeGRPC.REL_RESPONSE);
            assertThat(responseFiles.size(), equalTo(1));
            final MockFlowFile response = responseFiles.get(0);
            response.assertAttributeEquals(InvokeGRPC.RESPONSE_CODE, String.valueOf(FlowFileReply.ResponseCode.SUCCESS));
            response.assertAttributeEquals(InvokeGRPC.RESPONSE_BODY, "success");
            response.assertAttributeEquals(InvokeGRPC.SERVICE_HOST, TestGRPCServer.HOST);
            response.assertAttributeEquals(InvokeGRPC.SERVICE_PORT, String.valueOf(port));

            final List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(InvokeGRPC.REL_SUCCESS_REQ);
            assertThat(successFiles.size(), equalTo(1));
            final MockFlowFile successFile = successFiles.get(0);
            successFile.assertAttributeEquals(InvokeGRPC.RESPONSE_CODE, String.valueOf(FlowFileReply.ResponseCode.SUCCESS));
            successFile.assertAttributeEquals(InvokeGRPC.RESPONSE_BODY, "success");
            successFile.assertAttributeEquals(InvokeGRPC.SERVICE_HOST, TestGRPCServer.HOST);
            successFile.assertAttributeEquals(InvokeGRPC.SERVICE_PORT, String.valueOf(port));
        } finally {
            server.stop();
        }
    }

    private static Map<String, String> getTruststoreProperties() {
        final Map<String, String> props = new HashMap<>();
        props.put(StandardSSLContextService.TRUSTSTORE.getName(), "src/test/resources/localhost-ts.jks");
        props.put(StandardSSLContextService.TRUSTSTORE_PASSWORD.getName(), "localtest");
        props.put(StandardSSLContextService.TRUSTSTORE_TYPE.getName(), "JKS");
        return props;
    }

    private static Map<String, String> getKeystoreProperties() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(StandardSSLContextService.KEYSTORE.getName(), "src/test/resources/localhost-ks.jks");
        properties.put(StandardSSLContextService.KEYSTORE_PASSWORD.getName(), "localtest");
        properties.put(StandardSSLContextService.KEYSTORE_TYPE.getName(), "JKS");
        return properties;
    }

    private void useSSLContextService(final TestRunner controller, final Map<String, String> sslProperties) {
        final SSLContextService service = new StandardSSLContextService();
        try {
            controller.addControllerService("ssl-service", service, sslProperties);
            controller.enableControllerService(service);
        } catch (InitializationException ex) {
            ex.printStackTrace();
            Assert.fail("Could not create SSL Context Service");
        }

        controller.setProperty(InvokeGRPC.PROP_SSL_CONTEXT_SERVICE, "ssl-service");
    }

    /**
     * Dummy gRPC service whose responses are dictated by the IDs on the messages it receives
     */
    private static class DummyFlowFileService extends FlowFileServiceGrpc.FlowFileServiceImplBase {

        public DummyFlowFileService() {
        }

        @Override
        public void send(FlowFileRequest request, StreamObserver<FlowFileReply> responseObserver) {
            final FlowFileReply.Builder replyBuilder = FlowFileReply.newBuilder();
            // use the id to dictate response codes
            final long id = request.getId();

            if (id == ERROR) {
                replyBuilder.setResponseCode(FlowFileReply.ResponseCode.ERROR)
                        .setBody("error");
            } else if (id == SUCCESS) {
                replyBuilder.setResponseCode(FlowFileReply.ResponseCode.SUCCESS)
                        .setBody("success");
            } else if (id == RETRY) {
                replyBuilder.setResponseCode(FlowFileReply.ResponseCode.RETRY)
                        .setBody("retry");
                // else, assume the request is to include the flowfile content in the response
            } else {
                replyBuilder.setResponseCode(FlowFileReply.ResponseCode.SUCCESS)
                        .setBody(request.getContent().toStringUtf8());

            }
            responseObserver.onNext(replyBuilder.build());
            responseObserver.onCompleted();
        }
    }
}
