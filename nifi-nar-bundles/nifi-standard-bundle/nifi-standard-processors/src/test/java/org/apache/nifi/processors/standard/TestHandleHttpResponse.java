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
package org.apache.nifi.processors.standard;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.servlet.AsyncContext;
import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.http.HttpContextMap;
import org.apache.nifi.processor.exception.FlowFileAccessException;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.standard.util.HTTPUtils;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class TestHandleHttpResponse {

    private static final String CONTEXT_MAP_ID = MockHttpContextMap.class.getSimpleName();

    private static final String HTTP_REQUEST_ID = "HTTP-Request-Identifier";

    private static final int HTTP_STATUS_CREATED = HttpServletResponse.SC_CREATED;

    private static final String FLOW_FILE_CONTENT = "TESTING";

    @Test
    public void testEnsureCompleted() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(HandleHttpResponse.class);

        final MockHttpContextMap contextMap = new MockHttpContextMap(HTTP_REQUEST_ID, null, null);
        runner.addControllerService(CONTEXT_MAP_ID, contextMap);
        runner.enableControllerService(contextMap);
        runner.setProperty(HandleHttpResponse.HTTP_CONTEXT_MAP, CONTEXT_MAP_ID);
        runner.setProperty(HandleHttpResponse.STATUS_CODE, "${status.code}");
        runner.setProperty("my-attr", "${my-attr}");
        runner.setProperty("no-valid-attr", "${no-valid-attr}");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(HTTPUtils.HTTP_CONTEXT_ID, HTTP_REQUEST_ID);
        attributes.put(HTTPUtils.HTTP_REQUEST_URI, "/test");
        attributes.put(HTTPUtils.HTTP_LOCAL_NAME, "server");
        attributes.put(HTTPUtils.HTTP_PORT, "8443");
        attributes.put(HTTPUtils.HTTP_REMOTE_HOST, "client");
        attributes.put(HTTPUtils.HTTP_SSL_CERT, "sslDN");
        attributes.put("my-attr", "hello");
        attributes.put("status.code", Integer.toString(HTTP_STATUS_CREATED));

        runner.enqueue(FLOW_FILE_CONTENT.getBytes(), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(HandleHttpResponse.REL_SUCCESS, 1);
        assertEquals(1, runner.getProvenanceEvents().size());
        assertEquals(ProvenanceEventType.SEND, runner.getProvenanceEvents().get(0).getEventType());
        assertEquals("https://client@server:8443/test", runner.getProvenanceEvents().get(0).getTransitUri());

        assertEquals(FLOW_FILE_CONTENT, contextMap.outputStream.toString());
        assertEquals("hello", contextMap.headersSent.get("my-attr"));
        assertNull(contextMap.headersSent.get("no-valid-attr"));
        assertEquals(HTTP_STATUS_CREATED, contextMap.statusCode);
        assertEquals(1, contextMap.getCompletionCount());
        assertTrue(contextMap.headersWithNoValue.isEmpty());
    }

    @Test
    public void testRegexHeaders() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(HandleHttpResponse.class);

        final MockHttpContextMap contextMap = new MockHttpContextMap(HTTP_REQUEST_ID, null, null);
        runner.addControllerService(CONTEXT_MAP_ID, contextMap);
        runner.enableControllerService(contextMap);
        runner.setProperty(HandleHttpResponse.HTTP_CONTEXT_MAP, CONTEXT_MAP_ID);
        runner.setProperty(HandleHttpResponse.STATUS_CODE, "${status.code}");
        runner.setProperty(HandleHttpResponse.ATTRIBUTES_AS_HEADERS_REGEX, "^(my.*)$");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(HTTPUtils.HTTP_CONTEXT_ID, HTTP_REQUEST_ID);
        attributes.put(HTTPUtils.HTTP_REQUEST_URI, "/test");
        attributes.put(HTTPUtils.HTTP_LOCAL_NAME, "server");
        attributes.put(HTTPUtils.HTTP_PORT, "8443");
        attributes.put(HTTPUtils.HTTP_REMOTE_HOST, "client");
        attributes.put(HTTPUtils.HTTP_SSL_CERT, "sslDN");
        attributes.put("my-attr", "hello");
        attributes.put("my-blank-attr", "");
        attributes.put("status.code", Integer.toString(HTTP_STATUS_CREATED));

        runner.enqueue(FLOW_FILE_CONTENT.getBytes(), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(HandleHttpResponse.REL_SUCCESS, 1);
        assertEquals(1, runner.getProvenanceEvents().size());
        assertEquals(ProvenanceEventType.SEND, runner.getProvenanceEvents().get(0).getEventType());
        assertEquals("https://client@server:8443/test", runner.getProvenanceEvents().get(0).getTransitUri());

        assertEquals(FLOW_FILE_CONTENT, contextMap.outputStream.toString());
        assertEquals("hello", contextMap.headersSent.get("my-attr"));
        assertNull(contextMap.headersSent.get("my-blank-attr"));
        assertEquals(HTTP_STATUS_CREATED, contextMap.statusCode);
        assertEquals(1, contextMap.getCompletionCount());
        assertTrue(contextMap.headersWithNoValue.isEmpty());
    }

    @Test
    public void testResponseFlowFileAccessException() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(HandleHttpResponse.class);

        final MockHttpContextMap contextMap = new MockHttpContextMap(HTTP_REQUEST_ID, new FlowFileAccessException("Access Problem"), null);
        runner.addControllerService(CONTEXT_MAP_ID, contextMap);
        runner.enableControllerService(contextMap);
        runner.setProperty(HandleHttpResponse.HTTP_CONTEXT_MAP, CONTEXT_MAP_ID);
        runner.setProperty(HandleHttpResponse.STATUS_CODE, "${status.code}");
        runner.setProperty("my-attr", "${my-attr}");
        runner.setProperty("no-valid-attr", "${no-valid-attr}");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(HTTPUtils.HTTP_CONTEXT_ID, HTTP_REQUEST_ID);
        attributes.put("my-attr", "hello");
        attributes.put("status.code", Integer.toString(HTTP_STATUS_CREATED));

        runner.enqueue(FLOW_FILE_CONTENT.getBytes(), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(HandleHttpResponse.REL_FAILURE, 1);
        assertEquals(0, contextMap.getCompletionCount());
    }

    @Test
    public void testResponseProcessException() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(HandleHttpResponse.class);

        final MockHttpContextMap contextMap = new MockHttpContextMap(HTTP_REQUEST_ID, new ProcessException(), null);
        runner.addControllerService(CONTEXT_MAP_ID, contextMap);
        runner.enableControllerService(contextMap);
        runner.setProperty(HandleHttpResponse.HTTP_CONTEXT_MAP, CONTEXT_MAP_ID);
        runner.setProperty(HandleHttpResponse.STATUS_CODE, "${status.code}");
        runner.setProperty("my-attr", "${my-attr}");
        runner.setProperty("no-valid-attr", "${no-valid-attr}");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(HTTPUtils.HTTP_CONTEXT_ID, HTTP_REQUEST_ID);
        attributes.put("my-attr", "hello");
        attributes.put("status.code", Integer.toString(HTTP_STATUS_CREATED));

        runner.enqueue(FLOW_FILE_CONTENT.getBytes(), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(HandleHttpResponse.REL_FAILURE, 1);
        assertEquals(1, contextMap.getCompletionCount());
    }

    @Test
    public void testResponseProcessExceptionThenIllegalStateException() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(HandleHttpResponse.class);

        final MockHttpContextMap contextMap = new MockHttpContextMap(HTTP_REQUEST_ID, new ProcessException(), new IllegalStateException());
        runner.addControllerService(CONTEXT_MAP_ID, contextMap);
        runner.enableControllerService(contextMap);
        runner.setProperty(HandleHttpResponse.HTTP_CONTEXT_MAP, CONTEXT_MAP_ID);
        runner.setProperty(HandleHttpResponse.STATUS_CODE, "${status.code}");
        runner.setProperty("my-attr", "${my-attr}");
        runner.setProperty("no-valid-attr", "${no-valid-attr}");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(HTTPUtils.HTTP_CONTEXT_ID, HTTP_REQUEST_ID);
        attributes.put("my-attr", "hello");
        attributes.put("status.code", Integer.toString(HTTP_STATUS_CREATED));

        runner.enqueue(FLOW_FILE_CONTENT.getBytes(), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(HandleHttpResponse.REL_FAILURE, 1);
        assertEquals(0, contextMap.getCompletionCount());
    }

    @Test
    public void testStatusCodeEmpty() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(HandleHttpResponse.class);

        final MockHttpContextMap contextMap = new MockHttpContextMap(HTTP_REQUEST_ID, null, null);
        runner.addControllerService(CONTEXT_MAP_ID, contextMap);
        runner.enableControllerService(contextMap);
        runner.setProperty(HandleHttpResponse.HTTP_CONTEXT_MAP, CONTEXT_MAP_ID);
        runner.setProperty(HandleHttpResponse.STATUS_CODE, "${status.code}");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(HTTPUtils.HTTP_CONTEXT_ID, HTTP_REQUEST_ID);
        attributes.put("my-attr", "hello");

        runner.enqueue(FLOW_FILE_CONTENT.getBytes(), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(HandleHttpResponse.REL_FAILURE, 1);
        assertEquals(0, contextMap.getCompletionCount());
    }

    private static class MockHttpContextMap extends AbstractControllerService implements HttpContextMap {

        private final String id;
        private final AtomicInteger completedCount = new AtomicInteger(0);
        private final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        private final ConcurrentMap<String, String> headersSent = new ConcurrentHashMap<>();
        private final Exception responseException;
        private final RuntimeException completeException;
        private volatile int statusCode = -1;

        private final List<String> headersWithNoValue = new CopyOnWriteArrayList<>();

        public MockHttpContextMap(final String expectedIdentifier, final Exception responseException, final RuntimeException completeException) {
            this.id = expectedIdentifier;
            this.responseException = responseException;
            this.completeException = completeException;
        }

        @Override
        public boolean register(String identifier, HttpServletRequest request, HttpServletResponse response, AsyncContext context) {
            return true;
        }

        @Override
        public HttpServletResponse getResponse(final String identifier) {
            if (!id.equals(identifier)) {
                Assert.fail("attempting to respond to wrong request; should have been " + id + " but was " + identifier);
            }

            try {
                final HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
                if (responseException == null) {
                    Mockito.when(response.getOutputStream()).thenReturn(new ServletOutputStream() {
                        @Override
                        public boolean isReady() {
                            return true;
                        }

                        @Override
                        public void setWriteListener(WriteListener writeListener) {
                        }

                        @Override
                        public void write(int b) {
                            outputStream.write(b);
                        }

                        @Override
                        public void write(byte[] b) throws IOException {
                            outputStream.write(b);
                        }

                        @Override
                        public void write(byte[] b, int off, int len) {
                            outputStream.write(b, off, len);
                        }
                    });
                } else {
                    Mockito.when(response.getOutputStream()).thenThrow(responseException);
                }

                Mockito.doAnswer(invocation -> {
                    final String key = invocation.getArgument(0);
                    final String value = invocation.getArgument(1);
                    if (value == null) {
                        headersWithNoValue.add(key);
                    } else {
                        headersSent.put(key, value);
                    }

                    return null;
                }).when(response).setHeader(Mockito.any(String.class), Mockito.any(String.class));

                Mockito.doAnswer(invocation -> {
                    statusCode = invocation.getArgument(0);
                    return null;
                }).when(response).setStatus(Mockito.anyInt());

                return response;
            } catch (final Exception e) {
                e.printStackTrace();
                Assert.fail(e.toString());
                return null;
            }
        }

        @Override
        public void complete(final String identifier) {
            if (!id.equals(identifier)) {
                Assert.fail("attempting to respond to wrong request; should have been " + id + " but was " + identifier);
            }

            if (completeException != null) {
                throw completeException;
            }

            completedCount.incrementAndGet();
        }

        public int getCompletionCount() {
            return completedCount.get();
        }

        @Override
        public long getRequestTimeout(TimeUnit timeUnit) {
            return timeUnit.convert(30000, TimeUnit.MILLISECONDS);
        }
    }
}
