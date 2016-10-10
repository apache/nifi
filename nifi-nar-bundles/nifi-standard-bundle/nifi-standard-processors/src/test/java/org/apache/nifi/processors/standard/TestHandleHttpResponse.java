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
import java.util.concurrent.atomic.AtomicInteger;

import javax.servlet.AsyncContext;
import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.http.HttpContextMap;
import org.apache.nifi.processor.exception.FlowFileAccessException;
import org.apache.nifi.processors.standard.util.HTTPUtils;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestHandleHttpResponse {

    @Test
    public void testEnsureCompleted() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(HandleHttpResponse.class);

        final MockHttpContextMap contextMap = new MockHttpContextMap("my-id", false);
        runner.addControllerService("http-context-map", contextMap);
        runner.enableControllerService(contextMap);
        runner.setProperty(HandleHttpResponse.HTTP_CONTEXT_MAP, "http-context-map");
        runner.setProperty(HandleHttpResponse.STATUS_CODE, "${status.code}");
        runner.setProperty("my-attr", "${my-attr}");
        runner.setProperty("no-valid-attr", "${no-valid-attr}");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(HTTPUtils.HTTP_CONTEXT_ID, "my-id");
        attributes.put(HTTPUtils.HTTP_REQUEST_URI, "/test");
        attributes.put(HTTPUtils.HTTP_LOCAL_NAME, "server");
        attributes.put(HTTPUtils.HTTP_PORT, "8443");
        attributes.put(HTTPUtils.HTTP_REMOTE_HOST, "client");
        attributes.put(HTTPUtils.HTTP_SSL_CERT, "sslDN");
        attributes.put("my-attr", "hello");
        attributes.put("status.code", "201");

        runner.enqueue("hello".getBytes(), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(HandleHttpResponse.REL_SUCCESS, 1);
        assertTrue(runner.getProvenanceEvents().size() == 1);
        assertEquals(ProvenanceEventType.SEND, runner.getProvenanceEvents().get(0).getEventType());
        assertEquals("https://client@server:8443/test", runner.getProvenanceEvents().get(0).getTransitUri());

        assertEquals("hello", contextMap.baos.toString());
        assertEquals("hello", contextMap.headersSent.get("my-attr"));
        assertNull(contextMap.headersSent.get("no-valid-attr"));
        assertEquals(201, contextMap.statusCode);
        assertEquals(1, contextMap.getCompletionCount());
        assertTrue(contextMap.headersWithNoValue.isEmpty());
    }

    @Test
    public void testWithExceptionThrown() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(HandleHttpResponse.class);

        final MockHttpContextMap contextMap = new MockHttpContextMap("my-id", true);
        runner.addControllerService("http-context-map", contextMap);
        runner.enableControllerService(contextMap);
        runner.setProperty(HandleHttpResponse.HTTP_CONTEXT_MAP, "http-context-map");
        runner.setProperty(HandleHttpResponse.STATUS_CODE, "${status.code}");
        runner.setProperty("my-attr", "${my-attr}");
        runner.setProperty("no-valid-attr", "${no-valid-attr}");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(HTTPUtils.HTTP_CONTEXT_ID, "my-id");
        attributes.put("my-attr", "hello");
        attributes.put("status.code", "201");

        runner.enqueue("hello".getBytes(), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(HandleHttpResponse.REL_FAILURE, 1);
    }

    private static class MockHttpContextMap extends AbstractControllerService implements HttpContextMap {

        private final String id;
        private final AtomicInteger completedCount = new AtomicInteger(0);
        private final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        private final ConcurrentMap<String, String> headersSent = new ConcurrentHashMap<>();
        private final boolean shouldThrowException;
        private volatile int statusCode = -1;

        private final List<String> headersWithNoValue = new CopyOnWriteArrayList<>();

        public MockHttpContextMap(final String expectedIdentifier, final boolean shouldThrowException) {
            this.id = expectedIdentifier;
            this.shouldThrowException = shouldThrowException;
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
                if(shouldThrowException) {
                    Mockito.when(response.getOutputStream()).thenThrow(new FlowFileAccessException("exception"));
                } else {
                    Mockito.when(response.getOutputStream()).thenReturn(new ServletOutputStream() {
                        @Override
                        public boolean isReady() {
                            return true;
                        }

                        @Override
                        public void setWriteListener(WriteListener writeListener) {
                        }

                        @Override
                        public void write(int b) throws IOException {
                            baos.write(b);
                        }

                        @Override
                        public void write(byte[] b) throws IOException {
                            baos.write(b);
                        }

                        @Override
                        public void write(byte[] b, int off, int len) throws IOException {
                            baos.write(b, off, len);
                        }
                    });
                }

                Mockito.doAnswer(new Answer<Object>() {
                    @Override
                    public Object answer(final InvocationOnMock invocation) throws Throwable {
                        final String key = invocation.getArgumentAt(0, String.class);
                        final String value = invocation.getArgumentAt(1, String.class);
                        if (value == null) {
                            headersWithNoValue.add(key);
                        } else {
                            headersSent.put(key, value);
                        }

                        return null;
                    }
                }).when(response).setHeader(Mockito.any(String.class), Mockito.any(String.class));

                Mockito.doAnswer(new Answer<Object>() {
                    @Override
                    public Object answer(final InvocationOnMock invocation) throws Throwable {
                        statusCode = invocation.getArgumentAt(0, int.class);
                        return null;
                    }
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

            completedCount.incrementAndGet();
        }

        public int getCompletionCount() {
            return completedCount.get();
        }
    }
}
