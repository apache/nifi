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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import javax.net.ssl.SSLContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.nifi.processors.standard.util.TestInvokeHttpCommon;
import org.apache.nifi.security.util.KeystoreType;
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.security.util.StandardTlsConfiguration;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunners;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestInvokeHTTP extends TestInvokeHttpCommon {
    private static final Logger logger = LoggerFactory.getLogger(TestInvokeHTTP.class);

    private static final String TRUSTSTORE_PATH = "src/test/resources/truststore.jks";
    private static final String TRUSTSTORE_PASSWORD = "passwordpassword";
    private static final KeystoreType TRUSTSTORE_TYPE = KeystoreType.JKS;
    private static final String KEYSTORE_PATH = "src/test/resources/keystore.jks";
    private static final String KEYSTORE_PASSWORD = "passwordpassword";
    private static final KeystoreType KEYSTORE_TYPE = KeystoreType.JKS;

    private static final TlsConfiguration TLS_CONFIGURATION = new StandardTlsConfiguration(
            KEYSTORE_PATH,
            KEYSTORE_PASSWORD,
            KEYSTORE_TYPE,
            TRUSTSTORE_PATH,
            TRUSTSTORE_PASSWORD,
            TRUSTSTORE_TYPE
    );

    @BeforeClass
    public static void beforeClass() throws Exception {
        configureServer(null, null);
    }

    @Before
    public void before() throws Exception {
        runner = TestRunners.newTestRunner(InvokeHTTP.class);
    }

    @Test
    public void testSslSetHttpRequest() throws Exception {
        final String serviceIdentifier = SSLContextService.class.getName();
        final SSLContextService sslContextService = Mockito.mock(SSLContextService.class);
        Mockito.when(sslContextService.getIdentifier()).thenReturn(serviceIdentifier);
        final SSLContext sslContext = SslContextFactory.createSslContext(TLS_CONFIGURATION);
        Mockito.when(sslContextService.createContext()).thenReturn(sslContext);
        Mockito.when(sslContextService.createTlsConfiguration()).thenReturn(TLS_CONFIGURATION);

        runner = TestRunners.newTestRunner(InvokeHTTP.class);

        runner.addControllerService(serviceIdentifier, sslContextService);
        runner.enableControllerService(sslContextService);
        runner.setProperty(InvokeHTTP.PROP_SSL_CONTEXT_SERVICE, serviceIdentifier);

        addHandler(new GetOrHeadHandler());

        runner.setProperty(InvokeHTTP.PROP_URL, url + "/status/200");

        createFlowFiles(runner);

        runner.run();

        runner.assertTransferCount(InvokeHTTP.REL_SUCCESS_REQ, 1);
        runner.assertTransferCount(InvokeHTTP.REL_RESPONSE, 1);
        runner.assertTransferCount(InvokeHTTP.REL_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_NO_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_FAILURE, 0);
        runner.assertPenalizeCount(0);

        // expected in request status.code and status.message
        // original flow file (+attributes)
        final MockFlowFile bundle = runner.getFlowFilesForRelationship(InvokeHTTP.REL_SUCCESS_REQ).get(0);
        bundle.assertContentEquals("Hello".getBytes(StandardCharsets.UTF_8));
        bundle.assertAttributeEquals(InvokeHTTP.STATUS_CODE, "200");
        bundle.assertAttributeEquals(InvokeHTTP.STATUS_MESSAGE, "OK");
        bundle.assertAttributeEquals("Foo", "Bar");

        // expected in response
        // status code, status message, all headers from server response --> ff attributes
        // server response message body into payload of ff
        final MockFlowFile bundle1 = runner.getFlowFilesForRelationship(InvokeHTTP.REL_RESPONSE).get(0);
        bundle1.assertContentEquals("/status/200".getBytes(StandardCharsets.UTF_8));
        bundle1.assertAttributeEquals(InvokeHTTP.STATUS_CODE, "200");
        bundle1.assertAttributeEquals(InvokeHTTP.STATUS_MESSAGE, "OK");
        bundle1.assertAttributeEquals("Foo", "Bar");
        bundle1.assertAttributeEquals("Content-Type", "text/plain;charset=iso-8859-1");
    }

    // Currently InvokeHttp does not support Proxy via Https
    @Test
    public void testProxy() throws Exception {
        addHandler(new MyProxyHandler());
        URL proxyURL = new URL(url);

        runner.setVariable("proxy.host", proxyURL.getHost());
        runner.setVariable("proxy.port", String.valueOf(proxyURL.getPort()));
        runner.setVariable("proxy.username", "username");
        runner.setVariable("proxy.password", "password");

        runner.setProperty(InvokeHTTP.PROP_URL, "http://nifi.apache.org/"); // just a dummy URL no connection goes out
        runner.setProperty(InvokeHTTP.PROP_PROXY_HOST, "${proxy.host}");

        try{
            runner.run();
            Assert.fail();
        } catch (AssertionError e){
            // Expect assertion error when proxy port isn't set but host is.
        }
        runner.setProperty(InvokeHTTP.PROP_PROXY_PORT, "${proxy.port}");

        runner.setProperty(InvokeHTTP.PROP_PROXY_USER, "${proxy.username}");

        try{
            runner.run();
            Assert.fail();
        } catch (AssertionError e){
            // Expect assertion error when proxy password isn't set but host is.
        }
        runner.setProperty(InvokeHTTP.PROP_PROXY_PASSWORD, "${proxy.password}");

        createFlowFiles(runner);

        runner.run();

        runner.assertTransferCount(InvokeHTTP.REL_SUCCESS_REQ, 1);
        runner.assertTransferCount(InvokeHTTP.REL_RESPONSE, 1);
        runner.assertTransferCount(InvokeHTTP.REL_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_NO_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_FAILURE, 0);
        runner.assertPenalizeCount(0);

        //expected in request status.code and status.message
        //original flow file (+attributes)
        final MockFlowFile bundle = runner.getFlowFilesForRelationship(InvokeHTTP.REL_SUCCESS_REQ).get(0);
        bundle.assertContentEquals("Hello".getBytes(StandardCharsets.UTF_8));
        bundle.assertAttributeEquals(InvokeHTTP.STATUS_CODE, "200");
        bundle.assertAttributeEquals(InvokeHTTP.STATUS_MESSAGE, "OK");
        bundle.assertAttributeEquals("Foo", "Bar");

        //expected in response
        //status code, status message, all headers from server response --> ff attributes
        //server response message body into payload of ff
        final MockFlowFile bundle1 = runner.getFlowFilesForRelationship(InvokeHTTP.REL_RESPONSE).get(0);
        bundle1.assertContentEquals("http://nifi.apache.org/".getBytes(StandardCharsets.UTF_8));
        bundle1.assertAttributeEquals(InvokeHTTP.STATUS_CODE, "200");
        bundle1.assertAttributeEquals(InvokeHTTP.STATUS_MESSAGE, "OK");
        bundle1.assertAttributeEquals("Foo", "Bar");
        bundle1.assertAttributeEquals("Content-Type", "text/plain;charset=iso-8859-1");
    }

    @Test
    public void testFailingHttpRequest() throws Exception {

        runner = TestRunners.newTestRunner(InvokeHTTP.class);

        // Remember: we expect that connecting to the following URL should raise a Java exception
        runner.setProperty(InvokeHTTP.PROP_URL, "http://127.0.0.1:0");

        createFlowFiles(runner);

        runner.run();

        runner.assertTransferCount(InvokeHTTP.REL_SUCCESS_REQ, 0);
        runner.assertTransferCount(InvokeHTTP.REL_RESPONSE, 0);
        runner.assertTransferCount(InvokeHTTP.REL_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_NO_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_FAILURE, 1);
        runner.assertPenalizeCount(1);

        // expected in request java.exception
        final MockFlowFile bundle = runner.getFlowFilesForRelationship(InvokeHTTP.REL_FAILURE).get(0);
        bundle.assertAttributeEquals(InvokeHTTP.EXCEPTION_CLASS, "java.lang.IllegalArgumentException");

    }

    public static class MyProxyHandler extends AbstractHandler {

        @Override
        public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException {
            baseRequest.setHandled(true);

            if ("Get".equalsIgnoreCase(request.getMethod())) {
                response.setStatus(200);
                String proxyPath = baseRequest.getHttpURI().toString();
                response.setContentLength(proxyPath.length());
                response.setContentType("text/plain");

                try (PrintWriter writer = response.getWriter()) {
                    writer.print(proxyPath);
                    writer.flush();
                }
            } else {
                response.setStatus(404);
                response.setContentType("text/plain");
                response.setContentLength(0);
            }
        }
    }

    @Test
    public void testOnPropertyModified() throws Exception {
        final InvokeHTTP processor = new InvokeHTTP();
        final Field regexAttributesToSendField = InvokeHTTP.class.getDeclaredField("regexAttributesToSend");
        regexAttributesToSendField.setAccessible(true);

        assertNull(regexAttributesToSendField.get(processor));

        // Set Attributes to Send.
        processor.onPropertyModified(InvokeHTTP.PROP_ATTRIBUTES_TO_SEND, null, "uuid");
        assertNotNull(regexAttributesToSendField.get(processor));

        // Null clear Attributes to Send. NIFI-1125: Throws NullPointerException.
        processor.onPropertyModified(InvokeHTTP.PROP_ATTRIBUTES_TO_SEND, "uuid", null);
        assertNull(regexAttributesToSendField.get(processor));

        // Set Attributes to Send.
        processor.onPropertyModified(InvokeHTTP.PROP_ATTRIBUTES_TO_SEND, null, "uuid");
        assertNotNull(regexAttributesToSendField.get(processor));

        // Clear Attributes to Send with empty string.
        processor.onPropertyModified(InvokeHTTP.PROP_ATTRIBUTES_TO_SEND, "uuid", "");
        assertNull(regexAttributesToSendField.get(processor));

    }

    @Test
    public void testEmptyGzipHttpReponse() throws Exception {
        addHandler(new EmptyGzipResponseHandler());

        runner.setProperty(InvokeHTTP.PROP_URL, url);
        runner.setProperty(InvokeHTTP.IGNORE_RESPONSE_CONTENT, "true");

        createFlowFiles(runner);

        runner.run();

        runner.assertTransferCount(InvokeHTTP.REL_SUCCESS_REQ, 1);
        runner.assertTransferCount(InvokeHTTP.REL_RESPONSE, 1);
        runner.assertTransferCount(InvokeHTTP.REL_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_NO_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_FAILURE, 0);
        runner.assertPenalizeCount(0);

        //expected empty content in response FlowFile
        final MockFlowFile bundle = runner.getFlowFilesForRelationship(InvokeHTTP.REL_RESPONSE).get(0);
        bundle.assertContentEquals(new byte[0]);
        bundle.assertAttributeEquals(InvokeHTTP.STATUS_CODE, "200");
        bundle.assertAttributeEquals(InvokeHTTP.STATUS_MESSAGE, "OK");
        bundle.assertAttributeEquals("Foo", "Bar");
        bundle.assertAttributeEquals("Content-Type", "text/plain");
    }


    @Test
    public void testShouldAllowExtension() {
        // Arrange
        class ExtendedInvokeHTTP extends InvokeHTTP {
            private final int extendedNumber;

            public ExtendedInvokeHTTP(int num) {
                super();
                this.extendedNumber = num;
            }

            public int extendedMethod() {
                return this.extendedNumber;
            }
        }

        int num = Double.valueOf(Math.random() * 100).intValue();

        // Act
        ExtendedInvokeHTTP eih = new ExtendedInvokeHTTP(num);

        // Assert
        assertEquals(num, eih.extendedMethod());
    }

    public static class EmptyGzipResponseHandler extends AbstractHandler {

        @Override
        public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) {
            baseRequest.setHandled(true);
            response.setStatus(200);
            response.setContentLength(0);
            response.setContentType("text/plain");
            response.setHeader("Content-Encoding", "gzip");
        }

    }

    @Test
    public void testShouldNotSendUserAgentByDefault() throws Exception {
        // Arrange
        addHandler(new EchoUserAgentHandler());

        runner.setProperty(InvokeHTTP.PROP_URL, url);

        createFlowFiles(runner);

        // Act
        runner.run();

        // Assert
        runner.assertTransferCount(InvokeHTTP.REL_SUCCESS_REQ, 1);
        runner.assertTransferCount(InvokeHTTP.REL_RESPONSE, 1);
        runner.assertTransferCount(InvokeHTTP.REL_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_NO_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_FAILURE, 0);
        runner.assertPenalizeCount(0);

        final MockFlowFile response = runner.getFlowFilesForRelationship(InvokeHTTP.REL_RESPONSE).get(0);
        String content = new String(response.toByteArray(), UTF_8);
        logger.info("Returned flowfile content: " + content);
        assertTrue(content.isEmpty());

        response.assertAttributeEquals(InvokeHTTP.STATUS_CODE, "200");
        response.assertAttributeEquals(InvokeHTTP.STATUS_MESSAGE, "OK");
    }

    @Test
    public void testShouldSetUserAgentExplicitly() throws Exception {
        addHandler(new EchoUserAgentHandler());

        runner.setProperty(InvokeHTTP.PROP_USERAGENT, "Apache NiFi For The Win");
        runner.setProperty(InvokeHTTP.PROP_URL, url);

        createFlowFiles(runner);

        runner.run();

        runner.assertTransferCount(InvokeHTTP.REL_SUCCESS_REQ, 1);
        runner.assertTransferCount(InvokeHTTP.REL_RESPONSE, 1);
        runner.assertTransferCount(InvokeHTTP.REL_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_NO_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_FAILURE, 0);
        runner.assertPenalizeCount(0);

        final MockFlowFile response = runner.getFlowFilesForRelationship(InvokeHTTP.REL_RESPONSE).get(0);
        String content = new String(response.toByteArray(), UTF_8);
        assertTrue(content.startsWith("Apache NiFi For The Win"));

        response.assertAttributeEquals(InvokeHTTP.STATUS_CODE, "200");
        response.assertAttributeEquals(InvokeHTTP.STATUS_MESSAGE, "OK");
    }

    @Test
    public void testShouldSetUserAgentWithExpressionLanguage() throws Exception {
        addHandler(new EchoUserAgentHandler());

        runner.setProperty(InvokeHTTP.PROP_URL, url);
        runner.setProperty(InvokeHTTP.PROP_USERAGENT, "${literal('And now for something completely different...')}");

        createFlowFiles(runner);

        runner.run();

        runner.assertTransferCount(InvokeHTTP.REL_SUCCESS_REQ, 1);
        runner.assertTransferCount(InvokeHTTP.REL_RESPONSE, 1);
        runner.assertTransferCount(InvokeHTTP.REL_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_NO_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_FAILURE, 0);
        runner.assertPenalizeCount(0);

        final MockFlowFile response = runner.getFlowFilesForRelationship(InvokeHTTP.REL_RESPONSE).get(0);
        // One check to verify a custom value and that the expression language actually works.
        response.assertContentEquals("And now for something completely different...");

        response.assertAttributeEquals(InvokeHTTP.STATUS_CODE, "200");
        response.assertAttributeEquals(InvokeHTTP.STATUS_MESSAGE, "OK");
    }

    public static class EchoUserAgentHandler extends AbstractHandler {

        @Override
        public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException {
            baseRequest.setHandled(true);

            if ("Get".equalsIgnoreCase(request.getMethod())) {
                response.setStatus(200);
                String useragent = request.getHeader("User-agent");
                response.setContentLength(useragent.length());
                response.setContentType("text/plain");

                try (PrintWriter writer = response.getWriter()) {
                    writer.print(useragent);
                    writer.flush();
                }
            } else {
                response.setStatus(404);
                response.setContentType("text/plain");
                response.setContentLength(0);
            }
        }
    }

}
