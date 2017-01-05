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
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpServlet;
import javax.ws.rs.core.Response.Status;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.ssl.StandardSSLContextService;
import org.apache.nifi.util.FlowFileUnpackagerV3;
import org.apache.nifi.util.FlowFileValidator;
import org.apache.nifi.util.LogMessage;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.eclipse.jetty.servlet.ServletHandler;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.apache.nifi.processors.standard.PredictableResponseServlet;

public class TestPostHTTP {

    private TestServer server;
    private TestRunner runner;
    private HttpServlet servlet;

    private void setup(final Map<String, String> sslProperties) throws Exception {
        // set up web service
        ServletHandler handler = new ServletHandler();
        handler.addServletWithMapping(CaptureServlet.class, "/*");
        servlet = (HttpServlet) handler.getServlets()[0].getServlet();

        // create the service
        server = new TestServer(sslProperties);
        server.addHandler(handler);
        server.startServer();

        runner = TestRunners.newTestRunner(PostHTTP.class);
    }

    private void setup(final Map<String, String> sslProperties, final ServletHandler handler) throws Exception {
        servlet = (HttpServlet) handler.getServlets()[0].getServlet();

        // create the service
        server = new TestServer(sslProperties);
        server.addHandler(handler);
        server.startServer();

        runner = TestRunners.newTestRunner(PostHTTP.class);
    }

    @After
    public void cleanup() throws Exception {
        if (server != null) {
            server.shutdownServer();
            server = null;
        }
        runner = null;
        servlet = null;
    }

    @Test
    public void testTruststoreSSLOnly() throws Exception {
        final Map<String, String> sslProps = new HashMap<>();
        sslProps.put(TestServer.NEED_CLIENT_AUTH, "false");
        sslProps.put(StandardSSLContextService.KEYSTORE.getName(), "src/test/resources/localhost-ks.jks");
        sslProps.put(StandardSSLContextService.KEYSTORE_PASSWORD.getName(), "localtest");
        sslProps.put(StandardSSLContextService.KEYSTORE_TYPE.getName(), "JKS");
        setup(sslProps);

        final SSLContextService sslContextService = new StandardSSLContextService();
        runner.addControllerService("ssl-context", sslContextService);
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE, "src/test/resources/localhost-ts.jks");
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_PASSWORD, "localtest");
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_TYPE, "JKS");
        runner.enableControllerService(sslContextService);

        runner.setProperty(PostHTTP.URL, server.getSecureUrl());
        runner.setProperty(PostHTTP.SSL_CONTEXT_SERVICE, "ssl-context");
        runner.setProperty(PostHTTP.CHUNKED_ENCODING, "false");

        runner.enqueue("Hello world".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(PostHTTP.REL_SUCCESS, 1);
    }

    @Test
    public void testTwoWaySSL() throws Exception {
        final Map<String, String> sslProps = new HashMap<>();
        sslProps.put(StandardSSLContextService.KEYSTORE.getName(), "src/test/resources/localhost-ks.jks");
        sslProps.put(StandardSSLContextService.KEYSTORE_PASSWORD.getName(), "localtest");
        sslProps.put(StandardSSLContextService.KEYSTORE_TYPE.getName(), "JKS");
        sslProps.put(StandardSSLContextService.TRUSTSTORE.getName(), "src/test/resources/localhost-ts.jks");
        sslProps.put(StandardSSLContextService.TRUSTSTORE_PASSWORD.getName(), "localtest");
        sslProps.put(StandardSSLContextService.TRUSTSTORE_TYPE.getName(), "JKS");
        sslProps.put(TestServer.NEED_CLIENT_AUTH, "true");
        setup(sslProps);

        final SSLContextService sslContextService = new StandardSSLContextService();
        runner.addControllerService("ssl-context", sslContextService);
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE, "src/test/resources/localhost-ts.jks");
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_PASSWORD, "localtest");
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_TYPE, "JKS");
        runner.setProperty(sslContextService, StandardSSLContextService.KEYSTORE, "src/test/resources/localhost-ks.jks");
        runner.setProperty(sslContextService, StandardSSLContextService.KEYSTORE_PASSWORD, "localtest");
        runner.setProperty(sslContextService, StandardSSLContextService.KEYSTORE_TYPE, "JKS");
        runner.enableControllerService(sslContextService);

        runner.setProperty(PostHTTP.URL, server.getSecureUrl());
        runner.setProperty(PostHTTP.SSL_CONTEXT_SERVICE, "ssl-context");
        runner.setProperty(PostHTTP.CHUNKED_ENCODING, "false");

        runner.enqueue("Hello world".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(PostHTTP.REL_SUCCESS, 1);
    }

    @Test
    public void testOneWaySSLWhenServerConfiguredForTwoWay() throws Exception {
        final Map<String, String> sslProps = new HashMap<>();
        sslProps.put(StandardSSLContextService.KEYSTORE.getName(), "src/test/resources/localhost-ks.jks");
        sslProps.put(StandardSSLContextService.KEYSTORE_PASSWORD.getName(), "localtest");
        sslProps.put(StandardSSLContextService.KEYSTORE_TYPE.getName(), "JKS");
        sslProps.put(StandardSSLContextService.TRUSTSTORE.getName(), "src/test/resources/localhost-ts.jks");
        sslProps.put(StandardSSLContextService.TRUSTSTORE_PASSWORD.getName(), "localtest");
        sslProps.put(StandardSSLContextService.TRUSTSTORE_TYPE.getName(), "JKS");
        sslProps.put(TestServer.NEED_CLIENT_AUTH, "true");
        setup(sslProps);

        final SSLContextService sslContextService = new StandardSSLContextService();
        runner.addControllerService("ssl-context", sslContextService);
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE, "src/test/resources/localhost-ts.jks");
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_PASSWORD, "localtest");
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_TYPE, "JKS");
        runner.enableControllerService(sslContextService);

        runner.setProperty(PostHTTP.URL, server.getSecureUrl());
        runner.setProperty(PostHTTP.SSL_CONTEXT_SERVICE, "ssl-context");
        runner.setProperty(PostHTTP.CHUNKED_ENCODING, "false");

        runner.enqueue("Hello world".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(PostHTTP.REL_FAILURE, 1);
    }

    @Test
    public void testSendAsFlowFile() throws Exception {
        setup(null);
        runner.setProperty(PostHTTP.URL, server.getUrl());
        runner.setProperty(PostHTTP.SEND_AS_FLOWFILE, "true");

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("abc", "cba");

        runner.enqueue("Hello".getBytes(), attrs);
        attrs.put("abc", "abc");
        attrs.put("filename", "xyz.txt");
        runner.enqueue("World".getBytes(), attrs);

        runner.run(1);
        runner.assertAllFlowFilesTransferred(PostHTTP.REL_SUCCESS);

        final byte[] lastPost = ((CaptureServlet) servlet).getLastPost();
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final ByteArrayInputStream bais = new ByteArrayInputStream(lastPost);

        FlowFileUnpackagerV3 unpacker = new FlowFileUnpackagerV3();

        // unpack first flowfile received
        Map<String, String> receivedAttrs = unpacker.unpackageFlowFile(bais, baos);
        byte[] contentReceived = baos.toByteArray();
        assertEquals("Hello", new String(contentReceived));
        assertEquals("cba", receivedAttrs.get("abc"));

        assertTrue(unpacker.hasMoreData());

        baos.reset();
        receivedAttrs = unpacker.unpackageFlowFile(bais, baos);
        contentReceived = baos.toByteArray();

        assertEquals("World", new String(contentReceived));
        assertEquals("abc", receivedAttrs.get("abc"));
        assertEquals("xyz.txt", receivedAttrs.get("filename"));
        Assert.assertNull(receivedAttrs.get("Content-Length"));
    }

    @Test
    public void testSendAsFlowFileSecure() throws Exception {
        final Map<String, String> sslProps = new HashMap<>();
        sslProps.put(StandardSSLContextService.KEYSTORE.getName(), "src/test/resources/localhost-ks.jks");
        sslProps.put(StandardSSLContextService.KEYSTORE_PASSWORD.getName(), "localtest");
        sslProps.put(StandardSSLContextService.KEYSTORE_TYPE.getName(), "JKS");
        sslProps.put(StandardSSLContextService.TRUSTSTORE.getName(), "src/test/resources/localhost-ts.jks");
        sslProps.put(StandardSSLContextService.TRUSTSTORE_PASSWORD.getName(), "localtest");
        sslProps.put(StandardSSLContextService.TRUSTSTORE_TYPE.getName(), "JKS");
        sslProps.put(TestServer.NEED_CLIENT_AUTH, "true");
        setup(sslProps);

        final SSLContextService sslContextService = new StandardSSLContextService();
        runner.addControllerService("ssl-context", sslContextService);
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE, "src/test/resources/localhost-ts.jks");
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_PASSWORD, "localtest");
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_TYPE, "JKS");
        runner.setProperty(sslContextService, StandardSSLContextService.KEYSTORE, "src/test/resources/localhost-ks.jks");
        runner.setProperty(sslContextService, StandardSSLContextService.KEYSTORE_PASSWORD, "localtest");
        runner.setProperty(sslContextService, StandardSSLContextService.KEYSTORE_TYPE, "JKS");
        runner.enableControllerService(sslContextService);

        runner.setProperty(PostHTTP.URL, server.getSecureUrl());
        runner.setProperty(PostHTTP.SEND_AS_FLOWFILE, "true");
        runner.setProperty(PostHTTP.SSL_CONTEXT_SERVICE, "ssl-context");

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("abc", "cba");

        runner.enqueue("Hello".getBytes(), attrs);
        attrs.put("abc", "abc");
        attrs.put("filename", "xyz.txt");
        runner.enqueue("World".getBytes(), attrs);

        runner.run(1);
        runner.assertAllFlowFilesTransferred(PostHTTP.REL_SUCCESS);

        final byte[] lastPost = ((CaptureServlet) servlet).getLastPost();
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final ByteArrayInputStream bais = new ByteArrayInputStream(lastPost);

        FlowFileUnpackagerV3 unpacker = new FlowFileUnpackagerV3();

        // unpack first flowfile received
        Map<String, String> receivedAttrs = unpacker.unpackageFlowFile(bais, baos);
        byte[] contentReceived = baos.toByteArray();
        assertEquals("Hello", new String(contentReceived));
        assertEquals("cba", receivedAttrs.get("abc"));

        assertTrue(unpacker.hasMoreData());

        baos.reset();
        receivedAttrs = unpacker.unpackageFlowFile(bais, baos);
        contentReceived = baos.toByteArray();

        assertEquals("World", new String(contentReceived));
        assertEquals("abc", receivedAttrs.get("abc"));
        assertEquals("xyz.txt", receivedAttrs.get("filename"));
    }

    @Test
    public void testSendWithMimeType() throws Exception {
        setup(null);
        runner.setProperty(PostHTTP.URL, server.getUrl());

        final Map<String, String> attrs = new HashMap<>();

        final String suppliedMimeType = "text/plain";
        attrs.put(CoreAttributes.MIME_TYPE.key(), suppliedMimeType);
        runner.enqueue("Camping is great!".getBytes(), attrs);
        runner.setProperty(PostHTTP.CHUNKED_ENCODING, "false");

        runner.run(1);
        runner.assertAllFlowFilesTransferred(PostHTTP.REL_SUCCESS);

        Map<String, String> lastPostHeaders = ((CaptureServlet) servlet).getLastPostHeaders();
        Assert.assertEquals(suppliedMimeType, lastPostHeaders.get(PostHTTP.CONTENT_TYPE_HEADER));
        Assert.assertEquals("17",lastPostHeaders.get("Content-Length"));
    }

    @Test
    public void testSendWithEmptyELExpression() throws Exception {
        setup(null);
        runner.setProperty(PostHTTP.URL, server.getUrl());
        runner.setProperty(PostHTTP.CHUNKED_ENCODING, "false");

        final Map<String, String> attrs = new HashMap<>();
        attrs.put(CoreAttributes.MIME_TYPE.key(), "");
        runner.enqueue("The wilderness.".getBytes(), attrs);

        runner.run(1);
        runner.assertAllFlowFilesTransferred(PostHTTP.REL_SUCCESS);

        Map<String, String> lastPostHeaders = ((CaptureServlet) servlet).getLastPostHeaders();
        Assert.assertEquals(PostHTTP.DEFAULT_CONTENT_TYPE, lastPostHeaders.get(PostHTTP.CONTENT_TYPE_HEADER));
    }

    @Test
    public void testSendWithContentTypeProperty() throws Exception {
        setup(null);

        final String suppliedMimeType = "text/plain";
        runner.setProperty(PostHTTP.URL, server.getUrl());
        runner.setProperty(PostHTTP.CONTENT_TYPE, suppliedMimeType);
        runner.setProperty(PostHTTP.CHUNKED_ENCODING, "false");

        final Map<String, String> attrs = new HashMap<>();
        attrs.put(CoreAttributes.MIME_TYPE.key(), "text/csv");
        runner.enqueue("Sending with content type property.".getBytes(), attrs);

        runner.run(1);
        runner.assertAllFlowFilesTransferred(PostHTTP.REL_SUCCESS);

        Map<String, String> lastPostHeaders = ((CaptureServlet) servlet).getLastPostHeaders();
        Assert.assertEquals(suppliedMimeType, lastPostHeaders.get(PostHTTP.CONTENT_TYPE_HEADER));
    }

    @Test
    public void testSendWithCompressionServerAcceptGzip() throws Exception {
        setup(null);

        final String suppliedMimeType = "text/plain";
        runner.setProperty(PostHTTP.URL, server.getUrl());
        runner.setProperty(PostHTTP.CONTENT_TYPE, suppliedMimeType);
        runner.setProperty(PostHTTP.COMPRESSION_LEVEL, "9");

        final Map<String, String> attrs = new HashMap<>();
        attrs.put(CoreAttributes.MIME_TYPE.key(), "text/plain");

        runner.enqueue(StringUtils.repeat("Lines of sample text.", 100).getBytes(), attrs);

        runner.run(1);
        runner.assertAllFlowFilesTransferred(PostHTTP.REL_SUCCESS);

        Map<String, String> lastPostHeaders = ((CaptureServlet) servlet).getLastPostHeaders();
        Assert.assertEquals(suppliedMimeType, lastPostHeaders.get(PostHTTP.CONTENT_TYPE_HEADER));
        // Ensure that a 'Content-Encoding' header was set with a 'gzip' value
        Assert.assertEquals(PostHTTP.CONTENT_ENCODING_GZIP_VALUE, lastPostHeaders.get(PostHTTP.CONTENT_ENCODING_HEADER));
        Assert.assertNull(lastPostHeaders.get("Content-Length"));
    }

    @Test
    public void testSendWithoutCompressionServerAcceptGzip() throws Exception {
        setup(null);

        final String suppliedMimeType = "text/plain";
        runner.setProperty(PostHTTP.URL, server.getUrl());
        runner.setProperty(PostHTTP.CONTENT_TYPE, suppliedMimeType);
        runner.setProperty(PostHTTP.COMPRESSION_LEVEL, "0");
        runner.setProperty(PostHTTP.CHUNKED_ENCODING, "false");

        final Map<String, String> attrs = new HashMap<>();
        attrs.put(CoreAttributes.MIME_TYPE.key(), "text/plain");

        runner.enqueue(StringUtils.repeat("Lines of sample text.", 100).getBytes(), attrs);

        runner.run(1);
        runner.assertAllFlowFilesTransferred(PostHTTP.REL_SUCCESS);

        Map<String, String> lastPostHeaders = ((CaptureServlet) servlet).getLastPostHeaders();
        Assert.assertEquals(suppliedMimeType, lastPostHeaders.get(PostHTTP.CONTENT_TYPE_HEADER));
        // Ensure that the request was not sent with a 'Content-Encoding' header
        Assert.assertNull(lastPostHeaders.get(PostHTTP.CONTENT_ENCODING_HEADER));
        Assert.assertEquals("2100",lastPostHeaders.get("Content-Length"));
    }

    @Test
    public void testSendWithCompressionServerNotAcceptGzip() throws Exception {
        setup(null);

        final String suppliedMimeType = "text/plain";
        // Specify a property to the URL to have the CaptureServlet specify it doesn't accept gzip
        runner.setProperty(PostHTTP.URL, server.getUrl()+"?acceptGzip=false");
        runner.setProperty(PostHTTP.CONTENT_TYPE, suppliedMimeType);
        runner.setProperty(PostHTTP.COMPRESSION_LEVEL, "9");

        final Map<String, String> attrs = new HashMap<>();
        attrs.put(CoreAttributes.MIME_TYPE.key(), "text/plain");

        runner.enqueue(StringUtils.repeat("Lines of sample text.", 100).getBytes(), attrs);

        runner.run(1);
        runner.assertAllFlowFilesTransferred(PostHTTP.REL_SUCCESS);

        Map<String, String> lastPostHeaders = ((CaptureServlet) servlet).getLastPostHeaders();
        Assert.assertEquals(suppliedMimeType, lastPostHeaders.get(PostHTTP.CONTENT_TYPE_HEADER));
        // Ensure that the request was not sent with a 'Content-Encoding' header
        Assert.assertNull(lastPostHeaders.get(PostHTTP.CONTENT_ENCODING_HEADER));
    }

    @Test
    public void testSendChunked() throws Exception {
        setup(null);

        final String suppliedMimeType = "text/plain";
        runner.setProperty(PostHTTP.URL, server.getUrl());
        runner.setProperty(PostHTTP.CONTENT_TYPE, suppliedMimeType);
        runner.setProperty(PostHTTP.CHUNKED_ENCODING, "true");

        final Map<String, String> attrs = new HashMap<>();
        attrs.put(CoreAttributes.MIME_TYPE.key(), "text/plain");

        runner.enqueue(StringUtils.repeat("Lines of sample text.", 100).getBytes(), attrs);

        runner.run(1);
        runner.assertAllFlowFilesTransferred(PostHTTP.REL_SUCCESS);

        byte[] postValue = ((CaptureServlet) servlet).getLastPost();
        Assert.assertArrayEquals(StringUtils.repeat("Lines of sample text.", 100).getBytes(),postValue);

        Map<String, String> lastPostHeaders = ((CaptureServlet) servlet).getLastPostHeaders();
        Assert.assertEquals(suppliedMimeType, lastPostHeaders.get(PostHTTP.CONTENT_TYPE_HEADER));
        Assert.assertNull(lastPostHeaders.get("Content-Length"));
        Assert.assertEquals("chunked",lastPostHeaders.get("Transfer-Encoding"));
    }

    @Test
    public void testSendWithThrottler() throws Exception {
        setup(null);

        final String suppliedMimeType = "text/plain";
        runner.setProperty(PostHTTP.URL, server.getUrl());
        runner.setProperty(PostHTTP.CONTENT_TYPE, suppliedMimeType);
        runner.setProperty(PostHTTP.CHUNKED_ENCODING, "false");
        runner.setProperty(PostHTTP.MAX_DATA_RATE, "10kb");

        final Map<String, String> attrs = new HashMap<>();
        attrs.put(CoreAttributes.MIME_TYPE.key(), "text/plain");

        runner.enqueue(StringUtils.repeat("This is a line of sample text. Here is another.", 100).getBytes(), attrs);

        boolean stopOnFinish = true;
        runner.run(1, stopOnFinish);
        runner.assertAllFlowFilesTransferred(PostHTTP.REL_SUCCESS);

        byte[] postValue = ((CaptureServlet) servlet).getLastPost();
        Assert.assertArrayEquals(StringUtils.repeat("This is a line of sample text. Here is another.", 100).getBytes(),postValue);

        Map<String, String> lastPostHeaders = ((CaptureServlet) servlet).getLastPostHeaders();
        Assert.assertEquals(suppliedMimeType, lastPostHeaders.get(PostHTTP.CONTENT_TYPE_HEADER));
        Assert.assertEquals("4700",lastPostHeaders.get("Content-Length"));
    }

    @Test
    public void testDefaultUserAgent() throws Exception {
        setup(null);
        Assert.assertTrue(runner.getProcessContext().getProperty(PostHTTP.USER_AGENT).getValue().startsWith("Apache-HttpClient"));
    }

    @Test
    public void testSendWithResponseBodyDestinationAttribute() throws Exception {

        final ServletHandler handler = new ServletHandler();
        handler.addServletWithMapping(PredictableResponseServlet.class, "/*");

        setup(null, handler);

        ((PredictableResponseServlet) servlet).statusCode = Status.OK.getStatusCode();
        ((PredictableResponseServlet) servlet).responseBody = "{}";
        ((PredictableResponseServlet) servlet).headers = new HashMap<>();
        ((PredictableResponseServlet) servlet).headers.put("Content-Type", "application/json; charset=utf-8");

        final String suppliedMimeType = "text/plain";
        runner.setProperty(PostHTTP.URL, server.getUrl());
        runner.setProperty(PostHTTP.CONTENT_TYPE, suppliedMimeType);
        runner.setProperty(PostHTTP.CHUNKED_ENCODING, "true");
        runner.setProperty(PostHTTP.RESPONSE_BODY_DESTINATION, PostHTTP.DESTINATION_ATTRIBUTE);
        runner.setProperty(PostHTTP.RESPONSE_BODY_ATTRIBUTE, PostHTTP.RESPONSE_BODY_ATTRIBUTE.getDefaultValue());

        final Map<String, String> attrs = new HashMap<>();
        attrs.put(CoreAttributes.MIME_TYPE.key(), suppliedMimeType);

        runner.enqueue(StringUtils.repeat("This is a line of sample text. Here is another.", 100).getBytes(), attrs);

        runner.run(1);

        List<LogMessage> warnings = runner.getLogger().getWarnMessages();
        List<LogMessage> errors = runner.getLogger().getErrorMessages();

        Assert.assertEquals(0, warnings.size());
        Assert.assertEquals(0, errors.size());

        runner.assertAllFlowFilesTransferred(PostHTTP.REL_SUCCESS);

        runner.assertAllFlowFiles(new FlowFileValidator() {
            public void assertFlowFile(FlowFile f) {
                // Assert outgoing flowfile attribute is response body
                Assert.assertEquals("{}", f.getAttribute(PostHTTP.RESPONSE_BODY_ATTRIBUTE.getDefaultValue()));
                // Assert outgoing flowfile attribute mime-type not updated
                Assert.assertEquals(suppliedMimeType, f.getAttribute(CoreAttributes.MIME_TYPE.key()));
            }
        });
    }

    @Test
    public void testSendWithResponseBodyDestinationContent() throws Exception {

        final ServletHandler handler = new ServletHandler();
        handler.addServletWithMapping(PredictableResponseServlet.class, "/*");

        setup(null, handler);

        ((PredictableResponseServlet) servlet).statusCode = Status.OK.getStatusCode();
        ((PredictableResponseServlet) servlet).responseBody = "{}";
        ((PredictableResponseServlet) servlet).headers = new HashMap<>();
        ((PredictableResponseServlet) servlet).headers.put("Content-Type", "application/json; charset=utf-8");

        final String suppliedMimeType = "text/plain";
        runner.setProperty(PostHTTP.URL, server.getUrl());
        runner.setProperty(PostHTTP.CONTENT_TYPE, suppliedMimeType);
        runner.setProperty(PostHTTP.CHUNKED_ENCODING, "true");
        runner.setProperty(PostHTTP.RESPONSE_BODY_DESTINATION, PostHTTP.DESTINATION_CONTENT);

        final Map<String, String> attrs = new HashMap<>();
        attrs.put(CoreAttributes.MIME_TYPE.key(), suppliedMimeType);

        runner.enqueue(StringUtils.repeat("This is a line of sample text. Here is another.", 100).getBytes(), attrs);

        runner.run(1);

        List<LogMessage> warnings = runner.getLogger().getWarnMessages();
        List<LogMessage> errors = runner.getLogger().getErrorMessages();

        Assert.assertEquals(0, warnings.size());
        Assert.assertEquals(0, errors.size());

        runner.assertAllFlowFilesTransferred(PostHTTP.REL_SUCCESS);

        runner.assertAllFlowFiles(new FlowFileValidator() {
            public void assertFlowFile(FlowFile f) {
                MockFlowFile mf = (MockFlowFile) f;
                // Assert outgoing flowfile content is response body
                mf.assertContentEquals("{}");
                // Assert flowfile mimetype is updated to reflect response content type
                mf.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/json");
            }
        });
    }

    @Test
    public void testSendWithResponseCodeAttribute() throws Exception {

        final ServletHandler handler = new ServletHandler();
        handler.addServletWithMapping(PredictableResponseServlet.class, "/*");

        setup(null, handler);

        ((PredictableResponseServlet) servlet).statusCode = Status.OK.getStatusCode();
        ((PredictableResponseServlet) servlet).responseBody = "{}";
        ((PredictableResponseServlet) servlet).headers = new HashMap<>();
        ((PredictableResponseServlet) servlet).headers.put("Content-Type", "application/json; charset=utf-8");

        final String suppliedMimeType = "text/plain";
        runner.setProperty(PostHTTP.URL, server.getUrl());
        runner.setProperty(PostHTTP.CONTENT_TYPE, suppliedMimeType);
        runner.setProperty(PostHTTP.CHUNKED_ENCODING, "true");
        runner.setProperty(PostHTTP.RESPONSE_CODE_ATTRIBUTE, PostHTTP.RESPONSE_CODE_ATTRIBUTE.getDefaultValue());

        final Map<String, String> attrs = new HashMap<>();
        attrs.put(CoreAttributes.MIME_TYPE.key(), suppliedMimeType);

        runner.enqueue(StringUtils.repeat("This is a line of sample text. Here is another.", 100).getBytes(), attrs);

        runner.run(1);

        List<LogMessage> warnings = runner.getLogger().getWarnMessages();
        List<LogMessage> errors = runner.getLogger().getErrorMessages();

        Assert.assertEquals(0, warnings.size());
        Assert.assertEquals(0, errors.size());

        runner.assertAllFlowFilesTransferred(PostHTTP.REL_SUCCESS);

        runner.assertAllFlowFiles(new FlowFileValidator() {
            public void assertFlowFile(FlowFile f) {
                // Assert outgoing flowfile attribute is response code
                Assert.assertEquals("200", f.getAttribute(PostHTTP.RESPONSE_CODE_ATTRIBUTE.getDefaultValue()));
                // Assert outgoing flowfile attribute mime-type not updated
                Assert.assertEquals(suppliedMimeType, f.getAttribute(CoreAttributes.MIME_TYPE.key()));
            }
        });
    }

}
