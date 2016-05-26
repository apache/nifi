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
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.ssl.StandardSSLContextService;
import org.apache.nifi.util.FlowFileUnpackagerV3;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.eclipse.jetty.servlet.ServletHandler;
import org.junit.After;
import org.junit.Test;
import org.junit.Assert;

public class TestPostHTTP {

    private TestServer server;
    private TestRunner runner;
    private CaptureServlet servlet;

    private void setup(final Map<String, String> sslProperties) throws Exception {
        // set up web service
        ServletHandler handler = new ServletHandler();
        handler.addServletWithMapping(CaptureServlet.class, "/*");
        servlet = (CaptureServlet) handler.getServlets()[0].getServlet();

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

        final byte[] lastPost = servlet.getLastPost();
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

        final byte[] lastPost = servlet.getLastPost();
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

        Map<String, String> lastPostHeaders = servlet.getLastPostHeaders();
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

        Map<String, String> lastPostHeaders = servlet.getLastPostHeaders();
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

        Map<String, String> lastPostHeaders = servlet.getLastPostHeaders();
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

        Map<String, String> lastPostHeaders = servlet.getLastPostHeaders();
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

        Map<String, String> lastPostHeaders = servlet.getLastPostHeaders();
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

        Map<String, String> lastPostHeaders = servlet.getLastPostHeaders();
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

        byte[] postValue = servlet.getLastPost();
        Assert.assertArrayEquals(StringUtils.repeat("Lines of sample text.", 100).getBytes(),postValue);

        Map<String, String> lastPostHeaders = servlet.getLastPostHeaders();
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

        byte[] postValue = servlet.getLastPost();
        Assert.assertArrayEquals(StringUtils.repeat("This is a line of sample text. Here is another.", 100).getBytes(),postValue);

        Map<String, String> lastPostHeaders = servlet.getLastPostHeaders();
        Assert.assertEquals(suppliedMimeType, lastPostHeaders.get(PostHTTP.CONTENT_TYPE_HEADER));
        Assert.assertEquals("4700",lastPostHeaders.get("Content-Length"));
    }
}
