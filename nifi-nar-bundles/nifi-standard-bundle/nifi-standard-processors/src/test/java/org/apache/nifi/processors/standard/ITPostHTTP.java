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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.remote.io.socket.NetworkUtils;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.security.util.ClientAuth;
import org.apache.nifi.security.util.TlsException;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.FlowFileUnpackagerV3;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.web.util.JettyServerUtils;
import org.apache.nifi.web.util.ssl.SslContextUtils;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletHandler;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import javax.net.ssl.SSLContext;

/**
 * Integration Test for deprecated PostHTTP Processor
 */
@SuppressWarnings("deprecation")
public class ITPostHTTP {
    private Server server;
    private TestRunner runner;
    private CaptureServlet servlet;

    private static final String SSL_CONTEXT_IDENTIFIER = SSLContextService.class.getName();

    private static final String TEST_MESSAGE = String.class.getName();

    private static SSLContext keyStoreSslContext;

    private static SSLContext trustStoreSslContext;

    @BeforeClass
    public static void configureServices() throws TlsException {
        keyStoreSslContext = SslContextUtils.createKeyStoreSslContext();
        trustStoreSslContext = SslContextUtils.createTrustStoreSslContext();
    }

    private static String getUrl(final SSLContext sslContext, final int port) {
        final String protocol = sslContext == null ? "http" : "https";
        return String.format("%s://localhost:%d", protocol, port);
    }

    private void setup(final SSLContext serverSslContext, final ClientAuth clientAuth) throws Exception {
        runner = TestRunners.newTestRunner(org.apache.nifi.processors.standard.PostHTTP.class);
        final int port = NetworkUtils.availablePort();
        runner.setProperty(org.apache.nifi.processors.standard.PostHTTP.URL, getUrl(serverSslContext, port));

        // set up web service
        ServletHandler handler = new ServletHandler();
        handler.addServletWithMapping(CaptureServlet.class, "/*");

        final Server configuredServer = JettyServerUtils.createServer(port, serverSslContext, clientAuth);
        configuredServer.setHandler(handler);
        final ServerConnector connector = new ServerConnector(configuredServer);
        connector.setPort(port);

        JettyServerUtils.startServer(configuredServer);

        servlet = (CaptureServlet) handler.getServlets()[0].getServlet();
    }

    @After
    public void cleanup() throws Exception {
        if (server != null) {
            server.stop();
            server.destroy();
            server = null;
        }
    }

    @Test
    public void testUnauthenticatedTls() throws Exception {
        setup(keyStoreSslContext, ClientAuth.NONE);
        enableSslContextService(trustStoreSslContext);

        runner.setProperty(org.apache.nifi.processors.standard.PostHTTP.CHUNKED_ENCODING, Boolean.FALSE.toString());
        runner.enqueue(TEST_MESSAGE);
        runner.run();

        runner.assertAllFlowFilesTransferred(org.apache.nifi.processors.standard.PostHTTP.REL_SUCCESS, 1);
    }

    @Test
    public void testMutualTls() throws Exception {
        setup(keyStoreSslContext, ClientAuth.REQUIRED);
        enableSslContextService(keyStoreSslContext);

        runner.setProperty(org.apache.nifi.processors.standard.PostHTTP.CHUNKED_ENCODING, Boolean.FALSE.toString());
        runner.enqueue(TEST_MESSAGE);
        runner.run();

        runner.assertAllFlowFilesTransferred(org.apache.nifi.processors.standard.PostHTTP.REL_SUCCESS, 1);
    }

    @Test
    public void testMutualTlsClientCertificateMissing() throws Exception {
        setup(keyStoreSslContext, ClientAuth.REQUIRED);
        enableSslContextService(trustStoreSslContext);

        runner.setProperty(org.apache.nifi.processors.standard.PostHTTP.CHUNKED_ENCODING, Boolean.FALSE.toString());
        runner.enqueue(TEST_MESSAGE);
        runner.run();

        runner.assertAllFlowFilesTransferred(org.apache.nifi.processors.standard.PostHTTP.REL_FAILURE, 1);
    }

    @Test
    public void testSendAsFlowFile() throws Exception {
        setup( null, null);

        runner.setProperty(org.apache.nifi.processors.standard.PostHTTP.SEND_AS_FLOWFILE, "true");

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("abc", "cba");

        runner.enqueue(TEST_MESSAGE, attrs);
        attrs.put("abc", "abc");
        attrs.put("filename", "xyz.txt");
        runner.enqueue("World".getBytes(), attrs);

        runner.run(1);
        runner.assertAllFlowFilesTransferred(org.apache.nifi.processors.standard.PostHTTP.REL_SUCCESS);

        final byte[] lastPost = servlet.getLastPost();
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final ByteArrayInputStream bais = new ByteArrayInputStream(lastPost);

        FlowFileUnpackagerV3 unpacker = new FlowFileUnpackagerV3();

        // unpack first flowfile received
        Map<String, String> receivedAttrs = unpacker.unpackageFlowFile(bais, baos);
        byte[] contentReceived = baos.toByteArray();
        assertEquals(TEST_MESSAGE, new String(contentReceived));
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
    public void testMutualTlsSendFlowFile() throws Exception {
        setup(keyStoreSslContext, ClientAuth.REQUIRED);
        enableSslContextService(keyStoreSslContext);

        runner.setProperty(org.apache.nifi.processors.standard.PostHTTP.SEND_AS_FLOWFILE, "true");

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("abc", "cba");

        runner.enqueue(TEST_MESSAGE, attrs);
        attrs.put("abc", "abc");
        attrs.put("filename", "xyz.txt");
        runner.enqueue("World".getBytes(), attrs);

        runner.run(1);
        runner.assertAllFlowFilesTransferred(org.apache.nifi.processors.standard.PostHTTP.REL_SUCCESS);

        final byte[] lastPost = servlet.getLastPost();
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final ByteArrayInputStream bais = new ByteArrayInputStream(lastPost);

        FlowFileUnpackagerV3 unpacker = new FlowFileUnpackagerV3();

        // unpack first flowfile received
        Map<String, String> receivedAttrs = unpacker.unpackageFlowFile(bais, baos);
        byte[] contentReceived = baos.toByteArray();
        assertEquals(TEST_MESSAGE, new String(contentReceived));
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
        setup(null, null);

        final Map<String, String> attrs = new HashMap<>();

        final String suppliedMimeType = "text/plain";
        attrs.put(CoreAttributes.MIME_TYPE.key(), suppliedMimeType);
        runner.enqueue("Camping is great!".getBytes(), attrs);
        runner.setProperty(org.apache.nifi.processors.standard.PostHTTP.CHUNKED_ENCODING, Boolean.FALSE.toString());

        runner.run(1);
        runner.assertAllFlowFilesTransferred(org.apache.nifi.processors.standard.PostHTTP.REL_SUCCESS);

        Map<String, String> lastPostHeaders = servlet.getLastPostHeaders();
        Assert.assertEquals(suppliedMimeType, lastPostHeaders.get(org.apache.nifi.processors.standard.PostHTTP.CONTENT_TYPE_HEADER));
        Assert.assertEquals("17",lastPostHeaders.get("Content-Length"));
    }

    @Test
    public void testSendWithEmptyELExpression() throws Exception {
        setup( null, null);

        runner.setProperty(org.apache.nifi.processors.standard.PostHTTP.CHUNKED_ENCODING, Boolean.FALSE.toString());

        final Map<String, String> attrs = new HashMap<>();
        attrs.put(CoreAttributes.MIME_TYPE.key(), "");
        runner.enqueue("The wilderness.".getBytes(), attrs);

        runner.run(1);
        runner.assertAllFlowFilesTransferred(org.apache.nifi.processors.standard.PostHTTP.REL_SUCCESS);

        Map<String, String> lastPostHeaders = servlet.getLastPostHeaders();
        Assert.assertEquals(org.apache.nifi.processors.standard.PostHTTP.DEFAULT_CONTENT_TYPE, lastPostHeaders.get(org.apache.nifi.processors.standard.PostHTTP.CONTENT_TYPE_HEADER));
    }

    @Test
    public void testSendWithContentTypeProperty() throws Exception {
        setup(null, null);

        final String suppliedMimeType = "text/plain";
        runner.setProperty(org.apache.nifi.processors.standard.PostHTTP.CONTENT_TYPE, suppliedMimeType);
        runner.setProperty(org.apache.nifi.processors.standard.PostHTTP.CHUNKED_ENCODING, Boolean.FALSE.toString());

        final Map<String, String> attrs = new HashMap<>();
        attrs.put(CoreAttributes.MIME_TYPE.key(), "text/csv");
        runner.enqueue("Sending with content type property.".getBytes(), attrs);

        runner.run(1);
        runner.assertAllFlowFilesTransferred(org.apache.nifi.processors.standard.PostHTTP.REL_SUCCESS);

        Map<String, String> lastPostHeaders = servlet.getLastPostHeaders();
        Assert.assertEquals(suppliedMimeType, lastPostHeaders.get(org.apache.nifi.processors.standard.PostHTTP.CONTENT_TYPE_HEADER));
    }

    @Test
    public void testSendWithCompressionServerAcceptGzip() throws Exception {
        setup(null, null);

        final String suppliedMimeType = "text/plain";
        runner.setProperty(org.apache.nifi.processors.standard.PostHTTP.CONTENT_TYPE, suppliedMimeType);
        runner.setProperty(org.apache.nifi.processors.standard.PostHTTP.COMPRESSION_LEVEL, "9");

        final Map<String, String> attrs = new HashMap<>();
        attrs.put(CoreAttributes.MIME_TYPE.key(), "text/plain");

        runner.enqueue(StringUtils.repeat("Lines of sample text.", 100).getBytes(), attrs);

        runner.run(1);
        runner.assertAllFlowFilesTransferred(org.apache.nifi.processors.standard.PostHTTP.REL_SUCCESS);

        Map<String, String> lastPostHeaders = servlet.getLastPostHeaders();
        Assert.assertEquals(suppliedMimeType, lastPostHeaders.get(org.apache.nifi.processors.standard.PostHTTP.CONTENT_TYPE_HEADER));
        // Ensure that a 'Content-Encoding' header was set with a 'gzip' value
        Assert.assertEquals(org.apache.nifi.processors.standard.PostHTTP.CONTENT_ENCODING_GZIP_VALUE, lastPostHeaders.get(org.apache.nifi.processors.standard.PostHTTP.CONTENT_ENCODING_HEADER));
        Assert.assertNull(lastPostHeaders.get("Content-Length"));
    }

    @Test
    public void testSendWithoutCompressionServerAcceptGzip() throws Exception {
        setup(null, null);

        final String suppliedMimeType = "text/plain";
        runner.setProperty(org.apache.nifi.processors.standard.PostHTTP.CONTENT_TYPE, suppliedMimeType);
        runner.setProperty(org.apache.nifi.processors.standard.PostHTTP.COMPRESSION_LEVEL, "0");
        runner.setProperty(org.apache.nifi.processors.standard.PostHTTP.CHUNKED_ENCODING, "false");

        final Map<String, String> attrs = new HashMap<>();
        attrs.put(CoreAttributes.MIME_TYPE.key(), "text/plain");

        runner.enqueue(StringUtils.repeat("Lines of sample text.", 100).getBytes(), attrs);

        runner.run(1);
        runner.assertAllFlowFilesTransferred(org.apache.nifi.processors.standard.PostHTTP.REL_SUCCESS);

        Map<String, String> lastPostHeaders = servlet.getLastPostHeaders();
        Assert.assertEquals(suppliedMimeType, lastPostHeaders.get(org.apache.nifi.processors.standard.PostHTTP.CONTENT_TYPE_HEADER));
        // Ensure that the request was not sent with a 'Content-Encoding' header
        Assert.assertNull(lastPostHeaders.get(org.apache.nifi.processors.standard.PostHTTP.CONTENT_ENCODING_HEADER));
        Assert.assertEquals("2100",lastPostHeaders.get("Content-Length"));
    }

    @Test
    public void testSendWithCompressionServerNotAcceptGzip() throws Exception {
        setup(null, null);

        final String suppliedMimeType = "text/plain";
        // Specify a property to the URL to have the CaptureServlet specify it doesn't accept gzip

        final String serverUrl = runner.getProcessContext().getProperty(PostHTTP.URL).getValue();
        final String url = String.format("%s?acceptGzip=false", serverUrl);
        runner.setProperty(org.apache.nifi.processors.standard.PostHTTP.URL, url);
        runner.setProperty(org.apache.nifi.processors.standard.PostHTTP.CONTENT_TYPE, suppliedMimeType);
        runner.setProperty(org.apache.nifi.processors.standard.PostHTTP.COMPRESSION_LEVEL, "9");

        final Map<String, String> attrs = new HashMap<>();
        attrs.put(CoreAttributes.MIME_TYPE.key(), "text/plain");

        runner.enqueue(StringUtils.repeat("Lines of sample text.", 100).getBytes(), attrs);

        runner.run(1);
        runner.assertAllFlowFilesTransferred(org.apache.nifi.processors.standard.PostHTTP.REL_SUCCESS);

        Map<String, String> lastPostHeaders = servlet.getLastPostHeaders();
        Assert.assertEquals(suppliedMimeType, lastPostHeaders.get(org.apache.nifi.processors.standard.PostHTTP.CONTENT_TYPE_HEADER));
        // Ensure that the request was not sent with a 'Content-Encoding' header
        Assert.assertNull(lastPostHeaders.get(org.apache.nifi.processors.standard.PostHTTP.CONTENT_ENCODING_HEADER));
    }

    @Test
    public void testSendChunked() throws Exception {
        setup(null, null);

        final String suppliedMimeType = "text/plain";
        runner.setProperty(org.apache.nifi.processors.standard.PostHTTP.CONTENT_TYPE, suppliedMimeType);
        runner.setProperty(org.apache.nifi.processors.standard.PostHTTP.CHUNKED_ENCODING, Boolean.TRUE.toString());

        final Map<String, String> attrs = new HashMap<>();
        attrs.put(CoreAttributes.MIME_TYPE.key(), "text/plain");

        runner.enqueue(StringUtils.repeat("Lines of sample text.", 100).getBytes(), attrs);

        runner.run(1);
        runner.assertAllFlowFilesTransferred(org.apache.nifi.processors.standard.PostHTTP.REL_SUCCESS);

        byte[] postValue = servlet.getLastPost();
        Assert.assertArrayEquals(StringUtils.repeat("Lines of sample text.", 100).getBytes(),postValue);

        Map<String, String> lastPostHeaders = servlet.getLastPostHeaders();
        Assert.assertEquals(suppliedMimeType, lastPostHeaders.get(org.apache.nifi.processors.standard.PostHTTP.CONTENT_TYPE_HEADER));
        Assert.assertNull(lastPostHeaders.get("Content-Length"));
        Assert.assertEquals("chunked",lastPostHeaders.get("Transfer-Encoding"));
    }

    @Test
    public void testSendWithThrottler() throws Exception {
        setup(null, null);

        final String suppliedMimeType = "text/plain";
        runner.setProperty(org.apache.nifi.processors.standard.PostHTTP.CONTENT_TYPE, suppliedMimeType);
        runner.setProperty(org.apache.nifi.processors.standard.PostHTTP.CHUNKED_ENCODING, "false");
        runner.setProperty(org.apache.nifi.processors.standard.PostHTTP.MAX_DATA_RATE, "10kb");

        final Map<String, String> attrs = new HashMap<>();
        attrs.put(CoreAttributes.MIME_TYPE.key(), "text/plain");

        runner.enqueue(StringUtils.repeat("This is a line of sample text. Here is another.", 100).getBytes(), attrs);

        boolean stopOnFinish = true;
        runner.run(1, stopOnFinish);
        runner.assertAllFlowFilesTransferred(org.apache.nifi.processors.standard.PostHTTP.REL_SUCCESS);

        byte[] postValue = servlet.getLastPost();
        Assert.assertArrayEquals(StringUtils.repeat("This is a line of sample text. Here is another.", 100).getBytes(),postValue);

        Map<String, String> lastPostHeaders = servlet.getLastPostHeaders();
        Assert.assertEquals(suppliedMimeType, lastPostHeaders.get(org.apache.nifi.processors.standard.PostHTTP.CONTENT_TYPE_HEADER));
        Assert.assertEquals("4700",lastPostHeaders.get("Content-Length"));
    }

    @Test
    public void testDefaultUserAgent() throws Exception {
        setup(null, null);
        Assert.assertTrue(runner.getProcessContext().getProperty(org.apache.nifi.processors.standard.PostHTTP.USER_AGENT).getValue().startsWith("Apache-HttpClient"));
    }

    @Test
    public void testBatchWithMultipleUrls() throws Exception {
        setup(null,null);
        final CaptureServlet servletA = servlet;
        final String urlA = runner.getProcessContext().getProperty(org.apache.nifi.processors.standard.PostHTTP.URL).getValue();

        // set up second web service
        ServletHandler handler = new ServletHandler();
        handler.addServletWithMapping(CaptureServlet.class, "/*");

        // create the second service
        final int portB = NetworkUtils.availablePort();
        final String urlB = getUrl(null, portB);
        final Server serverB = JettyServerUtils.createServer(portB, null, null);
        serverB.setHandler(handler);
        JettyServerUtils.startServer(serverB);

        final CaptureServlet servletB = (CaptureServlet) handler.getServlets()[0].getServlet();

        runner.setProperty(org.apache.nifi.processors.standard.PostHTTP.URL, "${url}"); // use EL for the URL
        runner.setProperty(org.apache.nifi.processors.standard.PostHTTP.SEND_AS_FLOWFILE, "true");
        runner.setProperty(org.apache.nifi.processors.standard.PostHTTP.MAX_BATCH_SIZE, "10 b");

        Set<String> expectedContentA = new HashSet<>();
        Set<String> expectedContentB = new HashSet<>();

        Set<String> actualContentA = new HashSet<>();
        Set<String> actualContentB = new HashSet<>();

        // enqueue 9 FlowFiles
        for (int i = 0; i < 9; i++) {
            enqueueWithURL("a" + i, urlA);
            enqueueWithURL("b" + i, urlB);

            expectedContentA.add("a" + i);
            expectedContentB.add("b" + i);
        }

        // MAX_BATCH_SIZE is 10 bytes, each file is 2 bytes, so 18 files should produce 4 batches
        for (int i = 0; i < 4; i++) {
            runner.run(1);
            runner.assertAllFlowFilesTransferred(org.apache.nifi.processors.standard.PostHTTP.REL_SUCCESS);
            final List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(org.apache.nifi.processors.standard.PostHTTP.REL_SUCCESS);
            assertFalse(successFiles.isEmpty());

            MockFlowFile mff = successFiles.get(0);
            final String urlAttr = mff.getAttribute("url");

            if (urlA.equals(urlAttr)) {
                checkBatch(urlA, servletA, actualContentA, (actualContentA.isEmpty() ? 5 : 4));
            } else if (urlB.equals(urlAttr)) {
                checkBatch(urlB, servletB, actualContentB, (actualContentB.isEmpty() ? 5 : 4));
            } else {
                fail("unexpected url attribute");
            }
        }

        assertEquals(expectedContentA, actualContentA);
        assertEquals(expectedContentB, actualContentB);

        // make sure everything transferred, nothing more to do
        runner.run(1);
        runner.assertAllFlowFilesTransferred(org.apache.nifi.processors.standard.PostHTTP.REL_SUCCESS, 0);
    }

    private void enqueueWithURL(String data, String url) {
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("url", url);
        runner.enqueue(data.getBytes(), attrs);
    }

    private void checkBatch(final String url, CaptureServlet servlet, Set<String> actualContent, int expectedCount) throws Exception {
        FlowFileUnpackagerV3 unpacker = new FlowFileUnpackagerV3();
        Set<String> actualFFContent = new HashSet<>();
        Set<String> actualPostContent = new HashSet<>();

        runner.assertAllFlowFilesTransferred(org.apache.nifi.processors.standard.PostHTTP.REL_SUCCESS, expectedCount);

        // confirm that all FlowFiles transferred to 'success' have the same URL
        // also accumulate content to verify later
        final List<MockFlowFile> successFlowFiles = runner.getFlowFilesForRelationship(org.apache.nifi.processors.standard.PostHTTP.REL_SUCCESS);
        for (int i = 0; i < expectedCount; i++) {
            MockFlowFile mff = successFlowFiles.get(i);
            mff.assertAttributeEquals("url", url);
            String content = new String(mff.toByteArray());
            actualFFContent.add(content);
        }

        // confirm that all FlowFiles POSTed to server have the same URL
        // also accumulate content to verify later
        try (ByteArrayInputStream bais = new ByteArrayInputStream(servlet.getLastPost());
            ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            for (int i = 0; i < expectedCount; i++) {
                Map<String, String> receivedAttrs = unpacker.unpackageFlowFile(bais, baos);
                final byte[] bytesReceived = baos.toByteArray();
                String receivedContent = new String(bytesReceived, StandardCharsets.UTF_8);
                actualPostContent.add(receivedContent);
                assertEquals(url, receivedAttrs.get("url"));
                assertTrue(unpacker.hasMoreData() || i == (expectedCount - 1));
                baos.reset();
            }
        }

        // confirm that the transferred and POSTed content match
        assertEquals(actualFFContent, actualPostContent);

        // accumulate actual content
        actualContent.addAll(actualPostContent);
        runner.clearTransferState();
    }

    private void enableSslContextService(final SSLContext configuredSslContext) throws InitializationException {
        final SSLContextService sslContextService = Mockito.mock(SSLContextService.class);
        Mockito.when(sslContextService.getIdentifier()).thenReturn(SSL_CONTEXT_IDENTIFIER);
        Mockito.when(sslContextService.createContext()).thenReturn(configuredSslContext);
        runner.addControllerService(SSL_CONTEXT_IDENTIFIER, sslContextService);
        runner.enableControllerService(sslContextService);
        runner.setProperty(org.apache.nifi.processors.standard.PostHTTP.SSL_CONTEXT_SERVICE, SSL_CONTEXT_IDENTIFIER);
    }
}
