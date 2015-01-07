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

import org.apache.nifi.processors.standard.InvokeHTTP;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processors.standard.InvokeHTTP.Config;
import org.apache.nifi.ssl.StandardSSLContextService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestInvokeHTTP {

    private static Map<String, String> sslProperties;
    private static Server server;
    private static String url;

    private TestRunner runner;

    @BeforeClass
    public static void beforeClass() throws Exception {
        // useful for verbose logging output
        // don't commit this with this property enabled, or any 'mvn test' will be really verbose
        //		System.setProperty("org.slf4j.simpleLogger.log.nifi.processors.standard", "debug");

        // create the SSL properties, which basically store keystore / trustore information
        // this is used by the StandardSSLContextService and the Jetty Server
        sslProperties = createSslProperties();

        // create a Jetty server on a random port
        server = createServer();
        server.start();

        // we need the port to construct the base url
        int port = ((ServerConnector) server.getConnectors()[0]).getLocalPort();

        // this is the base url with the random port
        url = "https://localhost:" + port;

    }

    @AfterClass
    public static void afterClass() throws Exception {
        server.stop();
    }

    @Before
    public void before() throws Exception {
        runner = TestRunners.newTestRunner(InvokeHTTP.class);
        runner.addControllerService("ssl-context", new StandardSSLContextService(), sslProperties);
        runner.setProperty(Config.PROP_SSL_CONTEXT_SERVICE, "ssl-context");

        HandlerCollection hc = (HandlerCollection) server.getHandler();
        Handler[] ha = hc.getHandlers();
        if (ha != null) {
            for (Handler h : ha) {
                hc.removeHandler(h);
            }
        }
    }

    @After
    public void after() {
        runner.shutdown();
    }

    private void addHandler(Handler handler) {
        ((HandlerCollection) server.getHandler()).addHandler(handler);
    }

    @Test
    public void testDateGeneration() throws Exception {
        DateHandler dh = new DateHandler();
        addHandler(dh);

        runner.setProperty(Config.PROP_URL, url);
        createFlowFiles(runner);
        runner.run();

        // extract the date string sent to the server
        // and store it as a java.util.Date
        SimpleDateFormat sdf = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z", Locale.US);
        Date date = sdf.parse(dh.dateString);

        // calculate the difference between the date string sent by the client and
        // the current system time -- these should be within a second or two
        // (just enough time to run the test).
        //
        // If the difference is more like in hours, it's likely that a timezone
        // conversion caused a problem.
        long diff = Math.abs(System.currentTimeMillis() - date.getTime());
        long threshold = 15000; // 15 seconds
        if (diff > threshold) {
            fail("Difference (" + diff + ") was greater than threshold (" + threshold + ")");
        }
        System.out.println("diff: " + diff);
    }

    @Test
    public void test200() throws Exception {
        addHandler(new GetOrHeadHandler());

        runner.setProperty(Config.PROP_URL, url + "/status/200");

        createFlowFiles(runner);

        runner.run();

        runner.assertTransferCount(Config.REL_SUCCESS_REQ, 1);
        runner.assertTransferCount(Config.REL_SUCCESS_RESP, 1);
        runner.assertTransferCount(Config.REL_RETRY, 0);
        runner.assertTransferCount(Config.REL_NO_RETRY, 0);
        runner.assertTransferCount(Config.REL_FAILURE, 0);

        //expected in request status.code and status.message
        //original flow file (+attributes)??????????
        final MockFlowFile bundle = runner.getFlowFilesForRelationship(Config.REL_SUCCESS_REQ).get(0);
        bundle.assertAttributeEquals(Config.STATUS_CODE, "200");
        bundle.assertAttributeEquals(Config.STATUS_MESSAGE, "OK");
        final String actual = new String(bundle.toByteArray(), StandardCharsets.UTF_8);
        final String expected = "Hello";
        Assert.assertEquals(expected, actual);
        bundle.assertAttributeEquals("Foo", "Bar");

        //expected in response
        //status code, status message, all headers from server response --> ff attributes
        //server response message body into payload of ff
        //should not contain any original ff attributes
        final MockFlowFile bundle1 = runner.getFlowFilesForRelationship(Config.REL_SUCCESS_RESP).get(0);
        bundle1.assertContentEquals("/status/200".getBytes("UTF-8"));
        bundle1.assertAttributeEquals(Config.STATUS_CODE, "200");
        bundle1.assertAttributeEquals(Config.STATUS_MESSAGE, "OK");
        bundle1.assertAttributeEquals("Foo", "Bar");
        bundle1.assertAttributeEquals("Content-Type", "text/plain; charset=ISO-8859-1");
        final String actual1 = new String(bundle1.toByteArray(), StandardCharsets.UTF_8);
        final String expected1 = "/status/200";
        Assert.assertEquals(expected1, actual1);

    }

    @Test
    public void test500() throws Exception {
        addHandler(new GetOrHeadHandler());

        runner.setProperty(Config.PROP_URL, url + "/status/500");

        createFlowFiles(runner);

        runner.run();
        runner.assertTransferCount(Config.REL_SUCCESS_REQ, 0);
        runner.assertTransferCount(Config.REL_SUCCESS_RESP, 0);
        runner.assertTransferCount(Config.REL_RETRY, 1);
        runner.assertTransferCount(Config.REL_NO_RETRY, 0);
        runner.assertTransferCount(Config.REL_FAILURE, 0);

        //expected in response
        final MockFlowFile bundle = runner.getFlowFilesForRelationship(Config.REL_RETRY).get(0);
        final String actual = new String(bundle.toByteArray(), StandardCharsets.UTF_8);
        bundle.assertAttributeEquals(Config.STATUS_CODE, "500");
        bundle.assertAttributeEquals(Config.STATUS_MESSAGE, "Server Error");
        bundle.assertAttributeEquals(Config.RESPONSE_BODY, "/status/500");

        final String expected = "Hello";
        Assert.assertEquals(expected, actual);
        bundle.assertAttributeEquals("Foo", "Bar");

    }

    @Test
    public void test300() throws Exception {
        addHandler(new GetOrHeadHandler());

        runner.setProperty(Config.PROP_URL, url + "/status/302");

        createFlowFiles(runner);

        runner.run();
        runner.assertTransferCount(Config.REL_SUCCESS_REQ, 0);
        runner.assertTransferCount(Config.REL_SUCCESS_RESP, 0);
        runner.assertTransferCount(Config.REL_RETRY, 0);
        runner.assertTransferCount(Config.REL_NO_RETRY, 1);
        runner.assertTransferCount(Config.REL_FAILURE, 0);
        //getMyFlowFiles();
        //expected in response
        final MockFlowFile bundle = runner.getFlowFilesForRelationship(Config.REL_NO_RETRY).get(0);
        final String actual = new String(bundle.toByteArray(), StandardCharsets.UTF_8);

        bundle.assertAttributeEquals(Config.STATUS_CODE, "302");
        bundle.assertAttributeEquals(Config.STATUS_MESSAGE, "Found");
        final String expected = "Hello";
        Assert.assertEquals(expected, actual);
        bundle.assertAttributeEquals("Foo", "Bar");

    }

    @Test
    public void test304() throws Exception {
        addHandler(new GetOrHeadHandler());

        runner.setProperty(Config.PROP_URL, url + "/status/304");

        createFlowFiles(runner);

        runner.run();
        runner.assertTransferCount(Config.REL_SUCCESS_REQ, 0);
        runner.assertTransferCount(Config.REL_SUCCESS_RESP, 0);
        runner.assertTransferCount(Config.REL_RETRY, 0);
        runner.assertTransferCount(Config.REL_NO_RETRY, 1);
        runner.assertTransferCount(Config.REL_FAILURE, 0);
        //getMyFlowFiles();
        //expected in response
        final MockFlowFile bundle = runner.getFlowFilesForRelationship(Config.REL_NO_RETRY).get(0);
        final String actual = new String(bundle.toByteArray(), StandardCharsets.UTF_8);

        bundle.assertAttributeEquals(Config.STATUS_CODE, "304");
        bundle.assertAttributeEquals(Config.STATUS_MESSAGE, "Not Modified");
        final String expected = "Hello";
        Assert.assertEquals(expected, actual);
        bundle.assertAttributeEquals("Foo", "Bar");

    }

    @Test
    public void test400() throws Exception {
        addHandler(new GetOrHeadHandler());

        runner.setProperty(Config.PROP_URL, url + "/status/400");

        createFlowFiles(runner);

        runner.run();
        runner.assertTransferCount(Config.REL_SUCCESS_REQ, 0);
        runner.assertTransferCount(Config.REL_SUCCESS_RESP, 0);
        runner.assertTransferCount(Config.REL_RETRY, 0);
        runner.assertTransferCount(Config.REL_NO_RETRY, 1);
        runner.assertTransferCount(Config.REL_FAILURE, 0);
        //getMyFlowFiles();
        //expected in response
        final MockFlowFile bundle = runner.getFlowFilesForRelationship(Config.REL_NO_RETRY).get(0);
        final String actual = new String(bundle.toByteArray(), StandardCharsets.UTF_8);

        bundle.assertAttributeEquals(Config.STATUS_CODE, "400");
        bundle.assertAttributeEquals(Config.STATUS_MESSAGE, "Bad Request");
        bundle.assertAttributeEquals(Config.RESPONSE_BODY, "/status/400");
        final String expected = "Hello";
        Assert.assertEquals(expected, actual);
        bundle.assertAttributeEquals("Foo", "Bar");

    }

    @Test
    public void test412() throws Exception {
        addHandler(new GetOrHeadHandler());

        runner.setProperty(Config.PROP_URL, url + "/status/412");
        runner.setProperty(Config.PROP_METHOD, "GET");

        createFlowFiles(runner);

        runner.run();
        runner.assertTransferCount(Config.REL_SUCCESS_REQ, 0);
        runner.assertTransferCount(Config.REL_SUCCESS_RESP, 0);
        runner.assertTransferCount(Config.REL_RETRY, 0);
        runner.assertTransferCount(Config.REL_NO_RETRY, 1);
        runner.assertTransferCount(Config.REL_FAILURE, 0);

        //expected in response
        final MockFlowFile bundle = runner.getFlowFilesForRelationship(Config.REL_NO_RETRY).get(0);
        final String actual = new String(bundle.toByteArray(), StandardCharsets.UTF_8);

        bundle.assertAttributeEquals(Config.STATUS_CODE, "412");
        bundle.assertAttributeEquals(Config.STATUS_MESSAGE, "Precondition Failed");
        bundle.assertAttributeEquals(Config.RESPONSE_BODY, "/status/412");
        final String expected = "Hello";
        Assert.assertEquals(expected, actual);
        bundle.assertAttributeEquals("Foo", "Bar");

    }

    @Test
    public void testHead() throws Exception {
        addHandler(new GetOrHeadHandler());

        runner.setProperty(Config.PROP_METHOD, "HEAD");
        runner.setProperty(Config.PROP_URL, url + "/status/200");

        createFlowFiles(runner);

        runner.run();
        runner.assertTransferCount(Config.REL_SUCCESS_REQ, 1);
        runner.assertTransferCount(Config.REL_SUCCESS_RESP, 1);
        runner.assertTransferCount(Config.REL_RETRY, 0);
        runner.assertTransferCount(Config.REL_NO_RETRY, 0);
        runner.assertTransferCount(Config.REL_FAILURE, 0);

        final MockFlowFile bundle = runner.getFlowFilesForRelationship(Config.REL_SUCCESS_REQ).get(0);
        bundle.assertAttributeEquals(Config.STATUS_CODE, "200");
        bundle.assertAttributeEquals(Config.STATUS_MESSAGE, "OK");
        final String actual = new String(bundle.toByteArray(), StandardCharsets.UTF_8);
        final String expected = "Hello";
        Assert.assertEquals(expected, actual);
        bundle.assertAttributeEquals("Foo", "Bar");

        final MockFlowFile bundle1 = runner.getFlowFilesForRelationship(Config.REL_SUCCESS_RESP).get(0);
        bundle1.assertContentEquals("".getBytes("UTF-8"));
        bundle1.assertAttributeEquals(Config.STATUS_CODE, "200");
        bundle1.assertAttributeEquals(Config.STATUS_MESSAGE, "OK");
        bundle1.assertAttributeEquals("Foo", "Bar");
        bundle1.assertAttributeEquals("Content-Type", "text/plain");
        final String actual1 = new String(bundle1.toByteArray(), StandardCharsets.UTF_8);
        final String expected1 = "";
        Assert.assertEquals(expected1, actual1);
    }

    @Test
    public void testPost() throws Exception {
        addHandler(new PostHandler());

        runner.setProperty(Config.PROP_METHOD, "POST");
        runner.setProperty(Config.PROP_URL, url + "/post");

        createFlowFiles(runner);

        runner.run();
        runner.assertTransferCount(Config.REL_SUCCESS_REQ, 1);
        runner.assertTransferCount(Config.REL_SUCCESS_RESP, 1);
        runner.assertTransferCount(Config.REL_RETRY, 0);
        runner.assertTransferCount(Config.REL_NO_RETRY, 0);
        runner.assertTransferCount(Config.REL_FAILURE, 0);

        final MockFlowFile bundle = runner.getFlowFilesForRelationship(Config.REL_SUCCESS_REQ).get(0);
        bundle.assertAttributeEquals(Config.STATUS_CODE, "200");
        bundle.assertAttributeEquals(Config.STATUS_MESSAGE, "OK");
        final String actual = new String(bundle.toByteArray(), StandardCharsets.UTF_8);
        final String expected = "Hello";
        Assert.assertEquals(expected, actual);
        bundle.assertAttributeEquals("Foo", "Bar");

        final MockFlowFile bundle1 = runner.getFlowFilesForRelationship(Config.REL_SUCCESS_RESP).get(0);
        bundle1.assertContentEquals("".getBytes("UTF-8"));
        bundle1.assertAttributeEquals(Config.STATUS_CODE, "200");
        bundle1.assertAttributeEquals(Config.STATUS_MESSAGE, "OK");
        bundle1.assertAttributeEquals("Foo", "Bar");
        bundle1.assertAttributeNotExists("Content-Type");

        final String actual1 = new String(bundle1.toByteArray(), StandardCharsets.UTF_8);
        final String expected1 = "";
        Assert.assertEquals(expected1, actual1);
    }

    @Test
    public void testPut() throws Exception {
        addHandler(new PostHandler());

        runner.setProperty(Config.PROP_METHOD, "PUT");
        runner.setProperty(Config.PROP_URL, url + "/post");

        createFlowFiles(runner);

        runner.run();
        runner.assertTransferCount(Config.REL_SUCCESS_REQ, 1);
        runner.assertTransferCount(Config.REL_SUCCESS_RESP, 1);
        runner.assertTransferCount(Config.REL_RETRY, 0);
        runner.assertTransferCount(Config.REL_NO_RETRY, 0);
        runner.assertTransferCount(Config.REL_FAILURE, 0);

        final MockFlowFile bundle = runner.getFlowFilesForRelationship(Config.REL_SUCCESS_REQ).get(0);
        bundle.assertAttributeEquals(Config.STATUS_CODE, "200");
        bundle.assertAttributeEquals(Config.STATUS_MESSAGE, "OK");
        final String actual = new String(bundle.toByteArray(), StandardCharsets.UTF_8);
        final String expected = "Hello";
        Assert.assertEquals(expected, actual);
        bundle.assertAttributeEquals("Foo", "Bar");

        final MockFlowFile bundle1 = runner.getFlowFilesForRelationship(Config.REL_SUCCESS_RESP).get(0);
        bundle1.assertContentEquals("".getBytes("UTF-8"));
        bundle1.assertAttributeEquals(Config.STATUS_CODE, "200");
        bundle1.assertAttributeEquals(Config.STATUS_MESSAGE, "OK");
        bundle1.assertAttributeEquals("Foo", "Bar");
        bundle1.assertAttributeNotExists("Content-Type");

        final String actual1 = new String(bundle1.toByteArray(), StandardCharsets.UTF_8);
        final String expected1 = "";
        Assert.assertEquals(expected1, actual1);
    }

    @Test
    public void testConnectFailBadPort() throws Exception {
        addHandler(new GetOrHeadHandler());

        // this is the bad urls
        String badurlport = "https://localhost:" + 445;

        runner.setProperty(Config.PROP_URL, badurlport + "/doesnotExist");
        createFlowFiles(runner);

        runner.run();
        runner.assertTransferCount(Config.REL_SUCCESS_REQ, 0);
        runner.assertTransferCount(Config.REL_SUCCESS_RESP, 0);
        runner.assertTransferCount(Config.REL_RETRY, 0);
        runner.assertTransferCount(Config.REL_NO_RETRY, 0);
        runner.assertTransferCount(Config.REL_FAILURE, 1);

        final MockFlowFile bundle = runner.getFlowFilesForRelationship(Config.REL_FAILURE).get(0);

        final String actual = new String(bundle.toByteArray(), StandardCharsets.UTF_8);
        final String expected = "Hello";
        Assert.assertEquals(expected, actual);
        bundle.assertAttributeEquals("Foo", "Bar");
    }

    //  @Test
    //  public void testGetFlowfileAttributes() throws IOException {
    //      Map<String, List<String>> input = new HashMap<>();
    //      input.put("A", Arrays.asList("1"));
    //      input.put("B", Arrays.asList("1", "2", "3"));
    //      input.put("C", new ArrayList<String>());
    //      input.put("D", null);
    //
    //      Map<String, String> expected = new HashMap<>();
    //      expected.put(Config.STATUS_CODE, "200");
    //      expected.put(Config.STATUS_MESSAGE, "OK");
    //      expected.put(Config.STATUS_LINE, "HTTP/1.1 200 OK");
    //      expected.put("A", "1");
    //      expected.put("B", "1, 2, 3");
    //
    //      URL url = new URL("file:/dev/null");
    //      HttpURLConnection conn = new MockHttpURLConnection(url, 200, "OK", input);
    //
    //      Map<String, String> actual = processor.getAttributesFromHeaders(conn);
    //
    //      assertEquals(expected, actual);
    //  }
    //  @Test
    //  public void testCsv() {
    //      // null input should return an empty string
    //      assertEquals("", processor.csv(null));
    //
    //      // empty collection returns empty string
    //      assertEquals("", processor.csv(new ArrayList<String>()));
    //
    //      // pretty normal checks
    //      assertEquals("1", processor.csv(Arrays.asList("1")));
    //      assertEquals("1, 2", processor.csv(Arrays.asList("1", "2")));
    //      assertEquals("1, 2, 3", processor.csv(Arrays.asList("1", "2", "3")));
    //
    //      // values should be trimmed
    //      assertEquals("1, 2, 3", processor.csv(Arrays.asList("    1", "    2       ", "3       ")));
    //
    //      // empty values should be skipped
    //      assertEquals("1, 3", processor.csv(Arrays.asList("1", "", "3")));
    //
    //      // whitespace values should be skipped
    //      assertEquals("1, 3", processor.csv(Arrays.asList("1", "      ", "3")));
    //
    //      // this (mis)behavior is currently expected, embedded comma delimiters are not escaped
    //      // note the embedded unescaped comma in the "1, " value
    //      assertEquals("1,, 2, 3", processor.csv(Arrays.asList("1, ", "2", "3")));
    //  }
    @Test
    public void testConnectFailBadHost() throws Exception {
        addHandler(new GetOrHeadHandler());

        String badurlhost = "https://localhOOst:" + 445;

        runner.setProperty(Config.PROP_URL, badurlhost + "/doesnotExist");
        createFlowFiles(runner);

        runner.run();
        runner.assertTransferCount(Config.REL_SUCCESS_REQ, 0);
        runner.assertTransferCount(Config.REL_SUCCESS_RESP, 0);
        runner.assertTransferCount(Config.REL_RETRY, 0);
        runner.assertTransferCount(Config.REL_NO_RETRY, 0);
        runner.assertTransferCount(Config.REL_FAILURE, 1);

        final MockFlowFile bundle = runner.getFlowFilesForRelationship(Config.REL_FAILURE).get(0);

        final String actual = new String(bundle.toByteArray(), StandardCharsets.UTF_8);
        final String expected = "Hello";
        Assert.assertEquals(expected, actual);
        bundle.assertAttributeEquals("Foo", "Bar");
    }

    private static Map<String, String> createSslProperties() {
        Map<String, String> map = new HashMap<String, String>();
        map.put(StandardSSLContextService.KEYSTORE.getName(), "src/test/resources/localhost-ks.jks");
        map.put(StandardSSLContextService.KEYSTORE_PASSWORD.getName(), "localtest");
        map.put(StandardSSLContextService.KEYSTORE_TYPE.getName(), "JKS");
        map.put(StandardSSLContextService.TRUSTSTORE.getName(), "src/test/resources/localhost-ts.jks");
        map.put(StandardSSLContextService.TRUSTSTORE_PASSWORD.getName(), "localtest");
        map.put(StandardSSLContextService.TRUSTSTORE_TYPE.getName(), "JKS");
        return map;
    }

    private static Server createServer() throws IOException {
        SslContextFactory ssl = new SslContextFactory();
        ssl.setKeyStorePath(sslProperties.get(StandardSSLContextService.KEYSTORE.getName()));
        ssl.setKeyStorePassword(sslProperties.get(StandardSSLContextService.KEYSTORE_PASSWORD.getName()));
        ssl.setKeyStoreType(sslProperties.get(StandardSSLContextService.KEYSTORE_TYPE.getName()));
        ssl.setTrustStorePath(sslProperties.get(StandardSSLContextService.TRUSTSTORE.getName()));
        ssl.setTrustStorePassword(sslProperties.get(StandardSSLContextService.TRUSTSTORE_PASSWORD.getName()));
        ssl.setTrustStoreType(sslProperties.get(StandardSSLContextService.TRUSTSTORE_TYPE.getName()));

        ssl.setNeedClientAuth(true);

        Server server = new Server();

        server.addConnector(new ServerConnector(server, ssl));
        server.setHandler(new HandlerCollection(true));

        return server;
    }

    private static void createFlowFiles(final TestRunner testRunner) throws UnsupportedEncodingException {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.MIME_TYPE.key(), "application/plain-text");
        attributes.put("Foo", "Bar");
        testRunner.enqueue("Hello".getBytes("UTF-8"), attributes);

    }

    private static class PostHandler extends AbstractHandler {

        @Override
        public void handle(String target, Request baseRequest,
                HttpServletRequest request, HttpServletResponse response)
                throws IOException, ServletException {

            baseRequest.setHandled(true);

            assertEquals("/post", target);

            String body = request.getReader().readLine();
            assertEquals("Hello", body);

        }
    }

    private static class GetOrHeadHandler extends AbstractHandler {

        @Override
        public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
            baseRequest.setHandled(true);

            int status = Integer.valueOf(target.substring("/status".length() + 1));
            response.setStatus(status);

            response.setContentType("text/plain");
            response.setContentLength(target.length());

            if ("GET".equalsIgnoreCase(request.getMethod())) {
                try (PrintWriter writer = response.getWriter()) {
                    writer.print(target);
                    writer.flush();
                }
            }

        }

    }

    private static class DateHandler extends AbstractHandler {

        private String dateString;

        @Override
        public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
            baseRequest.setHandled(true);

            dateString = request.getHeader("Date");

            response.setStatus(200);
            response.setContentType("text/plain");
            response.getWriter().println("Way to go!");
        }
    }

}
