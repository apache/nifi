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
package org.apache.nifi.remote.client.http;

import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.nifi.remote.protocol.http.HttpHeaders.LOCATION_HEADER_NAME;
import static org.apache.nifi.remote.protocol.http.HttpHeaders.LOCATION_URI_INTENT_NAME;
import static org.apache.nifi.remote.protocol.http.HttpHeaders.LOCATION_URI_INTENT_VALUE;
import static org.apache.nifi.remote.protocol.http.HttpHeaders.PROTOCOL_VERSION;
import static org.apache.nifi.remote.protocol.http.HttpHeaders.SERVER_SIDE_TRANSACTION_TTL;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.remote.Peer;
import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.client.KeystoreType;
import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.remote.codec.StandardFlowFileCodec;
import org.apache.nifi.remote.exception.HandshakeException;
import org.apache.nifi.remote.io.CompressionInputStream;
import org.apache.nifi.remote.io.CompressionOutputStream;
import org.apache.nifi.remote.protocol.DataPacket;
import org.apache.nifi.remote.protocol.ResponseCode;
import org.apache.nifi.remote.protocol.SiteToSiteTransportProtocol;
import org.apache.nifi.remote.protocol.http.HttpHeaders;
import org.apache.nifi.remote.protocol.http.HttpProxy;
import org.apache.nifi.remote.util.StandardDataPacket;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.web.api.dto.ControllerDTO;
import org.apache.nifi.web.api.dto.PortDTO;
import org.apache.nifi.web.api.dto.remote.PeerDTO;
import org.apache.nifi.web.api.entity.ControllerEntity;
import org.apache.nifi.web.api.entity.PeersEntity;
import org.apache.nifi.web.api.entity.TransactionResultEntity;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.littleshoot.proxy.HttpProxyServer;
import org.littleshoot.proxy.ProxyAuthenticator;
import org.littleshoot.proxy.impl.DefaultHttpProxyServer;
import org.littleshoot.proxy.impl.ThreadPoolConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestHttpClient {

    private static Logger logger = LoggerFactory.getLogger(TestHttpClient.class);

    private static Server server;
    private static ServerConnector httpConnector;
    private static ServerConnector sslConnector;
    private static CountDownLatch testCaseFinished;

    private static HttpProxyServer proxyServer;
    private static HttpProxyServer proxyServerWithAuth;

    private static Set<PortDTO> inputPorts;
    private static Set<PortDTO> outputPorts;
    private static Set<PeerDTO> peers;
    private static Set<PeerDTO> peersSecure;
    private static String serverChecksum;

    public static class SiteInfoServlet extends HttpServlet {

        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

            final ControllerDTO controller = new ControllerDTO();

            if (req.getLocalPort() == httpConnector.getLocalPort()) {
                controller.setRemoteSiteHttpListeningPort(httpConnector.getLocalPort());
                controller.setSiteToSiteSecure(false);
            } else {
                controller.setRemoteSiteHttpListeningPort(sslConnector.getLocalPort());
                controller.setSiteToSiteSecure(true);
            }

            controller.setId("remote-controller-id");
            controller.setInstanceId("remote-instance-id");
            controller.setName("Remote NiFi Flow");

            assertNotNull("Test case should set <inputPorts> depending on the test scenario.", inputPorts);
            controller.setInputPorts(inputPorts);
            controller.setInputPortCount(inputPorts.size());

            assertNotNull("Test case should set <outputPorts> depending on the test scenario.", outputPorts);
            controller.setOutputPorts(outputPorts);
            controller.setOutputPortCount(outputPorts.size());

            final ControllerEntity controllerEntity = new ControllerEntity();
            controllerEntity.setController(controller);

            respondWithJson(resp, controllerEntity);
        }
    }

    public static class WrongSiteInfoServlet extends HttpServlet {

        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
            // This response simulates when a Site-to-Site is given an URL which has wrong path.
            respondWithText(resp, "<p class=\"message-pane-content\">You may have mistyped...</p>", 200);
        }
    }

    public static class PeersServlet extends HttpServlet {

        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

            final PeersEntity peersEntity = new PeersEntity();

            if (req.getLocalPort() == httpConnector.getLocalPort()) {
                assertNotNull("Test case should set <peers> depending on the test scenario.", peers);
                peersEntity.setPeers(peers);
            } else {
                assertNotNull("Test case should set <peersSecure> depending on the test scenario.", peersSecure);
                peersEntity.setPeers(peersSecure);
            }

            respondWithJson(resp, peersEntity);
        }
    }

    public static class PortTransactionsServlet extends HttpServlet {

        @Override
        protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

            final int reqProtocolVersion = getReqProtocolVersion(req);

            TransactionResultEntity entity = new TransactionResultEntity();
            entity.setResponseCode(ResponseCode.PROPERTIES_OK.getCode());
            entity.setMessage("A transaction is created.");

            resp.setHeader(LOCATION_URI_INTENT_NAME, LOCATION_URI_INTENT_VALUE);
            resp.setHeader(LOCATION_HEADER_NAME, req.getRequestURL() + "/transaction-id");
            setCommonResponseHeaders(resp, reqProtocolVersion);

            respondWithJson(resp, entity, HttpServletResponse.SC_CREATED);
        }

    }

    public static class PortTransactionsAccessDeniedServlet extends HttpServlet {

        @Override
        protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

            respondWithText(resp, "Unable to perform the desired action" +
                    " due to insufficient permissions. Contact the system administrator.", 403);

        }

    }

    public static class InputPortTransactionServlet extends HttpServlet {

        @Override
        protected void doPut(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
            final int reqProtocolVersion = getReqProtocolVersion(req);

            final TransactionResultEntity entity = new TransactionResultEntity();
            entity.setResponseCode(ResponseCode.CONTINUE_TRANSACTION.getCode());
            entity.setMessage("Extended TTL.");

            setCommonResponseHeaders(resp, reqProtocolVersion);

            respondWithJson(resp, entity, HttpServletResponse.SC_OK);
        }

        @Override
        protected void doDelete(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

            final int reqProtocolVersion = getReqProtocolVersion(req);

            TransactionResultEntity entity = new TransactionResultEntity();
            entity.setResponseCode(ResponseCode.TRANSACTION_FINISHED.getCode());
            entity.setMessage("The transaction is finished.");

            setCommonResponseHeaders(resp, reqProtocolVersion);

            respondWithJson(resp, entity, HttpServletResponse.SC_OK);
        }

    }

    public static class OutputPortTransactionServlet extends HttpServlet {

        @Override
        protected void doPut(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
            final int reqProtocolVersion = getReqProtocolVersion(req);

            final TransactionResultEntity entity = new TransactionResultEntity();
            entity.setResponseCode(ResponseCode.CONTINUE_TRANSACTION.getCode());
            entity.setMessage("Extended TTL.");

            setCommonResponseHeaders(resp, reqProtocolVersion);

            respondWithJson(resp, entity, HttpServletResponse.SC_OK);
        }

        @Override
        protected void doDelete(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

            final int reqProtocolVersion = getReqProtocolVersion(req);

            TransactionResultEntity entity = new TransactionResultEntity();
            entity.setResponseCode(ResponseCode.CONFIRM_TRANSACTION.getCode());
            entity.setMessage("The transaction is confirmed.");

            setCommonResponseHeaders(resp, reqProtocolVersion);

            respondWithJson(resp, entity, HttpServletResponse.SC_OK);
        }

    }

    public static class FlowFilesServlet extends HttpServlet {

        @Override
        protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

            final int reqProtocolVersion = getReqProtocolVersion(req);

            setCommonResponseHeaders(resp, reqProtocolVersion);

            DataPacket dataPacket;
            while ((dataPacket = readIncomingPacket(req)) != null) {
                logger.info("received {}", dataPacket);
                consumeDataPacket(dataPacket);
            }
            logger.info("finish receiving data packets.");

            assertNotNull("Test case should set <serverChecksum> depending on the test scenario.", serverChecksum);
            respondWithText(resp, serverChecksum, HttpServletResponse.SC_ACCEPTED);
        }

        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

            final int reqProtocolVersion = getReqProtocolVersion(req);

            resp.setStatus(HttpServletResponse.SC_ACCEPTED);
            resp.setContentType("application/octet-stream");
            setCommonResponseHeaders(resp, reqProtocolVersion);

            final OutputStream outputStream = getOutputStream(req, resp);
            writeOutgoingPacket(outputStream);
            writeOutgoingPacket(outputStream);
            writeOutgoingPacket(outputStream);
            resp.flushBuffer();
        }
    }

    public static class FlowFilesTimeoutServlet extends FlowFilesServlet {

        @Override
        protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

            sleepUntilTestCaseFinish();

            super.doPost(req, resp);
        }

        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

            sleepUntilTestCaseFinish();

            super.doGet(req, resp);
        }

    }

    public static class FlowFilesTimeoutAfterDataExchangeServlet extends HttpServlet {

        @Override
        protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

            final int reqProtocolVersion = getReqProtocolVersion(req);

            setCommonResponseHeaders(resp, reqProtocolVersion);

            consumeDataPacket(readIncomingPacket(req));

            sleepUntilTestCaseFinish();

        }

        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

            final int reqProtocolVersion = getReqProtocolVersion(req);

            resp.setStatus(HttpServletResponse.SC_ACCEPTED);
            resp.setContentType("application/octet-stream");
            setCommonResponseHeaders(resp, reqProtocolVersion);

            writeOutgoingPacket(getOutputStream(req, resp));

            sleepUntilTestCaseFinish();
        }
    }

    private static void sleepUntilTestCaseFinish() {
        try {
            if (!testCaseFinished.await(3, TimeUnit.MINUTES)) {
                fail("Test case timeout.");
            }
        } catch (InterruptedException e) {
        }
    }

    private static void writeOutgoingPacket(OutputStream outputStream) throws IOException {
        final DataPacket packet = new DataPacketBuilder()
                    .contents("Example contents from server.")
                    .attr("Server attr 1", "Server attr 1 value")
                    .attr("Server attr 2", "Server attr 2 value")
                    .build();
        new StandardFlowFileCodec().encode(packet, outputStream);
        outputStream.flush();
    }

    private static OutputStream getOutputStream(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        OutputStream outputStream = resp.getOutputStream();
        if (Boolean.valueOf(req.getHeader(HttpHeaders.HANDSHAKE_PROPERTY_USE_COMPRESSION))){
            outputStream = new CompressionOutputStream(outputStream);
        }
        return outputStream;
    }

    private static DataPacket readIncomingPacket(HttpServletRequest req) throws IOException {
        final StandardFlowFileCodec codec = new StandardFlowFileCodec();
        InputStream inputStream = req.getInputStream();
        if (Boolean.valueOf(req.getHeader(HttpHeaders.HANDSHAKE_PROPERTY_USE_COMPRESSION))){
            inputStream = new CompressionInputStream(inputStream);
        }

        return codec.decode(inputStream);
    }

    private static int getReqProtocolVersion(HttpServletRequest req) {
        final String reqProtocolVersionStr = req.getHeader(PROTOCOL_VERSION);
        assertTrue(!isEmpty(reqProtocolVersionStr));
        return Integer.parseInt(reqProtocolVersionStr);
    }

    private static void setCommonResponseHeaders(HttpServletResponse resp, int reqProtocolVersion) {
        resp.setHeader(PROTOCOL_VERSION, String.valueOf(reqProtocolVersion));
        resp.setHeader(SERVER_SIDE_TRANSACTION_TTL, "3");
    }

    private static void respondWithJson(HttpServletResponse resp, Object entity) throws IOException {
        respondWithJson(resp, entity, HttpServletResponse.SC_OK);
    }

    private static void respondWithJson(HttpServletResponse resp, Object entity, int statusCode) throws IOException {
        resp.setContentType("application/json");
        resp.setStatus(statusCode);
        final ServletOutputStream out = resp.getOutputStream();
        new ObjectMapper().writer().writeValue(out, entity);
        out.flush();
    }

    private static void respondWithText(HttpServletResponse resp, String result, int statusCode) throws IOException {
        resp.setContentType("text/plain");
        resp.setStatus(statusCode);
        final ServletOutputStream out = resp.getOutputStream();
        out.write(result.getBytes());
        out.flush();
    }

    @BeforeClass
    public static void setup() throws Exception {
        // Create embedded Jetty server
        // Use less threads to mitigate Gateway Timeout (504) with proxy test
        // Minimum thread pool size = (acceptors=2 + selectors=8 + request=1), defaults to max=200
        final QueuedThreadPool threadPool = new QueuedThreadPool(50);
        server = new Server(threadPool);

        final ContextHandlerCollection handlerCollection = new ContextHandlerCollection();

        final ServletContextHandler contextHandler = new ServletContextHandler();
        contextHandler.setContextPath("/nifi-api");

        final ServletContextHandler wrongPathContextHandler = new ServletContextHandler();
        wrongPathContextHandler.setContextPath("/wrong/nifi-api");

        handlerCollection.setHandlers(new Handler[]{contextHandler, wrongPathContextHandler});

        server.setHandler(handlerCollection);

        final ServletHandler servletHandler = new ServletHandler();
        contextHandler.insertHandler(servletHandler);

        final ServletHandler wrongPathServletHandler = new ServletHandler();
        wrongPathContextHandler.insertHandler(wrongPathServletHandler);

        final SslContextFactory sslContextFactory = new SslContextFactory();
        sslContextFactory.setKeyStorePath("src/test/resources/certs/keystore.jks");
        sslContextFactory.setKeyStorePassword("passwordpassword");
        sslContextFactory.setKeyStoreType("JKS");

        httpConnector = new ServerConnector(server);

        final HttpConfiguration https = new HttpConfiguration();
        https.addCustomizer(new SecureRequestCustomizer());
        sslConnector = new ServerConnector(server,
                new SslConnectionFactory(sslContextFactory, "http/1.1"),
                new HttpConnectionFactory(https));

        server.setConnectors(new Connector[] { httpConnector, sslConnector });

        wrongPathServletHandler.addServletWithMapping(WrongSiteInfoServlet.class, "/site-to-site");

        servletHandler.addServletWithMapping(SiteInfoServlet.class, "/site-to-site");
        servletHandler.addServletWithMapping(PeersServlet.class, "/site-to-site/peers");

        servletHandler.addServletWithMapping(PortTransactionsAccessDeniedServlet.class, "/data-transfer/input-ports/input-access-denied-id/transactions");
        servletHandler.addServletWithMapping(PortTransactionsServlet.class, "/data-transfer/input-ports/input-running-id/transactions");
        servletHandler.addServletWithMapping(InputPortTransactionServlet.class, "/data-transfer/input-ports/input-running-id/transactions/transaction-id");
        servletHandler.addServletWithMapping(FlowFilesServlet.class, "/data-transfer/input-ports/input-running-id/transactions/transaction-id/flow-files");

        servletHandler.addServletWithMapping(PortTransactionsServlet.class, "/data-transfer/input-ports/input-timeout-id/transactions");
        servletHandler.addServletWithMapping(InputPortTransactionServlet.class, "/data-transfer/input-ports/input-timeout-id/transactions/transaction-id");
        servletHandler.addServletWithMapping(FlowFilesTimeoutServlet.class, "/data-transfer/input-ports/input-timeout-id/transactions/transaction-id/flow-files");

        servletHandler.addServletWithMapping(PortTransactionsServlet.class, "/data-transfer/input-ports/input-timeout-data-ex-id/transactions");
        servletHandler.addServletWithMapping(InputPortTransactionServlet.class, "/data-transfer/input-ports/input-timeout-data-ex-id/transactions/transaction-id");
        servletHandler.addServletWithMapping(FlowFilesTimeoutAfterDataExchangeServlet.class, "/data-transfer/input-ports/input-timeout-data-ex-id/transactions/transaction-id/flow-files");

        servletHandler.addServletWithMapping(PortTransactionsServlet.class, "/data-transfer/output-ports/output-running-id/transactions");
        servletHandler.addServletWithMapping(OutputPortTransactionServlet.class, "/data-transfer/output-ports/output-running-id/transactions/transaction-id");
        servletHandler.addServletWithMapping(FlowFilesServlet.class, "/data-transfer/output-ports/output-running-id/transactions/transaction-id/flow-files");

        servletHandler.addServletWithMapping(PortTransactionsServlet.class, "/data-transfer/output-ports/output-timeout-id/transactions");
        servletHandler.addServletWithMapping(OutputPortTransactionServlet.class, "/data-transfer/output-ports/output-timeout-id/transactions/transaction-id");
        servletHandler.addServletWithMapping(FlowFilesTimeoutServlet.class, "/data-transfer/output-ports/output-timeout-id/transactions/transaction-id/flow-files");

        servletHandler.addServletWithMapping(PortTransactionsServlet.class, "/data-transfer/output-ports/output-timeout-data-ex-id/transactions");
        servletHandler.addServletWithMapping(OutputPortTransactionServlet.class, "/data-transfer/output-ports/output-timeout-data-ex-id/transactions/transaction-id");
        servletHandler.addServletWithMapping(FlowFilesTimeoutAfterDataExchangeServlet.class, "/data-transfer/output-ports/output-timeout-data-ex-id/transactions/transaction-id/flow-files");

        server.start();

        logger.info("Starting server on port {} for HTTP, and {} for HTTPS", httpConnector.getLocalPort(), sslConnector.getLocalPort());

        startProxyServer();
        startProxyServerWithAuth();
    }

    private static void startProxyServer() throws IOException {
        int proxyServerPort;
        try (final ServerSocket serverSocket = new ServerSocket(0)) {
            proxyServerPort = serverSocket.getLocalPort();
        }
        proxyServer = DefaultHttpProxyServer.bootstrap()
                .withPort(proxyServerPort)
                .withAllowLocalOnly(true)
                // Use less threads to mitigate Gateway Timeout (504) with proxy test
                .withThreadPoolConfiguration(new ThreadPoolConfiguration()
                    .withAcceptorThreads(2)
                    .withClientToProxyWorkerThreads(4)
                    .withProxyToServerWorkerThreads(4))
                .start();
    }

    private static final String PROXY_USER = "proxy user";
    private static final String PROXY_PASSWORD = "proxy password";
    private static void startProxyServerWithAuth() throws IOException {
        int proxyServerPort;
        try (final ServerSocket serverSocket = new ServerSocket(0)) {
            proxyServerPort = serverSocket.getLocalPort();
        }
        proxyServerWithAuth = DefaultHttpProxyServer.bootstrap()
                .withPort(proxyServerPort)
                .withAllowLocalOnly(true)
                .withProxyAuthenticator(new ProxyAuthenticator() {
                    @Override
                    public boolean authenticate(String userName, String password) {
                        return PROXY_USER.equals(userName) && PROXY_PASSWORD.equals(password);
                    }

                    @Override
                    public String getRealm() {
                        return "NiFi Unit Test";
                    }
                })
                // Use less threads to mitigate Gateway Timeout (504) with proxy test
                .withThreadPoolConfiguration(new ThreadPoolConfiguration()
                        .withAcceptorThreads(2)
                        .withClientToProxyWorkerThreads(4)
                        .withProxyToServerWorkerThreads(4))
                .start();
    }

    @AfterClass
    public static void teardown() throws Exception {
        logger.info("Stopping servers.");
        try {
            server.stop();
        } catch (Exception e) {
            logger.error("Failed to stop Jetty server due to " + e, e);
        }
        try {
            proxyServer.stop();
        } catch (Exception e) {
            logger.error("Failed to stop Proxy server due to " + e, e);
        }
        try {
            proxyServerWithAuth.stop();
        } catch (Exception e) {
            logger.error("Failed to stop Proxy server with auth due to " + e, e);
        }
    }

    private static class DataPacketBuilder {
        private final Map<String, String> attributes = new HashMap<>();
        private String contents;

        private DataPacketBuilder attr(final String k, final String v) {
            attributes.put(k, v);
            return this;
        }

        private DataPacketBuilder contents(final String contents) {
            this.contents = contents;
            return this;
        }

        private DataPacket build() {
            byte[] bytes = contents.getBytes();
            return new StandardDataPacket(attributes, new ByteArrayInputStream(bytes), bytes.length);
        }

    }

    @Before
    public void before() throws Exception {

        System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi.remote", "TRACE");
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi.remote.protocol.http.HttpClientTransaction", "DEBUG");

        testCaseFinished = new CountDownLatch(1);

        final PeerDTO peer = new PeerDTO();
        peer.setHostname("localhost");
        peer.setPort(httpConnector.getLocalPort());
        peer.setFlowFileCount(10);
        peer.setSecure(false);

        peers = new HashSet<>();
        peers.add(peer);

        final PeerDTO peerSecure = new PeerDTO();
        peerSecure.setHostname("localhost");
        peerSecure.setPort(sslConnector.getLocalPort());
        peerSecure.setFlowFileCount(10);
        peerSecure.setSecure(true);

        peersSecure = new HashSet<>();
        peersSecure.add(peerSecure);

        inputPorts = new HashSet<>();

        final PortDTO runningInputPort = new PortDTO();
        runningInputPort.setName("input-running");
        runningInputPort.setId("input-running-id");
        runningInputPort.setType("INPUT_PORT");
        runningInputPort.setState(ScheduledState.RUNNING.name());
        inputPorts.add(runningInputPort);

        final PortDTO timeoutInputPort = new PortDTO();
        timeoutInputPort.setName("input-timeout");
        timeoutInputPort.setId("input-timeout-id");
        timeoutInputPort.setType("INPUT_PORT");
        timeoutInputPort.setState(ScheduledState.RUNNING.name());
        inputPorts.add(timeoutInputPort);

        final PortDTO timeoutDataExInputPort = new PortDTO();
        timeoutDataExInputPort.setName("input-timeout-data-ex");
        timeoutDataExInputPort.setId("input-timeout-data-ex-id");
        timeoutDataExInputPort.setType("INPUT_PORT");
        timeoutDataExInputPort.setState(ScheduledState.RUNNING.name());
        inputPorts.add(timeoutDataExInputPort);

        final PortDTO accessDeniedInputPort = new PortDTO();
        accessDeniedInputPort.setName("input-access-denied");
        accessDeniedInputPort.setId("input-access-denied-id");
        accessDeniedInputPort.setType("INPUT_PORT");
        accessDeniedInputPort.setState(ScheduledState.RUNNING.name());
        inputPorts.add(accessDeniedInputPort);

        outputPorts = new HashSet<>();

        final PortDTO runningOutputPort = new PortDTO();
        runningOutputPort.setName("output-running");
        runningOutputPort.setId("output-running-id");
        runningOutputPort.setType("OUTPUT_PORT");
        runningOutputPort.setState(ScheduledState.RUNNING.name());
        outputPorts.add(runningOutputPort);

        final PortDTO timeoutOutputPort = new PortDTO();
        timeoutOutputPort.setName("output-timeout");
        timeoutOutputPort.setId("output-timeout-id");
        timeoutOutputPort.setType("OUTPUT_PORT");
        timeoutOutputPort.setState(ScheduledState.RUNNING.name());
        outputPorts.add(timeoutOutputPort);

        final PortDTO timeoutDataExOutputPort = new PortDTO();
        timeoutDataExOutputPort.setName("output-timeout-data-ex");
        timeoutDataExOutputPort.setId("output-timeout-data-ex-id");
        timeoutDataExOutputPort.setType("OUTPUT_PORT");
        timeoutDataExOutputPort.setState(ScheduledState.RUNNING.name());
        outputPorts.add(timeoutDataExOutputPort);


    }

    @After
    public void after() throws Exception {
        testCaseFinished.countDown();
    }

    private SiteToSiteClient.Builder getDefaultBuilder() {
        return new SiteToSiteClient.Builder().transportProtocol(SiteToSiteTransportProtocol.HTTP)
                .url("http://localhost:" + httpConnector.getLocalPort() + "/nifi")
                .timeout(3, TimeUnit.MINUTES)
                ;
    }

    private SiteToSiteClient.Builder getDefaultBuilderHTTPS() {
        return new SiteToSiteClient.Builder().transportProtocol(SiteToSiteTransportProtocol.HTTP)
                .url("https://localhost:" + sslConnector.getLocalPort() + "/nifi")
                .timeout(3, TimeUnit.MINUTES)
                .keystoreFilename("src/test/resources/certs/keystore.jks")
                .keystorePass("passwordpassword")
                .keystoreType(KeystoreType.JKS)
                .truststoreFilename("src/test/resources/certs/truststore.jks")
                .truststorePass("passwordpassword")
                .truststoreType(KeystoreType.JKS)
                ;
    }

    private static void consumeDataPacket(DataPacket packet) throws IOException {
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        StreamUtils.copy(packet.getData(), bos);
        String contents = new String(bos.toByteArray());
        logger.info("received: {}, {}", contents, packet.getAttributes());
    }


    @Test
    public void testUnknownClusterUrl() throws Exception {

        final URI uri = server.getURI();

        try (
            SiteToSiteClient client = getDefaultBuilder()
                .url("http://" + uri.getHost() + ":" + uri.getPort() + "/unknown")
                .portName("input-running")
                .build()
        ) {
            final Transaction transaction = client.createTransaction(TransferDirection.SEND);

            assertNull(transaction);

        }

    }

    @Test
    public void testWrongPath() throws Exception {

        final URI uri = server.getURI();

        try (
            SiteToSiteClient client = getDefaultBuilder()
                .url("http://" + uri.getHost() + ":" + uri.getPort() + "/wrong")
                .portName("input-running")
                .build()
        ) {
            final Transaction transaction = client.createTransaction(TransferDirection.SEND);

            assertNull(transaction);

        }

    }

    @Test
    public void testNoAvailablePeer() throws Exception {

        peers = new HashSet<>();

        try (
            SiteToSiteClient client = getDefaultBuilder()
                .portName("input-running")
                .build()
        ) {
            final Transaction transaction = client.createTransaction(TransferDirection.SEND);

            assertNull(transaction);

        }

    }

    @Test
    public void testSendUnknownPort() throws Exception {

        try (
            SiteToSiteClient client = getDefaultBuilder()
                .portName("input-unknown")
                .build()
        ) {
            try {
                client.createTransaction(TransferDirection.SEND);
                fail();
            } catch (IOException e) {
                logger.info("Exception message: {}", e.getMessage());
                assertTrue(e.getMessage().contains("Failed to determine the identifier of port"));
            }
        }
    }

    private void testSend(SiteToSiteClient client) throws Exception {

        testSendIgnoreProxyError(client, transaction -> {
            serverChecksum = "1071206772";

            for (int i = 0; i < 20; i++) {
                DataPacket packet = new DataPacketBuilder()
                        .contents("Example contents from client.")
                        .attr("Client attr 1", "Client attr 1 value")
                        .attr("Client attr 2", "Client attr 2 value")
                        .build();
                transaction.send(packet);
                long written = ((Peer)transaction.getCommunicant()).getCommunicationsSession().getBytesWritten();
                logger.info("{}: {} bytes have been written.", i, written);
            }
        });

    }

    @Test
    public void testSendSuccess() throws Exception {

        try (
                final SiteToSiteClient client = getDefaultBuilder()
                    .portName("input-running")
                    .build()
        ) {
            testSend(client);
        }

    }

    @Test
    public void testSendSuccessMultipleUrls() throws Exception {

        final Set<String> urls = new LinkedHashSet<>();
        urls.add("http://localhost:9999");
        urls.add("http://localhost:" + httpConnector.getLocalPort() + "/nifi");

        try (
                final SiteToSiteClient client = getDefaultBuilder()
                        .urls(urls)
                        .portName("input-running")
                        .build()
        ) {
            testSend(client);
        }

    }

    @Test
    public void testSendSuccessWithProxy() throws Exception {

        try (
                final SiteToSiteClient client = getDefaultBuilder()
                        .portName("input-running")
                        .httpProxy(new HttpProxy("localhost", proxyServer.getListenAddress().getPort(), null, null))
                        .build()
        ) {
            testSend(client);
        }

    }

    @Test
    public void testSendProxyAuthFailed() throws Exception {

        try (
                final SiteToSiteClient client = getDefaultBuilder()
                        .portName("input-running")
                        .httpProxy(new HttpProxy("localhost", proxyServerWithAuth.getListenAddress().getPort(), null, null))
                        .build()
        ) {
            final Transaction transaction = client.createTransaction(TransferDirection.SEND);
            assertNull("createTransaction should fail at peer selection and return null.", transaction);
        }

    }

    @Test
    public void testSendSuccessWithProxyAuth() throws Exception {

        try (
                final SiteToSiteClient client = getDefaultBuilder()
                        .portName("input-running")
                        .httpProxy(new HttpProxy("localhost", proxyServerWithAuth.getListenAddress().getPort(), PROXY_USER, PROXY_PASSWORD))
                        .build()
        ) {
            testSend(client);
        }

    }

    @Test
    public void testSendAccessDeniedHTTPS() throws Exception {

        try (
                final SiteToSiteClient client = getDefaultBuilderHTTPS()
                        .portName("input-access-denied")
                        .build()
        ) {
            try {
                client.createTransaction(TransferDirection.SEND);
                fail("Handshake exception should be thrown.");
            } catch (HandshakeException e) {
            }
        }

    }

    @Test
    public void testSendSuccessHTTPS() throws Exception {

        try (
                final SiteToSiteClient client = getDefaultBuilderHTTPS()
                        .portName("input-running")
                        .build()
        ) {
            testSend(client);
        }

    }

    private interface SendData {
        void apply(final Transaction transaction) throws IOException;
    }

    private static void testSendIgnoreProxyError(final SiteToSiteClient client, final SendData function) throws IOException {
        final boolean isProxyEnabled = client.getConfig().getHttpProxy() != null;
        try {
            final Transaction transaction = client.createTransaction(TransferDirection.SEND);

            if (isProxyEnabled && transaction == null) {
                // Transaction is not created sometimes at AppVeyor.
                logger.warn("Transaction was not created. Most likely an environment dependent issue.");
                return;
            }

            assertNotNull(transaction);

            function.apply(transaction);

            transaction.confirm();

            transaction.complete();
        } catch (final IOException e) {
            if (isProxyEnabled && e.getMessage().contains("504")) {
                // Gateway Timeout happens sometimes at Travis CI.
                logger.warn("Request timeout. Most likely an environment dependent issue.", e);
            } else {
                throw e;
            }
        }
    }

    private static void testSendLargeFile(SiteToSiteClient client) throws IOException {

        testSendIgnoreProxyError(client, transaction -> {
            serverChecksum = "1527414060";

            final int contentSize = 10_000;
            final StringBuilder sb = new StringBuilder(contentSize);
            for (int i = 0; i < contentSize; i++) {
                sb.append("a");
            }

            DataPacket packet = new DataPacketBuilder()
                    .contents(sb.toString())
                    .attr("Client attr 1", "Client attr 1 value")
                    .attr("Client attr 2", "Client attr 2 value")
                    .build();
            transaction.send(packet);
            long written = ((Peer)transaction.getCommunicant()).getCommunicationsSession().getBytesWritten();
            logger.info("{} bytes have been written.", written);
        });

    }

    @Test
    public void testSendLargeFileHTTP() throws Exception {

        try (
                SiteToSiteClient client = getDefaultBuilder()
                        .portName("input-running")
                        .build()
        ) {
            testSendLargeFile(client);
        }

    }

    @Test
    public void testSendLargeFileHTTPWithProxy() throws Exception {

        try (
                SiteToSiteClient client = getDefaultBuilder()
                        .portName("input-running")
                        .httpProxy(new HttpProxy("localhost", proxyServer.getListenAddress().getPort(), null, null))
                        .build()
        ) {
            testSendLargeFile(client);
        }

    }

    @Test
    public void testSendLargeFileHTTPWithProxyAuth() throws Exception {

        try (
                SiteToSiteClient client = getDefaultBuilder()
                        .portName("input-running")
                        .httpProxy(new HttpProxy("localhost", proxyServerWithAuth.getListenAddress().getPort(), PROXY_USER, PROXY_PASSWORD))
                        .build()
        ) {
            testSendLargeFile(client);
        }

    }

    @Test
    public void testSendLargeFileHTTPS() throws Exception {

        try (
                SiteToSiteClient client = getDefaultBuilderHTTPS()
                        .portName("input-running")
                        .build()
        ) {
            testSendLargeFile(client);
        }

    }

    @Test
    public void testSendLargeFileHTTPSWithProxy() throws Exception {

        try (
                SiteToSiteClient client = getDefaultBuilderHTTPS()
                        .portName("input-running")
                        .httpProxy(new HttpProxy("localhost", proxyServer.getListenAddress().getPort(), null, null))
                        .build()
        ) {
            testSendLargeFile(client);
        }

    }

    @Test
    public void testSendLargeFileHTTPSWithProxyAuth() throws Exception {

        try (
                SiteToSiteClient client = getDefaultBuilderHTTPS()
                        .portName("input-running")
                        .httpProxy(new HttpProxy("localhost", proxyServerWithAuth.getListenAddress().getPort(), PROXY_USER, PROXY_PASSWORD))
                        .build()
        ) {
            testSendLargeFile(client);
        }

    }

    @Test
    public void testSendSuccessCompressed() throws Exception {

        try (
                SiteToSiteClient client = getDefaultBuilder()
                        .portName("input-running")
                        .useCompression(true)
                        .build()
        ) {
            final Transaction transaction = client.createTransaction(TransferDirection.SEND);

            assertNotNull(transaction);

            serverChecksum = "1071206772";


            for (int i = 0; i < 20; i++) {
                DataPacket packet = new DataPacketBuilder()
                        .contents("Example contents from client.")
                        .attr("Client attr 1", "Client attr 1 value")
                        .attr("Client attr 2", "Client attr 2 value")
                        .build();
                transaction.send(packet);
                long written = ((Peer)transaction.getCommunicant()).getCommunicationsSession().getBytesWritten();
                logger.info("{}: {} bytes have been written.", i, written);
            }

            transaction.confirm();

            transaction.complete();
        }

    }

    @Test
    public void testSendSlowClientSuccess() throws Exception {

        try (
                SiteToSiteClient client = getDefaultBuilder()
                        .idleExpiration(1000, TimeUnit.MILLISECONDS)
                        .portName("input-running")
                        .build()
        ) {
            final Transaction transaction = client.createTransaction(TransferDirection.SEND);

            assertNotNull(transaction);

            serverChecksum = "3882825556";


            for (int i = 0; i < 3; i++) {
                DataPacket packet = new DataPacketBuilder()
                        .contents("Example contents from client.")
                        .attr("Client attr 1", "Client attr 1 value")
                        .attr("Client attr 2", "Client attr 2 value")
                        .build();
                transaction.send(packet);
                long written = ((Peer)transaction.getCommunicant()).getCommunicationsSession().getBytesWritten();
                logger.info("{} bytes have been written.", written);
                Thread.sleep(50);
            }

            transaction.confirm();
            transaction.complete();
        }

    }

    private void completeShouldFail(Transaction transaction) throws IOException {
        try {
            transaction.complete();
            fail("Complete operation should fail since transaction has already failed.");
        } catch (IllegalStateException e) {
            logger.info("An exception was thrown as expected.", e);
        }
    }

    private void confirmShouldFail(Transaction transaction) throws IOException {
        try {
            transaction.confirm();
            fail("Confirm operation should fail since transaction has already failed.");
        } catch (IllegalStateException e) {
            logger.info("An exception was thrown as expected.", e);
        }
    }

    @Test
    public void testSendTimeout() throws Exception {
        assumeFalse(isWindowsEnvironment());//skip on windows
        try (
            SiteToSiteClient client = getDefaultBuilder()
                .timeout(1, TimeUnit.SECONDS)
                .portName("input-timeout")
                .build()
        ) {
            final Transaction transaction = client.createTransaction(TransferDirection.SEND);

            assertNotNull(transaction);

            DataPacket packet = new DataPacketBuilder()
                .contents("Example contents from client.")
                .attr("Client attr 1", "Client attr 1 value")
                .attr("Client attr 2", "Client attr 2 value")
                .build();
            serverChecksum = "1345413116";

            transaction.send(packet);
            try {
                transaction.confirm();
                fail();
            } catch (IOException e) {
                logger.info("An exception was thrown as expected.", e);
                assertTrue(e.getMessage().contains("TimeoutException"));
            }

            completeShouldFail(transaction);
        }

    }

    private boolean isWindowsEnvironment() {
        return System.getProperty("os.name").toLowerCase().startsWith("windows");
    }

    @Test
    public void testSendTimeoutAfterDataExchange() throws Exception {
        assumeFalse(isWindowsEnvironment());//skip on windows
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi.remote.protocol.http.HttpClientTransaction", "INFO");

        try (
                SiteToSiteClient client = getDefaultBuilder()
                        .idleExpiration(500, TimeUnit.MILLISECONDS)
                        .timeout(500, TimeUnit.MILLISECONDS)
                        .portName("input-timeout-data-ex")
                        .build()
        ) {
            final Transaction transaction = client.createTransaction(TransferDirection.SEND);

            assertNotNull(transaction);

            DataPacket packet = new DataPacketBuilder()
                    .contents("Example contents from client.")
                    .attr("Client attr 1", "Client attr 1 value")
                    .attr("Client attr 2", "Client attr 2 value")
                    .build();

                for(int i = 0; i < 100; i++) {
                    transaction.send(packet);
                    if (i % 10 == 0) {
                        logger.info("Sent {} packets...", i);
                    }
                }

            try {
                confirmShouldFail(transaction);
                fail("Should be timeout.");
            } catch (IOException e) {
                logger.info("Exception message: {}", e.getMessage());
                assertTrue(e.getMessage().contains("TimeoutException"));
            }

            completeShouldFail(transaction);
        }

    }

    @Test
    public void testReceiveUnknownPort() throws Exception {

        try (
            SiteToSiteClient client = getDefaultBuilder()
                .portName("output-unknown")
                .build()
        ) {
            try {
                client.createTransaction(TransferDirection.RECEIVE);
                fail();
            } catch (IOException e) {
                logger.info("Exception message: {}", e.getMessage());
                assertTrue(e.getMessage().contains("Failed to determine the identifier of port"));
            }
        }
    }

    private void testReceive(SiteToSiteClient client) throws IOException {
        final Transaction transaction = client.createTransaction(TransferDirection.RECEIVE);

        assertNotNull(transaction);

        DataPacket packet;
        while ((packet = transaction.receive()) != null) {
            consumeDataPacket(packet);
        }
        transaction.confirm();
        transaction.complete();
    }

    @Test
    public void testReceiveSuccess() throws Exception {

        try (
            SiteToSiteClient client = getDefaultBuilder()
                .portName("output-running")
                .build()
        ) {
            testReceive(client);
        }
    }

    @Test
    public void testReceiveSuccessWithProxy() throws Exception {

        try (
                SiteToSiteClient client = getDefaultBuilder()
                        .portName("output-running")
                        .httpProxy(new HttpProxy("localhost", proxyServer.getListenAddress().getPort(), null, null))
                        .build()
        ) {
            testReceive(client);
        }
    }

    @Test
    public void testReceiveSuccessWithProxyAuth() throws Exception {

        try (
                SiteToSiteClient client = getDefaultBuilder()
                        .portName("output-running")
                        .httpProxy(new HttpProxy("localhost", proxyServerWithAuth.getListenAddress().getPort(), PROXY_USER, PROXY_PASSWORD))
                        .build()
        ) {
            testReceive(client);
        }
    }

    @Test
    public void testReceiveSuccessHTTPS() throws Exception {

        try (
                SiteToSiteClient client = getDefaultBuilderHTTPS()
                        .portName("output-running")
                        .build()
        ) {
            testReceive(client);
        }
    }

    @Test
    public void testReceiveSuccessHTTPSWithProxy() throws Exception {

        try (
                SiteToSiteClient client = getDefaultBuilderHTTPS()
                        .portName("output-running")
                        .httpProxy(new HttpProxy("localhost", proxyServer.getListenAddress().getPort(), null, null))
                        .build()
        ) {
            testReceive(client);
        }
    }

    @Test
    public void testReceiveSuccessHTTPSWithProxyAuth() throws Exception {

        try (
                SiteToSiteClient client = getDefaultBuilderHTTPS()
                        .portName("output-running")
                        .httpProxy(new HttpProxy("localhost", proxyServerWithAuth.getListenAddress().getPort(), PROXY_USER, PROXY_PASSWORD))
                        .build()
        ) {
            testReceive(client);
        }
    }

    @Test
    public void testReceiveSuccessCompressed() throws Exception {

        try (
                SiteToSiteClient client = getDefaultBuilder()
                        .portName("output-running")
                        .useCompression(true)
                        .build()
        ) {
            testReceive(client);
        }
    }

    @Test
    public void testReceiveSlowClientSuccess() throws Exception {

        try (
                SiteToSiteClient client = getDefaultBuilder()
                        .portName("output-running")
                        .build()
        ) {
            final Transaction transaction = client.createTransaction(TransferDirection.RECEIVE);

            assertNotNull(transaction);

            DataPacket packet;
            while ((packet = transaction.receive()) != null) {
                consumeDataPacket(packet);
                Thread.sleep(500);
            }
            transaction.confirm();
            transaction.complete();
        }
    }

    @Test
    public void testReceiveTimeout() throws Exception {

        try (
                SiteToSiteClient client = getDefaultBuilder()
                        .timeout(1, TimeUnit.SECONDS)
                        .portName("output-timeout")
                        .build()
        ) {
            try {
                client.createTransaction(TransferDirection.RECEIVE);
                fail();
            } catch (IOException e) {
                logger.info("An exception was thrown as expected.", e);
                assertTrue(e instanceof SocketTimeoutException);
            }
        }
    }

    @Test
    public void testReceiveTimeoutAfterDataExchange() throws Exception {

        try (
                SiteToSiteClient client = getDefaultBuilder()
                        .timeout(1, TimeUnit.SECONDS)
                        .portName("output-timeout-data-ex")
                        .build()
        ) {
            final Transaction transaction = client.createTransaction(TransferDirection.RECEIVE);
            assertNotNull(transaction);

            DataPacket packet = transaction.receive();
            assertNotNull(packet);
            consumeDataPacket(packet);

            try {
                transaction.receive();
                fail();
            } catch (IOException e) {
                logger.info("An exception was thrown as expected.", e);
                assertTrue(e.getCause() instanceof SocketTimeoutException);
            }

            confirmShouldFail(transaction);
            completeShouldFail(transaction);
        }
    }

}
