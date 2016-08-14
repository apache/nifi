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

import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.remote.Peer;
import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.client.KeystoreType;
import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.remote.codec.StandardFlowFileCodec;
import org.apache.nifi.remote.io.CompressionInputStream;
import org.apache.nifi.remote.io.CompressionOutputStream;
import org.apache.nifi.remote.protocol.DataPacket;
import org.apache.nifi.remote.protocol.ResponseCode;
import org.apache.nifi.remote.protocol.SiteToSiteTransportProtocol;
import org.apache.nifi.remote.protocol.http.HttpHeaders;
import org.apache.nifi.remote.util.StandardDataPacket;
import org.apache.nifi.stream.io.ByteArrayInputStream;
import org.apache.nifi.stream.io.ByteArrayOutputStream;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.web.api.dto.ControllerDTO;
import org.apache.nifi.web.api.dto.PortDTO;
import org.apache.nifi.web.api.dto.remote.PeerDTO;
import org.apache.nifi.web.api.entity.ControllerEntity;
import org.apache.nifi.web.api.entity.PeersEntity;
import org.apache.nifi.web.api.entity.TransactionResultEntity;
import org.codehaus.jackson.map.ObjectMapper;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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

public class TestHttpClient {

    private static Logger logger = LoggerFactory.getLogger(TestHttpClient.class);

    private static Server server;
    private static ServerConnector httpConnector;
    private static ServerConnector sslConnector;
    final private static AtomicBoolean isTestCaseFinished = new AtomicBoolean(false);

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
        while (!isTestCaseFinished.get()) {
            try {
                logger.info("Sleeping...");
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                logger.info("Got an exception while sleeping.", e);
                break;
            }
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
        server = new Server(0);

        ServletContextHandler contextHandler = new ServletContextHandler();
        contextHandler.setContextPath("/nifi-api");
        server.setHandler(contextHandler);

        ServletHandler servletHandler = new ServletHandler();
        contextHandler.insertHandler(servletHandler);

        SslContextFactory sslContextFactory = new SslContextFactory();
        sslContextFactory.setKeyStorePath("src/test/resources/certs/localhost-ks.jks");
        sslContextFactory.setKeyStorePassword("localtest");
        sslContextFactory.setKeyStoreType("JKS");

        httpConnector = new ServerConnector(server);

        HttpConfiguration https = new HttpConfiguration();
        https.addCustomizer(new SecureRequestCustomizer());
        sslConnector = new ServerConnector(server,
                new SslConnectionFactory(sslContextFactory, "http/1.1"),
                new HttpConnectionFactory(https));

        server.setConnectors(new Connector[] { httpConnector, sslConnector });

        servletHandler.addServletWithMapping(SiteInfoServlet.class, "/site-to-site");
        servletHandler.addServletWithMapping(PeersServlet.class, "/site-to-site/peers");

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
    }

    @AfterClass
    public static void teardown() throws Exception {
        logger.info("Stopping server.");
        server.stop();
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

        final URI uri = server.getURI();
        isTestCaseFinished.set(false);

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
        runningInputPort.setId("running-input-port");
        inputPorts.add(runningInputPort);
        runningInputPort.setName("input-running");
        runningInputPort.setId("input-running-id");
        runningInputPort.setType("INPUT_PORT");
        runningInputPort.setState(ScheduledState.RUNNING.name());

        final PortDTO timeoutInputPort = new PortDTO();
        timeoutInputPort.setId("timeout-input-port");
        inputPorts.add(timeoutInputPort);
        timeoutInputPort.setName("input-timeout");
        timeoutInputPort.setId("input-timeout-id");
        timeoutInputPort.setType("INPUT_PORT");
        timeoutInputPort.setState(ScheduledState.RUNNING.name());

        final PortDTO timeoutDataExInputPort = new PortDTO();
        timeoutDataExInputPort.setId("timeout-dataex-input-port");
        inputPorts.add(timeoutDataExInputPort);
        timeoutDataExInputPort.setName("input-timeout-data-ex");
        timeoutDataExInputPort.setId("input-timeout-data-ex-id");
        timeoutDataExInputPort.setType("INPUT_PORT");
        timeoutDataExInputPort.setState(ScheduledState.RUNNING.name());

        outputPorts = new HashSet<>();

        final PortDTO runningOutputPort = new PortDTO();
        runningOutputPort.setId("running-output-port");
        outputPorts.add(runningOutputPort);
        runningOutputPort.setName("output-running");
        runningOutputPort.setId("output-running-id");
        runningOutputPort.setType("OUTPUT_PORT");
        runningOutputPort.setState(ScheduledState.RUNNING.name());

        final PortDTO timeoutOutputPort = new PortDTO();
        timeoutOutputPort.setId("timeout-output-port");
        outputPorts.add(timeoutOutputPort);
        timeoutOutputPort.setName("output-timeout");
        timeoutOutputPort.setId("output-timeout-id");
        timeoutOutputPort.setType("OUTPUT_PORT");
        timeoutOutputPort.setState(ScheduledState.RUNNING.name());

        final PortDTO timeoutDataExOutputPort = new PortDTO();
        timeoutDataExOutputPort.setId("timeout-dataex-output-port");
        outputPorts.add(timeoutDataExOutputPort);
        timeoutDataExOutputPort.setName("output-timeout-data-ex");
        timeoutDataExOutputPort.setId("output-timeout-data-ex-id");
        timeoutDataExOutputPort.setType("OUTPUT_PORT");
        timeoutDataExOutputPort.setState(ScheduledState.RUNNING.name());


    }

    @After
    public void after() throws Exception {
        isTestCaseFinished.set(true);
    }

    private SiteToSiteClient.Builder getDefaultBuilder() {
        return new SiteToSiteClient.Builder().transportProtocol(SiteToSiteTransportProtocol.HTTP)
                .url("http://localhost:" + httpConnector.getLocalPort() + "/nifi")
                ;
    }

    private SiteToSiteClient.Builder getDefaultBuilderHTTPS() {
        return new SiteToSiteClient.Builder().transportProtocol(SiteToSiteTransportProtocol.HTTP)
                .url("https://localhost:" + sslConnector.getLocalPort() + "/nifi")
                .keystoreFilename("src/test/resources/certs/localhost-ks.jks")
                .keystorePass("localtest")
                .keystoreType(KeystoreType.JKS)
                .truststoreFilename("src/test/resources/certs/localhost-ts.jks")
                .truststorePass("localtest")
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
    public void testUnkownClusterUrl() throws Exception {

        final URI uri = server.getURI();

        try (
            SiteToSiteClient client = getDefaultBuilder()
                .url("http://" + uri.getHost() + ":" + uri.getPort() + "/unkown")
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

    @Test
    public void testSendSuccess() throws Exception {

        try (
            SiteToSiteClient client = getDefaultBuilder()
                .portName("input-running")
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
    public void testSendSuccessHTTPS() throws Exception {

        try (
                SiteToSiteClient client = getDefaultBuilderHTTPS()
                        .portName("input-running")
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

    private static void testSendLargeFile(SiteToSiteClient client) throws IOException {
        final Transaction transaction = client.createTransaction(TransferDirection.SEND);

        assertNotNull(transaction);

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

        transaction.confirm();

        transaction.complete();
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

    @Test
    public void testSendTimeoutAfterDataExchange() throws Exception {

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

    @Test
    public void testReceiveSuccess() throws Exception {

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
            }
            transaction.confirm();
            transaction.complete();
        }
    }

    @Test
    public void testReceiveSuccessHTTPS() throws Exception {

        try (
                SiteToSiteClient client = getDefaultBuilderHTTPS()
                        .portName("output-running")
                        .build()
        ) {
            final Transaction transaction = client.createTransaction(TransferDirection.RECEIVE);

            assertNotNull(transaction);

            DataPacket packet;
            while ((packet = transaction.receive()) != null) {
                consumeDataPacket(packet);
            }
            transaction.confirm();
            transaction.complete();
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
            final Transaction transaction = client.createTransaction(TransferDirection.RECEIVE);

            assertNotNull(transaction);

            DataPacket packet;
            while ((packet = transaction.receive()) != null) {
                consumeDataPacket(packet);
            }
            transaction.confirm();
            transaction.complete();
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
