package org.apache.nifi.remote.protocol.raw;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.nio.Configurations;
import org.apache.nifi.nio.IOConsumer;
import org.apache.nifi.nio.MessageSequenceHandler;
import org.apache.nifi.nio.MessageSequenceTest.ClientRunnable;
import org.apache.nifi.nio.MessageSequenceTest.MessageSequenceHandlerProvider;
import org.apache.nifi.nio.MessageSequenceTest.ServerStarted;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.remote.Peer;
import org.apache.nifi.remote.PortAuthorizationResult;
import org.apache.nifi.remote.PublicPort;
import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.TransactionCompletion;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.remote.cluster.NodeInformation;
import org.apache.nifi.remote.protocol.DataPacket;
import org.apache.nifi.remote.protocol.FlowFileRequest;
import org.apache.nifi.remote.protocol.ProcessingResult;
import org.apache.nifi.remote.protocol.ServerProtocol;
import org.apache.nifi.remote.util.StandardDataPacket;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockFlowFileQueue;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.MockProcessSession;
import org.apache.nifi.util.SharedSessionState;
import org.apache.nifi.web.api.dto.ControllerDTO;
import org.apache.nifi.web.api.dto.PortDTO;
import org.apache.nifi.web.api.entity.ControllerEntity;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.apache.nifi.nio.MessageSequenceTest.testMessageSequence;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestSiteToSiteMessageSequence extends AbstractS2SMessageSequenceTest {

    private static void respondWithJson(HttpServletResponse resp, Object entity, int statusCode) throws IOException {
        resp.setContentType("application/json");
        resp.setStatus(statusCode);
        final ServletOutputStream out = resp.getOutputStream();
        new ObjectMapper().writer().writeValue(out, entity);
        out.flush();
    }

    private int startJetty(int rawProtocolPort, boolean siteToSiteSecure) throws Exception {
        final Server server = new Server();
        final ServletContextHandler contextHandler = new ServletContextHandler();
        contextHandler.setContextPath("/nifi-api");
        server.setHandler(contextHandler);

        final ServletHandler servletHandler = new ServletHandler();
        contextHandler.insertHandler(servletHandler);

        ServerConnector httpConnector = new ServerConnector(server);

        server.setConnectors(new Connector[]{httpConnector});

        HttpServlet siteInfoServlet = new HttpServlet() {
            @Override
            protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
                final ControllerDTO controller = new ControllerDTO();

                controller.setRemoteSiteListeningPort(rawProtocolPort);
                controller.setSiteToSiteSecure(siteToSiteSecure);

                controller.setId("remote-controller-id");
                controller.setInstanceId("remote-instance-id");
                controller.setName("Remote NiFi Flow");

                Set<PortDTO> inputPorts = new HashSet<>();
                final PortDTO inputPort1 = new PortDTO();
                inputPort1.setName("input-1");
                inputPort1.setId("input-1-id");
                inputPort1.setType("INPUT_PORT");
                inputPort1.setState(ScheduledState.RUNNING.name());
                inputPorts.add(inputPort1);

                controller.setInputPorts(inputPorts);
                controller.setInputPortCount(inputPorts.size());

                Set<PortDTO> outputPorts = new HashSet<>();
                final PortDTO outputPort1 = new PortDTO();
                outputPort1.setName("output-1");
                outputPort1.setId("output-1-id");
                outputPort1.setType("OUTPUT_PORT");
                outputPort1.setState(ScheduledState.RUNNING.name());
                outputPorts.add(outputPort1);

                controller.setOutputPorts(outputPorts);
                controller.setOutputPortCount(outputPorts.size());

                final ControllerEntity controllerEntity = new ControllerEntity();
                controllerEntity.setController(controller);

                respondWithJson(resp, controllerEntity, HttpServletResponse.SC_OK);
            }
        };

        servletHandler.addServletWithMapping(new ServletHolder(siteInfoServlet), "/site-to-site");

        server.start();

        log.info("Starting server on port {} for HTTP", httpConnector.getLocalPort());
        return httpConnector.getLocalPort();
    }

    @FunctionalInterface
    private interface PortSimulatorCreator {
        ServerStarted createPortSimulator(final BlockingQueue<FlowFileRequest> flowFileRequests);
    }

    private void testWithPortSimulator(boolean secureSiteToSite, IOConsumer<Integer> client, ProcessGroup processGroup, PublicPort port, PortSimulatorCreator simulatorCreator) throws Throwable {
        final BlockingQueue<FlowFileRequest> flowFileRequests = new ArrayBlockingQueue<>(1);
        final Answer<Object> offerFlowFileRequest = invocationOnMock -> {
            // Offer a new request to kick off the timer driven thread.
            final Peer peer = invocationOnMock.getArgument(0, Peer.class);
            final ServerProtocol protocol = invocationOnMock.getArgument(1, ServerProtocol.class);
            final FlowFileRequest request = new FlowFileRequest(peer, protocol);
            log.info("Offering new FlowFileRequest from {}", peer);
            flowFileRequests.offer(request);
            return request;
        };
        when(port.startReceivingFlowFiles(any(Peer.class), any(ServerProtocol.class))).then(offerFlowFileRequest);
        when(port.startTransferringFlowFiles(any(Peer.class), any(ServerProtocol.class))).then(offerFlowFileRequest);

        final ServerStarted timerDrivenThread = simulatorCreator.createPortSimulator(flowFileRequests);

        final AtomicInteger numOfShutdown = new AtomicInteger(0);
        MessageSequenceHandlerProvider handlerProvider = (serverAddress, latch, errorHandler) -> {
            final NodeInformation nodeInfo = new NodeInformation(serverAddress.getHostName(), serverAddress.getPort(), null, 8080, false, 100);
            return new MessageSequenceHandler<>(
                getClass().getSimpleName(),
                () -> new SiteToSiteMessageSequence(serverAddress, nodeInfo, null, null, () -> processGroup, new Configurations.Builder().build()),
                sequence -> {
                    final int i = numOfShutdown.incrementAndGet();
                    log.info("S2S sequence has finished!! {}", i);
                    if (i == 2) {
                        // There should be 2 shutdown requests. One is the pool requesting peer, another is when the client is closed.
                        log.info("Server completed, stopping.");
                        latch.countDown();
                    }
                },
                (sequence, e) -> errorHandler.accept(e));
        };


        CountDownLatch httpServerStarted = new CountDownLatch(1);
        final AtomicInteger httpServerPort = new AtomicInteger(0);
        ServerStarted httpServer = new ServerStarted("NiFi REST API", serverAddress -> {
            try {
                httpServerPort.set(startJetty(serverAddress.getPort(), secureSiteToSite));
                httpServerStarted.countDown();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        ServerStarted siteToSiteClient = new ServerStarted("S2S-Client", serverAddress -> {
            try {
                httpServerStarted.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            final int apiPort = httpServerPort.get();
            log.info("Going to connect using localhost:{}", apiPort);

            client.accept(apiPort);

        });

        testMessageSequence(handlerProvider,
            new ServerStarted[]{httpServer, siteToSiteClient, timerDrivenThread},
            new ClientRunnable[0], secureSiteToSite);
    }

    @SuppressWarnings("unchecked")
    private void testReceive(boolean secureSiteToSite, IOConsumer<Integer> client, Consumer<MockProcessSession> ... assertions) throws Throwable {

        final ProcessGroup processGroup = mock(ProcessGroup.class);
        final PublicPort port = mock(PublicPort.class);
        final PortAuthorizationResult authorizationResult = mock(PortAuthorizationResult.class);

        when(authorizationResult.isAuthorized()).thenReturn(true);
        when(port.checkUserAuthorization(anyString())).thenReturn(authorizationResult);
        when(port.isValid()).thenReturn(true);
        when(port.isRunning()).thenReturn(true);
        when(processGroup.isRootGroup()).thenReturn(true);
        when(processGroup.getInputPort(eq("input-1-id"))).thenReturn(port);

        final PortSimulatorCreator simulatorCreator = flowFileRequests -> new ServerStarted("InputPortSimulator", serverAddress -> {
            // TODO: Refactor this.
            final AtomicInteger txCount = new AtomicInteger(0);
            while (txCount.get() < assertions.length) {

                final FlowFileRequest request;
                try {
                    log.info("Polling new FlowFileRequests");
                    request = flowFileRequests.poll(100, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                if (request == null) {
                    continue;
                }

                log.info("Start processing request {}", request);
                request.setServiceBegin();
                final ServerProtocol protocol = request.getProtocol();

                // Create a dummy processor to use mock framework.
                final Processor processor = mock(Processor.class);
                when(processor.getIdentifier()).thenReturn("dummy-processor");
                when(processor.getRelationships()).thenReturn(Collections.singleton(Relationship.ANONYMOUS));

                final ProcessContext processContext = new MockProcessContext(processor);
                final MockProcessSession processSession = new MockProcessSession(
                    new SharedSessionState(processor, new AtomicLong(0)), processor);

                final int received = protocol.receiveFlowFiles(request.getPeer(), processContext, processSession, protocol.getPreNegotiatedCodec());
                log.debug("### received={}, at tx={}", received, txCount.get());
                request.getResponseQueue().add(new ProcessingResult(received));

                assertions[txCount.getAndIncrement()].accept(processSession);

                log.info("Tx {} finished.", txCount.get());
            }
        });
        testWithPortSimulator(secureSiteToSite, client, processGroup, port, simulatorCreator);
    }


    @SuppressWarnings("unchecked")
    private void testReceiveSingleFile(boolean secure) throws Throwable {

        testReceive(secure, apiPort -> {
            try (final SiteToSiteClient client = createClient(apiPort, "input-1")) {

                Map<String, String> attributes = new HashMap<>();
                attributes.put("foo", "1");
                attributes.put("bar", "1");
                final Transaction transaction = client.createTransaction(TransferDirection.SEND);
                transaction.send("test1".getBytes(StandardCharsets.UTF_8), attributes);
                transaction.confirm();
                transaction.complete();
            }

        }, processSession -> {
            final List<MockFlowFile> flowFiles = processSession.getFlowFilesForRelationship(Relationship.ANONYMOUS);
            assertEquals(1, flowFiles.size());
            flowFiles.get(0).assertContentEquals("test1", StandardCharsets.UTF_8);
        });

    }

    @Test
    public void testReceiveSingleFile() throws Throwable {
        testReceiveSingleFile(false);
    }

    @Test
    public void testReceiveSingleFileSecure() throws Throwable {
        testReceiveSingleFile(true);
    }

    @SuppressWarnings("unchecked")
    private void testReceiveLargeFile(boolean secure) throws Throwable {

        final File largeFile = new File("/Users/koji/Downloads/nifi-1.2.0-bin.zip");

        if (!largeFile.isFile()) {
            fail("Cannot read the file to send");
        }

        testReceive(secure, apiPort -> {
            try (final SiteToSiteClient client = createClient(apiPort, "input-1");
                 final FileInputStream fileInputStream = new FileInputStream(largeFile)) {

                Map<String, String> attributes = new HashMap<>();
                attributes.put("foo", "1");
                attributes.put("bar", "1");
                final Transaction transaction = client.createTransaction(TransferDirection.SEND);
                final StandardDataPacket packet = new StandardDataPacket(attributes, fileInputStream, largeFile.length());
                transaction.send(packet);
                transaction.confirm();
                transaction.complete();
            }

        }, processSession -> {
            final List<MockFlowFile> flowFiles = processSession.getFlowFilesForRelationship(Relationship.ANONYMOUS);
            assertEquals(1, flowFiles.size());
            assertEquals(largeFile.length(), flowFiles.get(0).getSize());
        });

    }

    @Ignore
    @Test
    public void testReceiveLargeFile() throws Throwable {
        testReceiveLargeFile(false);
    }

    @Ignore
    @Test
    public void testReceiveLargeFileSecure() throws Throwable {
        testReceiveLargeFile(true);
    }


    @Test
    @SuppressWarnings("unchecked")
    public void testReceiveMultipleFile() throws Throwable {

        testReceive(false, apiPort -> {
            try (final SiteToSiteClient client = createClient(apiPort, "input-1")) {

                Map<String, String> attributes = new HashMap<>();
                attributes.put("foo", "1");
                attributes.put("bar", "1");
                final Transaction transaction = client.createTransaction(TransferDirection.SEND);
                transaction.send("test1".getBytes(StandardCharsets.UTF_8), attributes);
                transaction.send("test2".getBytes(StandardCharsets.UTF_8), Collections.emptyMap());
                transaction.confirm();
                transaction.complete();

            }

        }, processSession -> {
            final List<MockFlowFile> flowFiles = processSession.getFlowFilesForRelationship(Relationship.ANONYMOUS);
            assertEquals(2, flowFiles.size());
            flowFiles.get(0).assertContentEquals("test1", StandardCharsets.UTF_8);
            flowFiles.get(1).assertContentEquals("test2", StandardCharsets.UTF_8);
        });

    }

    @SuppressWarnings("unchecked")
    public void testReceiveMultipleTx(boolean secure) throws Throwable {
        final int numOfTx = 50;
        final Consumer<MockProcessSession>[] checkTxResults = new Consumer[numOfTx];
        for (int i = 0; i < numOfTx; i++) {
            final int fi = i;
            checkTxResults[i] = processSession -> {
                final List<MockFlowFile> flowFiles = processSession.getFlowFilesForRelationship(Relationship.ANONYMOUS);
                assertEquals(1, flowFiles.size());
                flowFiles.get(0).assertContentEquals("test" + fi, StandardCharsets.UTF_8);
            };
        }

        testReceive(secure, apiPort -> {
            try (final SiteToSiteClient client = createClient(apiPort, "input-1")) {

                for (int i = 0; i < numOfTx; i++) {
                    Map<String, String> attributes = new HashMap<>();
                    attributes.put("foo", String.valueOf(i));
                    final Transaction transaction = client.createTransaction(TransferDirection.SEND);
                    transaction.send(("test" + i).getBytes(StandardCharsets.UTF_8), attributes);
                    transaction.confirm();
                    final TransactionCompletion complete = transaction.complete();
                    assertEquals(1, complete.getDataPacketsTransferred());
                }

            }

        }, checkTxResults);
    }

    @Test
    public void testReceiveMultipleTx() throws Throwable {
        testReceiveMultipleTx(false);
    }

    @Test
    public void testReceiveMultipleTxSecure() throws Throwable {
        testReceiveMultipleTx(true);
    }

    @SuppressWarnings("unchecked")
    private void testTransfer(boolean secureSiteToSite, BiConsumer<MockProcessSession, MockFlowFileQueue> setupFlowFiles,
                              IOConsumer<Integer> client, Consumer<MockProcessSession> ... assertions) throws Throwable {

        final ProcessGroup processGroup = mock(ProcessGroup.class);
        final PublicPort port = mock(PublicPort.class);
        final PortAuthorizationResult authorizationResult = mock(PortAuthorizationResult.class);

        when(authorizationResult.isAuthorized()).thenReturn(true);
        when(port.checkUserAuthorization(anyString())).thenReturn(authorizationResult);
        when(port.isValid()).thenReturn(true);
        when(port.isRunning()).thenReturn(true);
        when(processGroup.isRootGroup()).thenReturn(true);
        when(processGroup.getOutputPort(eq("output-1-id"))).thenReturn(port);

        final PortSimulatorCreator simulatorCreator = flowFileRequests -> new ServerStarted("OutputPortSimulator", serverAddress -> {

            // Create a dummy processor to use mock framework.
            final Processor processor = mock(Processor.class);
            when(processor.getIdentifier()).thenReturn("dummy-processor");
            when(processor.getRelationships()).thenReturn(Collections.singleton(Relationship.ANONYMOUS));

            final ProcessContext processContext = new MockProcessContext(processor);
            final SharedSessionState sharedState = new SharedSessionState(processor, new AtomicLong(0));
            final MockProcessSession processSession = new MockProcessSession(sharedState, processor);
            final MockFlowFileQueue flowFileQueue = sharedState.getFlowFileQueue();

            setupFlowFiles.accept(processSession, flowFileQueue);

            final AtomicInteger txCount = new AtomicInteger(0);
            while (txCount.get() < assertions.length) {

                final FlowFileRequest request;
                try {
                    log.info("Polling new FlowFileRequests");
                    request = flowFileRequests.poll(100, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                if (request == null) {
                    continue;
                }

                log.info("Start processing request {}", request);
                request.setServiceBegin();
                final ServerProtocol protocol = request.getProtocol();


                final int transferred = protocol.transferFlowFiles(request.getPeer(), processContext, processSession, protocol.getPreNegotiatedCodec());
                log.debug("### transferred={}, at tx={}", transferred, txCount.get());
                request.getResponseQueue().add(new ProcessingResult(transferred));

                assertions[txCount.getAndIncrement()].accept(processSession);

                log.info("Tx {} finished.", txCount.get());
            }
        });

        testWithPortSimulator(secureSiteToSite, client, processGroup, port, simulatorCreator);
    }

    @SuppressWarnings("unchecked")
    private void testTransferSingleFile(boolean secure) throws Throwable {

        testTransfer(secure, (processSession, flowFileQueue) -> {
            Map<String, String> attributes = new HashMap<>();
            attributes.put("foo", "1");
            attributes.put("bar", "1");
            final MockFlowFile flowFile = processSession.createFlowFile("test".getBytes(StandardCharsets.UTF_8), attributes);
            flowFileQueue.offer(flowFile);

        }, apiPort -> {
            try (final SiteToSiteClient client = createClient(apiPort, "output-1")) {

                final Transaction transaction = client.createTransaction(TransferDirection.RECEIVE);
                final DataPacket packet = transaction.receive();
                assertEquals(5, packet.getAttributes().size());
                assertEquals(4, packet.getSize());
                final byte[] bytes = new byte[(int) packet.getSize()];
                assertEquals(4, packet.getData().read(bytes));
                assertEquals("test", new String(bytes, StandardCharsets.UTF_8));

                assertNull("No more data to receive", transaction.receive());
                transaction.confirm();
                transaction.complete();

                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

        }, processSession -> {
            assertEquals(0, processSession.getQueueSize().getObjectCount());
        });
    }

    @Test
    public void testTransferSingleFile() throws Throwable {
        testTransferSingleFile(false);
    }

    @Test
    public void testTransferSingleFileSecure() throws Throwable {
        testTransferSingleFile(true);
    }

    @SuppressWarnings("unchecked")
    private void testTransferLargeFile(boolean secure) throws Throwable {

        final int numOfFlowFile = 1;
        final byte[] data = new byte[200 * 1024 * 1024];

        testTransfer(secure, (processSession, flowFileQueue) -> {
            Map<String, String> attributes = new HashMap<>();
            attributes.put("foo", "1");
            attributes.put("bar", "1");

            for (int i = 0; i < numOfFlowFile; i++) {
                final MockFlowFile flowFile = processSession.createFlowFile(data, attributes);
                flowFileQueue.offer(flowFile);
            }

        }, apiPort -> {
            try (final SiteToSiteClient client = createClient(apiPort, "output-1")) {

                int receivedFlowFiles = 0;
                while (receivedFlowFiles < numOfFlowFile) {
                    final Transaction transaction = client.createTransaction(TransferDirection.RECEIVE);

                    DataPacket packet;
                    while ((packet = transaction.receive()) != null) {
                        assertEquals(5, packet.getAttributes().size());
                        assertEquals(data.length, packet.getSize());
                        final byte[] bytes = new byte[(int) packet.getSize()];
                        StreamUtils.read(packet.getData(), bytes, bytes.length);
                        receivedFlowFiles++;
                    }

                    transaction.confirm();
                    transaction.complete();

                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }

            }

        }, processSession -> {
            assertEquals(0, processSession.getQueueSize().getObjectCount());
        });
    }

    @Ignore
    @Test
    public void testTransferLargeFile() throws Throwable {
        testTransferLargeFile(false);
    }

    @SuppressWarnings("unchecked")
    private void testTransferMultipleFlowFiles(boolean secure) throws Throwable {

        final int numOfFlowFiles = 5;
        final int numOfTx = 1;
        final Consumer<MockProcessSession>[] checkTxResults = new Consumer[numOfTx];
        for (int i = 0; i < numOfTx; i++) {
            checkTxResults[i] = processSession -> {
                assertEquals(0, processSession.getQueueSize().getObjectCount());
            };
        }

        testTransfer(secure, (processSession, flowFileQueue) -> {

            for (int i = 0; i < numOfFlowFiles; i++) {
                Map<String, String> attributes = new HashMap<>();
                attributes.put("foo", "1");
                attributes.put("bar", "1");
                final MockFlowFile flowFile = processSession.createFlowFile("test".getBytes(StandardCharsets.UTF_8), attributes);

                flowFileQueue.offer(flowFile);
            }

        }, apiPort -> {
            try (final SiteToSiteClient client = createClient(apiPort, "output-1")) {

                int receivedFlowFiles = 0;
                while (receivedFlowFiles < numOfFlowFiles)  {

                    final Transaction transaction = client.createTransaction(TransferDirection.RECEIVE);
                    DataPacket packet;
                    while ((packet = transaction.receive()) != null) {
                        assertEquals(5, packet.getAttributes().size());
                        assertEquals(4, packet.getSize());
                        final byte[] bytes = new byte[(int) packet.getSize()];
                        assertEquals(4, packet.getData().read(bytes));
                        assertEquals("test", new String(bytes, StandardCharsets.UTF_8));

                        receivedFlowFiles++;
                    }

                    transaction.confirm();
                    transaction.complete();

                }

            }

        }, checkTxResults);
    }

    @Test
    public void testTransferMultipleFlowFiles() throws Throwable {
        testTransferMultipleFlowFiles(false);
    }

}
