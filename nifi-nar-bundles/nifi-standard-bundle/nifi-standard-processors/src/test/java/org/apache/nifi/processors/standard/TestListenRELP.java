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

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.util.listen.dispatcher.ChannelDispatcher;
import org.apache.nifi.processor.util.listen.response.ChannelResponder;
import org.apache.nifi.processors.standard.relp.event.RELPEvent;
import org.apache.nifi.processors.standard.relp.frame.RELPEncoder;
import org.apache.nifi.processors.standard.relp.frame.RELPFrame;
import org.apache.nifi.processors.standard.relp.response.RELPResponse;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.ssl.StandardSSLContextService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.Socket;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

public class TestListenRELP {

    public static final String OPEN_FRAME_DATA = "relp_version=0\nrelp_software=librelp,1.2.7,http://librelp.adiscon.com\ncommands=syslog";
    public static final String SYSLOG_FRAME_DATA = "this is a syslog message here";

    static final RELPFrame OPEN_FRAME = new RELPFrame.Builder()
            .txnr(1)
            .command("open")
            .dataLength(OPEN_FRAME_DATA.length())
            .data(OPEN_FRAME_DATA.getBytes(StandardCharsets.UTF_8))
            .build();

    static final RELPFrame SYSLOG_FRAME = new RELPFrame.Builder()
            .txnr(2)
            .command("syslog")
            .dataLength(SYSLOG_FRAME_DATA.length())
            .data(SYSLOG_FRAME_DATA.getBytes(StandardCharsets.UTF_8))
            .build();

    static final RELPFrame CLOSE_FRAME = new RELPFrame.Builder()
            .txnr(3)
            .command("close")
            .dataLength(0)
            .data(new byte[0])
            .build();

    private RELPEncoder encoder;
    private ResponseCapturingListenRELP proc;
    private TestRunner runner;

    @Before
    public void setup() {
        encoder = new RELPEncoder(StandardCharsets.UTF_8);
        proc = new ResponseCapturingListenRELP();
        runner = TestRunners.newTestRunner(proc);
        runner.setProperty(ListenRELP.PORT, "0");
    }

    @Test
    public void testListenRELP() throws IOException, InterruptedException {
        final List<RELPFrame> frames = new ArrayList<>();
        frames.add(OPEN_FRAME);
        frames.add(SYSLOG_FRAME);
        frames.add(SYSLOG_FRAME);
        frames.add(SYSLOG_FRAME);
        frames.add(CLOSE_FRAME);

        // three syslog frames should be transferred and three responses should be sent
        run(frames, 3, 3, null);

        final List<ProvenanceEventRecord> events = runner.getProvenanceEvents();
        Assert.assertNotNull(events);
        Assert.assertEquals(3, events.size());

        final ProvenanceEventRecord event = events.get(0);
        Assert.assertEquals(ProvenanceEventType.RECEIVE, event.getEventType());
        Assert.assertTrue("transit uri must be set and start with proper protocol", event.getTransitUri().toLowerCase().startsWith("relp"));

        final List<MockFlowFile> mockFlowFiles = runner.getFlowFilesForRelationship(ListenRELP.REL_SUCCESS);
        Assert.assertEquals(3, mockFlowFiles.size());

        final MockFlowFile mockFlowFile = mockFlowFiles.get(0);
        Assert.assertEquals(String.valueOf(SYSLOG_FRAME.getTxnr()), mockFlowFile.getAttribute(ListenRELP.RELPAttributes.TXNR.key()));
        Assert.assertEquals(SYSLOG_FRAME.getCommand(), mockFlowFile.getAttribute(ListenRELP.RELPAttributes.COMMAND.key()));
        Assert.assertTrue(!StringUtils.isBlank(mockFlowFile.getAttribute(ListenRELP.RELPAttributes.PORT.key())));
        Assert.assertTrue(!StringUtils.isBlank(mockFlowFile.getAttribute(ListenRELP.RELPAttributes.SENDER.key())));
    }

    @Test
    public void testBatching() throws IOException, InterruptedException {
        runner.setProperty(ListenRELP.MAX_BATCH_SIZE, "5");

        final List<RELPFrame> frames = new ArrayList<>();
        frames.add(OPEN_FRAME);
        frames.add(SYSLOG_FRAME);
        frames.add(SYSLOG_FRAME);
        frames.add(SYSLOG_FRAME);
        frames.add(CLOSE_FRAME);

        // one syslog frame should be transferred since we are batching, but three responses should be sent
        run(frames, 1, 3, null);

        final List<ProvenanceEventRecord> events = runner.getProvenanceEvents();
        Assert.assertNotNull(events);
        Assert.assertEquals(1, events.size());

        final ProvenanceEventRecord event = events.get(0);
        Assert.assertEquals(ProvenanceEventType.RECEIVE, event.getEventType());
        Assert.assertTrue("transit uri must be set and start with proper protocol", event.getTransitUri().toLowerCase().startsWith("relp"));

        final List<MockFlowFile> mockFlowFiles = runner.getFlowFilesForRelationship(ListenRELP.REL_SUCCESS);
        Assert.assertEquals(1, mockFlowFiles.size());

        final MockFlowFile mockFlowFile = mockFlowFiles.get(0);
        Assert.assertEquals(SYSLOG_FRAME.getCommand(), mockFlowFile.getAttribute(ListenRELP.RELPAttributes.COMMAND.key()));
        Assert.assertTrue(!StringUtils.isBlank(mockFlowFile.getAttribute(ListenRELP.RELPAttributes.PORT.key())));
        Assert.assertTrue(!StringUtils.isBlank(mockFlowFile.getAttribute(ListenRELP.RELPAttributes.SENDER.key())));
    }

    @Test
    public void testTLS() throws InitializationException, IOException, InterruptedException {
        final SSLContextService sslContextService = new StandardSSLContextService();
        runner.addControllerService("ssl-context", sslContextService);
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE, "src/test/resources/truststore.jks");
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_PASSWORD, "passwordpassword");
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_TYPE, "JKS");
        runner.setProperty(sslContextService, StandardSSLContextService.KEYSTORE, "src/test/resources/keystore.jks");
        runner.setProperty(sslContextService, StandardSSLContextService.KEYSTORE_PASSWORD, "passwordpassword");
        runner.setProperty(sslContextService, StandardSSLContextService.KEYSTORE_TYPE, "JKS");
        runner.enableControllerService(sslContextService);

        runner.setProperty(PostHTTP.SSL_CONTEXT_SERVICE, "ssl-context");

        final List<RELPFrame> frames = new ArrayList<>();
        frames.add(OPEN_FRAME);
        frames.add(SYSLOG_FRAME);
        frames.add(SYSLOG_FRAME);
        frames.add(SYSLOG_FRAME);
        frames.add(SYSLOG_FRAME);
        frames.add(SYSLOG_FRAME);
        frames.add(CLOSE_FRAME);

        // three syslog frames should be transferred and three responses should be sent
        run(frames, 5, 5, sslContextService);
    }

    @Test
    public void testNoEventsAvailable() throws IOException, InterruptedException {
        MockListenRELP mockListenRELP = new MockListenRELP(new ArrayList<RELPEvent>());
        runner = TestRunners.newTestRunner(mockListenRELP);
        runner.setProperty(ListenRELP.PORT, "1");

        runner.run();
        runner.assertAllFlowFilesTransferred(ListenRELP.REL_SUCCESS, 0);
    }

    @Test
    public void testBatchingWithDifferentSenders() throws IOException, InterruptedException {
        final String sender1 = "sender1";
        final String sender2 = "sender2";
        final ChannelResponder<SocketChannel> responder = Mockito.mock(ChannelResponder.class);

        final List<RELPEvent> mockEvents = new ArrayList<>();
        mockEvents.add(new RELPEvent(sender1, SYSLOG_FRAME.getData(), responder, SYSLOG_FRAME.getTxnr(), SYSLOG_FRAME.getCommand()));
        mockEvents.add(new RELPEvent(sender1, SYSLOG_FRAME.getData(), responder, SYSLOG_FRAME.getTxnr(), SYSLOG_FRAME.getCommand()));
        mockEvents.add(new RELPEvent(sender2, SYSLOG_FRAME.getData(), responder, SYSLOG_FRAME.getTxnr(), SYSLOG_FRAME.getCommand()));
        mockEvents.add(new RELPEvent(sender2, SYSLOG_FRAME.getData(), responder, SYSLOG_FRAME.getTxnr(), SYSLOG_FRAME.getCommand()));

        MockListenRELP mockListenRELP = new MockListenRELP(mockEvents);
        runner = TestRunners.newTestRunner(mockListenRELP);
        runner.setProperty(ListenRELP.PORT, "1");
        runner.setProperty(ListenRELP.MAX_BATCH_SIZE, "10");

        runner.run();
        runner.assertAllFlowFilesTransferred(ListenRELP.REL_SUCCESS, 2);
    }


    protected void run(final List<RELPFrame> frames, final int expectedTransferred, final int expectedResponses, final SSLContextService sslContextService)
            throws IOException, InterruptedException {

        Socket socket = null;
        try {
            // schedule to start listening on a random port
            final ProcessSessionFactory processSessionFactory = runner.getProcessSessionFactory();
            final ProcessContext context = runner.getProcessContext();
            proc.onScheduled(context);

            // create a client connection to the port the dispatcher is listening on
            final int realPort = proc.getDispatcherPort();

            // create either a regular socket or ssl socket based on context being passed in
            if (sslContextService != null) {
                final SSLContext sslContext = sslContextService.createSSLContext(SSLContextService.ClientAuth.REQUIRED);
                socket = sslContext.getSocketFactory().createSocket("localhost", realPort);
            } else {
                socket = new Socket("localhost", realPort);
            }
            Thread.sleep(100);

            // send the frames to the port the processors is listening on
            sendFrames(frames, socket);

            long responseTimeout = 30000;

            // this first loop waits until the internal queue of the processor has the expected
            // number of messages ready before proceeding, we want to guarantee they are all there
            // before onTrigger gets a chance to run
            long startTimeQueueSizeCheck = System.currentTimeMillis();
            while (proc.getQueueSize() < expectedResponses
                    && (System.currentTimeMillis() - startTimeQueueSizeCheck < responseTimeout)) {
                Thread.sleep(100);
            }

            // want to fail here if the queue size isn't what we expect
            Assert.assertEquals(expectedResponses, proc.getQueueSize());

            // call onTrigger until we got a respond for all the frames, or a certain amount of time passes
            long startTimeProcessing = System.currentTimeMillis();
            while (proc.responses.size() < expectedResponses
                    && (System.currentTimeMillis() - startTimeProcessing < responseTimeout)) {
                proc.onTrigger(context, processSessionFactory);
                Thread.sleep(100);
            }

            // should have gotten a response for each frame
            Assert.assertEquals(expectedResponses, proc.responses.size());

            // should have transferred the expected events
            runner.assertTransferCount(ListenRELP.REL_SUCCESS, expectedTransferred);

        } finally {
            // unschedule to close connections
            proc.onUnscheduled();
            IOUtils.closeQuietly(socket);
        }
    }

    private void sendFrames(final List<RELPFrame> frames, final Socket socket) throws IOException, InterruptedException {
        // send the provided messages
        for (final RELPFrame frame : frames) {
            byte[] encodedFrame = encoder.encode(frame);
            socket.getOutputStream().write(encodedFrame);
        }
        socket.getOutputStream().flush();
    }

    // Extend ListenRELP so we can use the CapturingSocketChannelResponseDispatcher
    private static class ResponseCapturingListenRELP extends ListenRELP {

        private List<RELPResponse> responses = new ArrayList<>();

        @Override
        protected void respond(RELPEvent event, RELPResponse relpResponse) {
            this.responses.add(relpResponse);
            super.respond(event, relpResponse);
        }
    }

    // Extend ListenRELP to mock the ChannelDispatcher and allow us to return staged events
    private static class MockListenRELP extends ListenRELP {

        private List<RELPEvent> mockEvents;

        public MockListenRELP(List<RELPEvent> mockEvents) {
            this.mockEvents = mockEvents;
        }

        @OnScheduled
        @Override
        public void onScheduled(ProcessContext context) throws IOException {
            super.onScheduled(context);
            events.addAll(mockEvents);
        }

        @Override
        protected ChannelDispatcher createDispatcher(ProcessContext context, BlockingQueue<RELPEvent> events) throws IOException {
            return Mockito.mock(ChannelDispatcher.class);
        }

    }

}
