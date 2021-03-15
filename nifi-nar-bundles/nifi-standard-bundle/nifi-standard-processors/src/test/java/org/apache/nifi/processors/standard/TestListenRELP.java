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

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import javax.net.ssl.SSLContext;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.listen.dispatcher.ChannelDispatcher;
import org.apache.nifi.processor.util.listen.response.ChannelResponder;
import org.apache.nifi.processors.standard.relp.event.RELPEvent;
import org.apache.nifi.processors.standard.relp.frame.RELPEncoder;
import org.apache.nifi.processors.standard.relp.frame.RELPFrame;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.remote.io.socket.NetworkUtils;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.security.util.TlsException;
import org.apache.nifi.ssl.RestrictedSSLContextService;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.web.util.ssl.SslContextUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
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

    private static final String LOCALHOST = "localhost";

    @Mock
    private ChannelResponder<SocketChannel> responder;

    @Mock
    private ChannelDispatcher channelDispatcher;

    @Mock
    private RestrictedSSLContextService sslContextService;

    private RELPEncoder encoder;

    private TestRunner runner;

    @Before
    public void setup() {
        encoder = new RELPEncoder(StandardCharsets.UTF_8);
        runner = TestRunners.newTestRunner(ListenRELP.class);
    }

    @Test
    public void testRun() throws IOException {
        final int syslogFrames = 5;
        final List<RELPFrame> frames = getFrames(syslogFrames);

        // three syslog frames should be transferred and three responses should be sent
        run(frames, syslogFrames, syslogFrames, null);

        final List<ProvenanceEventRecord> events = runner.getProvenanceEvents();
        Assert.assertNotNull(events);
        Assert.assertEquals(syslogFrames, events.size());

        final ProvenanceEventRecord event = events.get(0);
        Assert.assertEquals(ProvenanceEventType.RECEIVE, event.getEventType());
        Assert.assertTrue("transit uri must be set and start with proper protocol", event.getTransitUri().toLowerCase().startsWith("relp"));

        final List<MockFlowFile> mockFlowFiles = runner.getFlowFilesForRelationship(ListenRELP.REL_SUCCESS);
        Assert.assertEquals(syslogFrames, mockFlowFiles.size());

        final MockFlowFile mockFlowFile = mockFlowFiles.get(0);
        Assert.assertEquals(String.valueOf(SYSLOG_FRAME.getTxnr()), mockFlowFile.getAttribute(ListenRELP.RELPAttributes.TXNR.key()));
        Assert.assertEquals(SYSLOG_FRAME.getCommand(), mockFlowFile.getAttribute(ListenRELP.RELPAttributes.COMMAND.key()));
        Assert.assertFalse(StringUtils.isBlank(mockFlowFile.getAttribute(ListenRELP.RELPAttributes.PORT.key())));
        Assert.assertFalse(StringUtils.isBlank(mockFlowFile.getAttribute(ListenRELP.RELPAttributes.SENDER.key())));
    }

    @Test
    public void testRunBatching() throws IOException {
        runner.setProperty(ListenRELP.MAX_BATCH_SIZE, "5");

        final int syslogFrames = 3;
        final List<RELPFrame> frames = getFrames(syslogFrames);

        // one syslog frame should be transferred since we are batching, but three responses should be sent
        final int expectedFlowFiles = 1;
        run(frames, expectedFlowFiles, syslogFrames, null);

        final List<ProvenanceEventRecord> events = runner.getProvenanceEvents();
        Assert.assertNotNull(events);
        Assert.assertEquals(expectedFlowFiles, events.size());

        final ProvenanceEventRecord event = events.get(0);
        Assert.assertEquals(ProvenanceEventType.RECEIVE, event.getEventType());
        Assert.assertTrue("transit uri must be set and start with proper protocol", event.getTransitUri().toLowerCase().startsWith("relp"));

        final List<MockFlowFile> mockFlowFiles = runner.getFlowFilesForRelationship(ListenRELP.REL_SUCCESS);
        Assert.assertEquals(expectedFlowFiles, mockFlowFiles.size());

        final MockFlowFile mockFlowFile = mockFlowFiles.get(0);
        Assert.assertEquals(SYSLOG_FRAME.getCommand(), mockFlowFile.getAttribute(ListenRELP.RELPAttributes.COMMAND.key()));
        Assert.assertFalse(StringUtils.isBlank(mockFlowFile.getAttribute(ListenRELP.RELPAttributes.PORT.key())));
        Assert.assertFalse(StringUtils.isBlank(mockFlowFile.getAttribute(ListenRELP.RELPAttributes.SENDER.key())));
    }

    @Test
    public void testRunMutualTls() throws IOException, TlsException, InitializationException {
        final String serviceIdentifier = SSLContextService.class.getName();
        Mockito.when(sslContextService.getIdentifier()).thenReturn(serviceIdentifier);
        final SSLContext sslContext = SslContextUtils.createKeyStoreSslContext();
        Mockito.when(sslContextService.createContext()).thenReturn(sslContext);
        runner.addControllerService(serviceIdentifier, sslContextService);
        runner.enableControllerService(sslContextService);

        runner.setProperty(ListenRELP.SSL_CONTEXT_SERVICE, serviceIdentifier);

        final int syslogFrames = 3;
        final List<RELPFrame> frames = getFrames(syslogFrames);
        run(frames, syslogFrames, syslogFrames, sslContext);
    }

    @Test
    public void testRunNoEventsAvailable() {
        MockListenRELP mockListenRELP = new MockListenRELP(new ArrayList<>());
        runner = TestRunners.newTestRunner(mockListenRELP);
        runner.setProperty(ListenRELP.PORT, Integer.toString(NetworkUtils.availablePort()));

        runner.run();
        runner.assertAllFlowFilesTransferred(ListenRELP.REL_SUCCESS, 0);
        runner.shutdown();
    }

    @Test
    public void testBatchingWithDifferentSenders() {
        final String sender1 = "sender1";
        final String sender2 = "sender2";

        final List<RELPEvent> mockEvents = new ArrayList<>();
        mockEvents.add(new RELPEvent(sender1, SYSLOG_FRAME.getData(), responder, SYSLOG_FRAME.getTxnr(), SYSLOG_FRAME.getCommand()));
        mockEvents.add(new RELPEvent(sender1, SYSLOG_FRAME.getData(), responder, SYSLOG_FRAME.getTxnr(), SYSLOG_FRAME.getCommand()));
        mockEvents.add(new RELPEvent(sender2, SYSLOG_FRAME.getData(), responder, SYSLOG_FRAME.getTxnr(), SYSLOG_FRAME.getCommand()));
        mockEvents.add(new RELPEvent(sender2, SYSLOG_FRAME.getData(), responder, SYSLOG_FRAME.getTxnr(), SYSLOG_FRAME.getCommand()));

        MockListenRELP mockListenRELP = new MockListenRELP(mockEvents);
        runner = TestRunners.newTestRunner(mockListenRELP);
        runner.setProperty(ListenRELP.PORT, Integer.toString(NetworkUtils.availablePort()));
        runner.setProperty(ListenRELP.MAX_BATCH_SIZE, "10");

        runner.run();
        runner.assertAllFlowFilesTransferred(ListenRELP.REL_SUCCESS, 2);
        runner.shutdown();
    }

    private void run(final List<RELPFrame> frames, final int flowFiles, final int responses, final SSLContext sslContext)
            throws IOException {

        final int port = NetworkUtils.availablePort();
        runner.setProperty(ListenRELP.PORT, Integer.toString(port));

        // Run Processor and start Dispatcher without shutting down
        runner.run(1, false, true);

        try (final Socket socket = getSocket(port, sslContext)) {
            final OutputStream outputStream = socket.getOutputStream();
            sendFrames(frames, outputStream);

            // Run Processor for number of responses
            runner.run(responses, false, false);

            runner.assertTransferCount(ListenRELP.REL_SUCCESS, flowFiles);
        } finally {
            runner.shutdown();
        }
    }

    private void sendFrames(final List<RELPFrame> frames, final OutputStream outputStream) throws IOException {
        for (final RELPFrame frame : frames) {
            final byte[] encodedFrame = encoder.encode(frame);
            outputStream.write(encodedFrame);
            outputStream.flush();
        }
    }

    private Socket getSocket(final int port, final SSLContext sslContext) throws IOException {
        final Socket socket;
        if (sslContext == null) {
            socket = new Socket(LOCALHOST, port);
        } else {
            socket = sslContext.getSocketFactory().createSocket(LOCALHOST, port);
        }
        return socket;
    }

    private List<RELPFrame> getFrames(final int syslogFrames) {
        final List<RELPFrame> frames = new ArrayList<>();
        frames.add(OPEN_FRAME);

        for (int i = 0; i < syslogFrames; i++) {
            frames.add(SYSLOG_FRAME);
        }

        frames.add(CLOSE_FRAME);
        return frames;
    }

    // Extend ListenRELP to mock the ChannelDispatcher and allow us to return staged events
    private class MockListenRELP extends ListenRELP {

        private final List<RELPEvent> mockEvents;

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
        protected ChannelDispatcher createDispatcher(ProcessContext context, BlockingQueue<RELPEvent> events) {
            return channelDispatcher;
        }

    }

}
