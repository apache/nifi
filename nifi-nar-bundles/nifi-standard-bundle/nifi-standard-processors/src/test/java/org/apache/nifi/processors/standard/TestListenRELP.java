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

import org.apache.commons.lang3.StringUtils;
import org.apache.ftpserver.ssl.ClientAuth;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.event.transport.EventSender;
import org.apache.nifi.event.transport.configuration.ShutdownQuietPeriod;
import org.apache.nifi.event.transport.configuration.ShutdownTimeout;
import org.apache.nifi.event.transport.configuration.TransportProtocol;
import org.apache.nifi.event.transport.netty.ByteArrayNettyEventSenderFactory;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.listen.ListenerProperties;
import org.apache.nifi.processors.standard.relp.event.RELPMessage;
import org.apache.nifi.processors.standard.relp.frame.RELPEncoder;
import org.apache.nifi.processors.standard.relp.frame.RELPFrame;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.ssl.RestrictedSSLContextService;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.web.util.ssl.SslContextUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.net.ssl.SSLContext;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestListenRELP {

    public static final String OPEN_FRAME_DATA = "relp_version=0\nrelp_software=librelp,1.2.7,http://librelp.adiscon.com\ncommands=syslog";
    public static final String RELP_FRAME_DATA = "this is a relp message here";

    private static final String LOCALHOST = "localhost";
    private static final Charset CHARSET = StandardCharsets.US_ASCII;
    private static final Duration SENDER_TIMEOUT = Duration.ofSeconds(10);

    static final RELPFrame OPEN_FRAME = new RELPFrame.Builder()
            .txnr(1)
            .command("open")
            .dataLength(OPEN_FRAME_DATA.length())
            .data(OPEN_FRAME_DATA.getBytes(CHARSET))
            .build();

    static final RELPFrame RELP_FRAME = new RELPFrame.Builder()
            .txnr(2)
            .command("syslog")
            .dataLength(RELP_FRAME_DATA.length())
            .data(RELP_FRAME_DATA.getBytes(CHARSET))
            .build();

    static final RELPFrame CLOSE_FRAME = new RELPFrame.Builder()
            .txnr(3)
            .command("close")
            .dataLength(0)
            .data(new byte[0])
            .build();

    @Mock
    private RestrictedSSLContextService sslContextService;

    private RELPEncoder encoder;

    private TestRunner runner;

    @BeforeEach
    public void setup() {
        encoder = new RELPEncoder(CHARSET);
        ListenRELP mockRELP = new MockListenRELP();
        runner = TestRunners.newTestRunner(mockRELP);
    }

    @AfterEach
    public void shutdown() {
        runner.shutdown();
    }

    @Test
    public void testRELPFramesAreReceivedSuccessfully() throws Exception {
        final int relpFrames = 5;
        final List<RELPFrame> frames = getFrames(relpFrames);

        // three RELP frames should be transferred
        run(frames, relpFrames, null);

        final List<ProvenanceEventRecord> events = runner.getProvenanceEvents();
        assertNotNull(events);
        assertEquals(relpFrames, events.size());

        final ProvenanceEventRecord event = events.get(0);
        assertEquals(ProvenanceEventType.RECEIVE, event.getEventType());
        assertTrue(event.getTransitUri().toLowerCase().startsWith("relp"), "transit uri must be set and start with proper protocol");

        final List<MockFlowFile> mockFlowFiles = runner.getFlowFilesForRelationship(ListenRELP.REL_SUCCESS);
        assertEquals(relpFrames, mockFlowFiles.size());

        final MockFlowFile mockFlowFile = mockFlowFiles.get(0);
        assertEquals(String.valueOf(RELP_FRAME.getTxnr()), mockFlowFile.getAttribute(ListenRELP.RELPAttributes.TXNR.key()));
        assertEquals(RELP_FRAME.getCommand(), mockFlowFile.getAttribute(ListenRELP.RELPAttributes.COMMAND.key()));
        assertFalse(StringUtils.isBlank(mockFlowFile.getAttribute(ListenRELP.RELPAttributes.PORT.key())));
        assertFalse(StringUtils.isBlank(mockFlowFile.getAttribute(ListenRELP.RELPAttributes.SENDER.key())));
    }

    @Test
    public void testRELPFramesAreReceivedSuccessfullyWhenBatched() throws Exception {

        runner.setProperty(ListenerProperties.MAX_BATCH_SIZE, "5");

        final int relpFrames = 3;
        final List<RELPFrame> frames = getFrames(relpFrames);

        // one relp frame should be transferred since we are batching
        final int expectedFlowFiles = 1;
        run(frames, expectedFlowFiles, null);

        final List<ProvenanceEventRecord> events = runner.getProvenanceEvents();
        assertNotNull(events);
        assertEquals(expectedFlowFiles, events.size());

        final ProvenanceEventRecord event = events.get(0);
        assertEquals(ProvenanceEventType.RECEIVE, event.getEventType());
        assertTrue(event.getTransitUri().toLowerCase().startsWith("relp"), "transit uri must be set and start with proper protocol");

        final List<MockFlowFile> mockFlowFiles = runner.getFlowFilesForRelationship(ListenRELP.REL_SUCCESS);
        assertEquals(expectedFlowFiles, mockFlowFiles.size());

        final MockFlowFile mockFlowFile = mockFlowFiles.get(0);
        assertEquals(RELP_FRAME.getCommand(), mockFlowFile.getAttribute(ListenRELP.RELPAttributes.COMMAND.key()));
        assertFalse(StringUtils.isBlank(mockFlowFile.getAttribute(ListenRELP.RELPAttributes.PORT.key())));
        assertFalse(StringUtils.isBlank(mockFlowFile.getAttribute(ListenRELP.RELPAttributes.SENDER.key())));
    }

    @Test
    public void testRunMutualTls() throws Exception {
        final String serviceIdentifier = SSLContextService.class.getName();
        when(sslContextService.getIdentifier()).thenReturn(serviceIdentifier);
        final SSLContext sslContext = SslContextUtils.createKeyStoreSslContext();
        when(sslContextService.createContext()).thenReturn(sslContext);
        runner.addControllerService(serviceIdentifier, sslContextService);
        runner.enableControllerService(sslContextService);

        runner.setProperty(ListenRELP.SSL_CONTEXT_SERVICE, serviceIdentifier);
        runner.setProperty(ListenRELP.CLIENT_AUTH, ClientAuth.NONE.name());

        final int relpFrames = 3;
        final List<RELPFrame> frames = getFrames(relpFrames);
        run(frames, relpFrames, sslContext);
    }

    @Test
    public void testBatchingWithDifferentSenders() {
        String sender1 = "/192.168.1.50:55000";
        String sender2 = "/192.168.1.50:55001";
        String sender3 = "/192.168.1.50:55002";

        final List<RELPMessage> mockEvents = new ArrayList<>();
        mockEvents.add(new RELPMessage(sender1, RELP_FRAME.getData(), RELP_FRAME.getTxnr(), RELP_FRAME.getCommand()));
        mockEvents.add(new RELPMessage(sender1, RELP_FRAME.getData(), RELP_FRAME.getTxnr(), RELP_FRAME.getCommand()));
        mockEvents.add(new RELPMessage(sender1, RELP_FRAME.getData(), RELP_FRAME.getTxnr(), RELP_FRAME.getCommand()));
        mockEvents.add(new RELPMessage(sender2, RELP_FRAME.getData(), RELP_FRAME.getTxnr(), RELP_FRAME.getCommand()));
        mockEvents.add(new RELPMessage(sender3, RELP_FRAME.getData(), RELP_FRAME.getTxnr(), RELP_FRAME.getCommand()));
        mockEvents.add(new RELPMessage(sender3, RELP_FRAME.getData(), RELP_FRAME.getTxnr(), RELP_FRAME.getCommand()));

        MockListenRELP mockListenRELP = new MockListenRELP(mockEvents);
        runner = TestRunners.newTestRunner(mockListenRELP);
        runner.setProperty(ListenerProperties.PORT, "0");
        runner.setProperty(ListenerProperties.MAX_BATCH_SIZE, "10");

        runner.run();
        runner.assertAllFlowFilesTransferred(ListenRELP.REL_SUCCESS, 3);
        runner.shutdown();
    }

    private void run(final List<RELPFrame> frames, final int flowFiles, final SSLContext sslContext) throws Exception {
        runner.setProperty(ListenerProperties.PORT, "0");
        runner.run(1, false, true);

        final int port = ((ListenRELP) runner.getProcessor()).getListeningPort();

        // Run Processor and start Dispatcher without shutting down
        final byte[] relpMessages = getRELPMessages(frames);
        sendMessages(port, relpMessages, sslContext);
        runner.run(flowFiles, false, false);
        runner.assertTransferCount(ListenRELP.REL_SUCCESS, flowFiles);
    }

    private byte[] getRELPMessages(final List<RELPFrame> frames) throws IOException {

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        for (final RELPFrame frame : frames) {
            final byte[] encodedFrame = encoder.encode(frame);
            outputStream.write(encodedFrame);
            outputStream.flush();
        }

        return outputStream.toByteArray();
    }

    private List<RELPFrame> getFrames(final int relpFrames) {
        final List<RELPFrame> frames = new ArrayList<>();
        frames.add(OPEN_FRAME);

        for (int i = 0; i < relpFrames; i++) {
            frames.add(RELP_FRAME);
        }

        frames.add(CLOSE_FRAME);
        return frames;
    }

    private void sendMessages(final int port, final byte[] relpMessages, final SSLContext sslContext) throws Exception {
        final ByteArrayNettyEventSenderFactory eventSenderFactory = new ByteArrayNettyEventSenderFactory(runner.getLogger(), LOCALHOST, port, TransportProtocol.TCP);
        eventSenderFactory.setShutdownQuietPeriod(ShutdownQuietPeriod.QUICK.getDuration());
        eventSenderFactory.setShutdownTimeout(ShutdownTimeout.QUICK.getDuration());
        if (sslContext != null) {
            eventSenderFactory.setSslContext(sslContext);
        }

        eventSenderFactory.setTimeout(SENDER_TIMEOUT);
        try (final EventSender<byte[]> eventSender = eventSenderFactory.getEventSender()) {
            eventSender.sendEvent(relpMessages);
        }
    }

    private static class MockListenRELP extends ListenRELP {
        private final List<RELPMessage> mockEvents;

        public MockListenRELP() {
            this.mockEvents = new ArrayList<>();
        }

        public MockListenRELP(List<RELPMessage> mockEvents) {
            this.mockEvents = mockEvents;
        }

        @OnScheduled
        @Override
        public void onScheduled(ProcessContext context) throws IOException {
            super.onScheduled(context);
            events.addAll(mockEvents);
        }
    }
}
