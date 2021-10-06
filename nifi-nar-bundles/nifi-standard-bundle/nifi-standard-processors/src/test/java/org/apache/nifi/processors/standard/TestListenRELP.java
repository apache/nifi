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
import org.apache.nifi.event.transport.configuration.TransportProtocol;
import org.apache.nifi.event.transport.netty.ByteArrayNettyEventSenderFactory;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.listen.AbstractListenEventBatchingProcessor;
import org.apache.nifi.processors.standard.relp.event.RELPNettyEvent;
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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import javax.net.ssl.SSLContext;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestListenRELP {

    public static final String OPEN_FRAME_DATA = "relp_version=0\nrelp_software=librelp,1.2.7,http://librelp.adiscon.com\ncommands=syslog";
    public static final String SYSLOG_FRAME_DATA = "this is a syslog message here";

    private static final String LOCALHOST = "localhost";
    private static final Charset CHARSET = StandardCharsets.US_ASCII;
    private static final Duration SENDER_TIMEOUT = Duration.ofSeconds(30);

    static final RELPFrame OPEN_FRAME = new RELPFrame.Builder()
            .txnr(1)
            .command("open")
            .dataLength(OPEN_FRAME_DATA.length())
            .data(OPEN_FRAME_DATA.getBytes(CHARSET))
            .build();

    static final RELPFrame SYSLOG_FRAME = new RELPFrame.Builder()
            .txnr(2)
            .command("syslog")
            .dataLength(SYSLOG_FRAME_DATA.length())
            .data(SYSLOG_FRAME_DATA.getBytes(CHARSET))
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

    @Before
    public void setup() {
        encoder = new RELPEncoder(CHARSET);
        ListenRELP mockRELP = new MockListenRELP();
        runner = TestRunners.newTestRunner(mockRELP);
    }

    @After
    public void shutdown() {
        runner.shutdown();
    }

    @Test
    public void testRELPFramesAreReceivedSuccessfully() throws IOException {
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
    public void testRELPFramesAreReceivedSuccessfullyWhenBatched() throws IOException {

        runner.setProperty(AbstractListenEventBatchingProcessor.MAX_BATCH_SIZE, "5");

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
        when(sslContextService.getIdentifier()).thenReturn(serviceIdentifier);
        final SSLContext sslContext = SslContextUtils.createKeyStoreSslContext();
        when(sslContextService.createContext()).thenReturn(sslContext);
        runner.addControllerService(serviceIdentifier, sslContextService);
        runner.enableControllerService(sslContextService);

        runner.setProperty(ListenRELP.SSL_CONTEXT_SERVICE, serviceIdentifier);
        runner.setProperty(ListenRELP.CLIENT_AUTH, ClientAuth.NONE.name());

        final int syslogFrames = 3;
        final List<RELPFrame> frames = getFrames(syslogFrames);
        run(frames, syslogFrames, syslogFrames, sslContext);
    }

    @Test
    public void testBatchingWithDifferentSenders() {
        String sender1 = "/192.168.1.50:55000";
        String sender2 = "/192.168.1.50:55001";
        String sender3 = "/192.168.1.50:55002";

        final List<RELPNettyEvent> mockEvents = new ArrayList<>();
        mockEvents.add(new RELPNettyEvent(sender1, SYSLOG_FRAME.getData(), SYSLOG_FRAME.getTxnr(), SYSLOG_FRAME.getCommand()));
        mockEvents.add(new RELPNettyEvent(sender1, SYSLOG_FRAME.getData(), SYSLOG_FRAME.getTxnr(), SYSLOG_FRAME.getCommand()));
        mockEvents.add(new RELPNettyEvent(sender1, SYSLOG_FRAME.getData(), SYSLOG_FRAME.getTxnr(), SYSLOG_FRAME.getCommand()));
        mockEvents.add(new RELPNettyEvent(sender2, SYSLOG_FRAME.getData(), SYSLOG_FRAME.getTxnr(), SYSLOG_FRAME.getCommand()));
        mockEvents.add(new RELPNettyEvent(sender3, SYSLOG_FRAME.getData(), SYSLOG_FRAME.getTxnr(), SYSLOG_FRAME.getCommand()));
        mockEvents.add(new RELPNettyEvent(sender3, SYSLOG_FRAME.getData(), SYSLOG_FRAME.getTxnr(), SYSLOG_FRAME.getCommand()));

        MockListenRELP mockListenRELP = new MockListenRELP(mockEvents);
        runner = TestRunners.newTestRunner(mockListenRELP);
        runner.setProperty(AbstractListenEventBatchingProcessor.PORT, Integer.toString(NetworkUtils.availablePort()));
        runner.setProperty(AbstractListenEventBatchingProcessor.MAX_BATCH_SIZE, "10");

        runner.run();
        runner.assertAllFlowFilesTransferred(ListenRELP.REL_SUCCESS, 3);
        runner.shutdown();
    }

    private void run(final List<RELPFrame> frames, final int flowFiles, final int responses, final SSLContext sslContext)
            throws IOException {

        final int port = NetworkUtils.availablePort();
        runner.setProperty(AbstractListenEventBatchingProcessor.PORT, Integer.toString(port));

        // Run Processor and start Dispatcher without shutting down
        runner.run(1, false, true);
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

    private List<RELPFrame> getFrames(final int syslogFrames) {
        final List<RELPFrame> frames = new ArrayList<>();
        frames.add(OPEN_FRAME);

        for (int i = 0; i < syslogFrames; i++) {
            frames.add(SYSLOG_FRAME);
        }

        frames.add(CLOSE_FRAME);
        return frames;
    }

    private void sendMessages(final int port, final byte[] relpMessages, final SSLContext sslContext) {
        final ByteArrayNettyEventSenderFactory eventSenderFactory = new ByteArrayNettyEventSenderFactory(runner.getLogger(), LOCALHOST, port, TransportProtocol.TCP);
        if (sslContext != null) {
            eventSenderFactory.setSslContext(sslContext);
        }

        eventSenderFactory.setTimeout(SENDER_TIMEOUT);
        EventSender<byte[]> eventSender = eventSenderFactory.getEventSender();
        eventSender.sendEvent(relpMessages);
    }

    private class MockListenRELP extends ListenRELP {
        private final List<RELPNettyEvent> mockEvents;

        public MockListenRELP() {
            this.mockEvents = new ArrayList<>();
        }

        public MockListenRELP(List<RELPNettyEvent> mockEvents) {
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
