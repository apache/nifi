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
//package org.apache.nifi.processors.beats;
//
//import org.apache.nifi.annotation.lifecycle.OnScheduled;
//import org.apache.nifi.event.transport.EventSender;
//import org.apache.nifi.event.transport.configuration.ShutdownQuietPeriod;
//import org.apache.nifi.event.transport.configuration.ShutdownTimeout;
//import org.apache.nifi.event.transport.configuration.TransportProtocol;
//import org.apache.nifi.event.transport.netty.ByteArrayNettyEventSenderFactory;
//import org.apache.nifi.processor.ProcessContext;
//import org.apache.nifi.processor.util.listen.ListenerProperties;
//import org.apache.nifi.processors.beats.frame.BeatsEncoder;
//import org.apache.nifi.processors.beats.frame.BeatsFrame;
//import org.apache.nifi.processors.beats.netty.BeatsMessage;
//import org.apache.nifi.remote.io.socket.NetworkUtils;
//import org.apache.nifi.ssl.RestrictedSSLContextService;
//import org.apache.nifi.util.TestRunner;
//import org.apache.nifi.util.TestRunners;
//import org.junit.After;
//import org.junit.Before;
//import org.junit.Test;
//import org.junit.runner.RunWith;
//import org.mockito.Mock;
//import org.mockito.junit.MockitoJUnitRunner;
//
//import javax.net.ssl.SSLContext;
//import java.io.ByteArrayOutputStream;
//import java.io.IOException;
//import java.nio.charset.Charset;
//import java.nio.charset.StandardCharsets;
//import java.time.Duration;
//import java.util.ArrayList;
//import java.util.List;
//
//@RunWith(MockitoJUnitRunner.class)
//public class TestListenBeats {
//
//    public static final String OPEN_FRAME_DATA = "relp_version=0\nrelp_software=librelp,1.2.7,http://librelp.adiscon.com\ncommands=syslog";
//    public static final String RELP_FRAME_DATA = "this is a relp message here";
//
//    private static final String LOCALHOST = "localhost";
//    private static final Charset CHARSET = StandardCharsets.US_ASCII;
//    private static final Duration SENDER_TIMEOUT = Duration.ofSeconds(10);
//
////    static final BeatsFrame OPEN_FRAME = new BeatsFrame.Builder()
////            .seqNumber(1)
////            .dataSize(OPEN_FRAME_DATA.length())
////            .payload(OPEN_FRAME_DATA.getBytes(CHARSET))
////            .build();
////
////    static final BeatsFrame RELP_FRAME = new BeatsFrame.Builder()
////            .seqNumber(2)
////            .frameType(BeatsFrame)
////            .dataSize(RELP_FRAME_DATA.length())
////            .payload(RELP_FRAME_DATA.getBytes(CHARSET))
////            .build();
//
////    static final RELPFrame CLOSE_FRAME = new RELPFrame.Builder()
////            .txnr(3)
////            .command("close")
////            .dataLength(0)
////            .data(new byte[0])
////            .build();
//
//    @Mock
//    private RestrictedSSLContextService sslContextService;
//
//    private BeatsEncoder encoder;
//
//    private TestRunner runner;
//
//    @Before
//    public void setup() {
//        encoder = new BeatsEncoder();
//        ListenBeats mockListenBeats = new MockListenRELP();
//        runner = TestRunners.newTestRunner(mockListenBeats);
//    }
//
//    @After
//    public void shutdown() {
//        runner.shutdown();
//    }
//
////    @Test
////    public void testRELPFramesAreReceivedSuccessfully() throws Exception {
////        final int relpFrames = 5;
////        final List<RELPFrame> frames = getFrames(relpFrames);
////
////        // three RELP frames should be transferred
////        run(frames, relpFrames, null);
////
////        final List<ProvenanceEventRecord> events = runner.getProvenanceEvents();
////        Assert.assertNotNull(events);
////        Assert.assertEquals(relpFrames, events.size());
////
////        final ProvenanceEventRecord event = events.get(0);
////        Assert.assertEquals(ProvenanceEventType.RECEIVE, event.getEventType());
////        Assert.assertTrue("transit uri must be set and start with proper protocol", event.getTransitUri().toLowerCase().startsWith("relp"));
////
////        final List<MockFlowFile> mockFlowFiles = runner.getFlowFilesForRelationship(ListenRELP.REL_SUCCESS);
////        Assert.assertEquals(relpFrames, mockFlowFiles.size());
////
////        final MockFlowFile mockFlowFile = mockFlowFiles.get(0);
////        Assert.assertEquals(String.valueOf(RELP_FRAME.getTxnr()), mockFlowFile.getAttribute(ListenRELP.RELPAttributes.TXNR.key()));
////        Assert.assertEquals(RELP_FRAME.getCommand(), mockFlowFile.getAttribute(ListenRELP.RELPAttributes.COMMAND.key()));
////        Assert.assertFalse(StringUtils.isBlank(mockFlowFile.getAttribute(ListenRELP.RELPAttributes.PORT.key())));
////        Assert.assertFalse(StringUtils.isBlank(mockFlowFile.getAttribute(ListenRELP.RELPAttributes.SENDER.key())));
////    }
////
////    @Test
////    public void testRELPFramesAreReceivedSuccessfullyWhenBatched() throws Exception {
////
////        runner.setProperty(ListenerProperties.MAX_BATCH_SIZE, "5");
////
////        final int relpFrames = 3;
////        final List<RELPFrame> frames = getFrames(relpFrames);
////
////        // one relp frame should be transferred since we are batching
////        final int expectedFlowFiles = 1;
////        run(frames, expectedFlowFiles, null);
////
////        final List<ProvenanceEventRecord> events = runner.getProvenanceEvents();
////        Assert.assertNotNull(events);
////        Assert.assertEquals(expectedFlowFiles, events.size());
////
////        final ProvenanceEventRecord event = events.get(0);
////        Assert.assertEquals(ProvenanceEventType.RECEIVE, event.getEventType());
////        Assert.assertTrue("transit uri must be set and start with proper protocol", event.getTransitUri().toLowerCase().startsWith("relp"));
////
////        final List<MockFlowFile> mockFlowFiles = runner.getFlowFilesForRelationship(ListenRELP.REL_SUCCESS);
////        Assert.assertEquals(expectedFlowFiles, mockFlowFiles.size());
////
////        final MockFlowFile mockFlowFile = mockFlowFiles.get(0);
////        Assert.assertEquals(RELP_FRAME.getCommand(), mockFlowFile.getAttribute(ListenRELP.RELPAttributes.COMMAND.key()));
////        Assert.assertFalse(StringUtils.isBlank(mockFlowFile.getAttribute(ListenRELP.RELPAttributes.PORT.key())));
////        Assert.assertFalse(StringUtils.isBlank(mockFlowFile.getAttribute(ListenRELP.RELPAttributes.SENDER.key())));
////    }
////
////    @Test
////    public void testRunMutualTls() throws Exception {
////        final String serviceIdentifier = SSLContextService.class.getName();
////        when(sslContextService.getIdentifier()).thenReturn(serviceIdentifier);
////        final SSLContext sslContext = SslContextUtils.createKeyStoreSslContext();
////        when(sslContextService.createContext()).thenReturn(sslContext);
////        runner.addControllerService(serviceIdentifier, sslContextService);
////        runner.enableControllerService(sslContextService);
////
////        runner.setProperty(ListenRELP.SSL_CONTEXT_SERVICE, serviceIdentifier);
////        runner.setProperty(ListenRELP.CLIENT_AUTH, ClientAuth.NONE.name());
////
////        final int relpFrames = 3;
////        final List<RELPFrame> frames = getFrames(relpFrames);
////        run(frames, relpFrames, sslContext);
////    }
////
////    @Test
////    public void testBatchingWithDifferentSenders() {
////        String sender1 = "/192.168.1.50:55000";
////        String sender2 = "/192.168.1.50:55001";
////        String sender3 = "/192.168.1.50:55002";
////
////        final List<BeatsMessage> mockEvents = new ArrayList<>();
////        mockEvents.add(new BeatsMessage(sender1, RELP_FRAME.getData(), RELP_FRAME.getTxnr(), RELP_FRAME.getCommand()));
////        mockEvents.add(new BeatsMessage(sender1, RELP_FRAME.getData(), RELP_FRAME.getTxnr(), RELP_FRAME.getCommand()));
////        mockEvents.add(new BeatsMessage(sender1, RELP_FRAME.getData(), RELP_FRAME.getTxnr(), RELP_FRAME.getCommand()));
////        mockEvents.add(new BeatsMessage(sender2, RELP_FRAME.getData(), RELP_FRAME.getTxnr(), RELP_FRAME.getCommand()));
////        mockEvents.add(new BeatsMessage(sender3, RELP_FRAME.getData(), RELP_FRAME.getTxnr(), RELP_FRAME.getCommand()));
////        mockEvents.add(new BeatsMessage(sender3, RELP_FRAME.getData(), RELP_FRAME.getTxnr(), RELP_FRAME.getCommand()));
////
////        MockListenRELP mockListenRELP = new MockListenRELP(mockEvents);
////        runner = TestRunners.newTestRunner(mockListenRELP);
////        runner.setProperty(AbstractListenEventBatchingProcessor.PORT, Integer.toString(NetworkUtils.availablePort()));
////        runner.setProperty(ListenerProperties.MAX_BATCH_SIZE, "10");
////
////        runner.run();
////        runner.assertAllFlowFilesTransferred(ListenBeats.REL_SUCCESS, 3);
////        runner.shutdown();
////    }
//
////        private void run(final List<BeatsFrame> frames, final int flowFiles, final SSLContext sslContext) throws Exception {
//    private void run(final String message, final int flowFiles, final SSLContext sslContext) throws Exception {
//        final int port = NetworkUtils.availablePort();
//        runner.setProperty(ListenerProperties.PORT, Integer.toString(port));
//        // Run Processor and start Dispatcher without shutting down
//        runner.run(1, false, true);
//        final byte[] beatsMessages = message.getBytes(StandardCharsets.UTF_8);
//        sendMessages(port, beatsMessages, sslContext);
//        runner.run(flowFiles, false, false);
//        runner.assertTransferCount(ListenBeats.REL_SUCCESS, flowFiles);
//    }
//
//    private byte[] getRELPMessages(final List<BeatsFrame> frames) throws IOException {
//
//        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
//        for (final BeatsFrame frame : frames) {
//            final byte[] encodedFrame = encoder.encode(frame);
//            outputStream.write(encodedFrame);
//            outputStream.flush();
//        }
//
//        return outputStream.toByteArray();
//    }
//
////    private List<BeatsFrame> getFrames(final int relpFrames) {
////        final List<BeatsFrame> frames = new ArrayList<>();
////        frames.add(OPEN_FRAME);
////
////        for (int i = 0; i < relpFrames; i++) {
////            frames.add(RELP_FRAME);
////        }
////
////        frames.add(CLOSE_FRAME);
////        return frames;
////    }
//
//    private void sendMessages(final int port, final byte[] relpMessages, final SSLContext sslContext) throws Exception {
//        final ByteArrayNettyEventSenderFactory eventSenderFactory = new ByteArrayNettyEventSenderFactory(runner.getLogger(), LOCALHOST, port, TransportProtocol.TCP);
//        eventSenderFactory.setShutdownQuietPeriod(ShutdownQuietPeriod.QUICK.getDuration());
//        eventSenderFactory.setShutdownTimeout(ShutdownTimeout.QUICK.getDuration());
//        if (sslContext != null) {
//            eventSenderFactory.setSslContext(sslContext);
//        }
//
//        eventSenderFactory.setTimeout(SENDER_TIMEOUT);
//        try (final EventSender<byte[]> eventSender = eventSenderFactory.getEventSender()) {
//            eventSender.sendEvent(relpMessages);
//        }
//    }
//
//    private static class MockListenRELP extends ListenBeats {
//        private final List<BeatsMessage> mockEvents;
//
//        public MockListenRELP() {
//            this.mockEvents = new ArrayList<>();
//        }
//
//        public MockListenRELP(List<BeatsMessage> mockEvents) {
//            this.mockEvents = mockEvents;
//        }
//
//        @OnScheduled
//        @Override
//        public void onScheduled(ProcessContext context) throws IOException {
//            super.onScheduled(context);
//            events.addAll(mockEvents);
//        }
//    }
//
//    @Test
//    public void testSendBeatMessage() throws Exception {
//        final String multipleJsonFrame = "3243000000E27801CC91414BC3401085477FCA3B6F" +
//                "93EEB6A5B8A71E3CF5ECC98BECC6491AC86643665290903FAE17A982540F8237E7F" +
//                "80D3C78EF734722BA21A2B71987C41A9E8306F819FA32303CBADCC020725078D46D" +
//                "C791836231D0EB7FDB0F933EE9354A2C129A4B44F8B8AF94197D4817CE7CCF67189" +
//                "CB2E80F74E651DADCC36357D8C2623138689B5834A4011E6E6DF7ABB55DAD770F76" +
//                "E3B7777EBB299CB58F30903C8D15C3A33CE5C465A8A74ACA2E3792A7B1E25259B4E" +
//                "87203835CD7C20ABF5FDC91886E89E8F58F237CEEF2EF1A5967BEFBFBD54F8C3162" +
//                "790F0000FFFF6CB6A08D";
//
//        final List<String> messages = new ArrayList<>();
//        messages.add(multipleJsonFrame);
//
//        final int port = NetworkUtils.availablePort();
//        run(multipleJsonFrame, 1, null);
//    }
//
//}