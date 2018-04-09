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

import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.util.put.sender.ChannelSender;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestPutSyslog {

    private MockCollectingSender sender;
    private MockPutSyslog proc;
    private TestRunner runner;

    @Before
    public void setup() throws IOException {
        sender = new MockCollectingSender();
        proc = new MockPutSyslog(sender);
        runner = TestRunners.newTestRunner(proc);
        runner.setProperty(PutSyslog.HOSTNAME, "localhost");
        runner.setProperty(PutSyslog.PORT, "12345");
    }

    @Test
    public void testValidMessageStaticPropertiesUdp() {
        final String pri = "34";
        final String version = "1";
        final String stamp = "2003-10-11T22:14:15.003Z";
        final String host = "mymachine.example.com";
        final String body = "su - ID47 - BOM'su root' failed for lonvick on /dev/pts/8";

        final String expectedMessage = "<" + pri + ">" + version + " " + stamp + " " + host + " " + body;

        runner.setProperty(PutSyslog.MSG_PRIORITY, pri);
        runner.setProperty(PutSyslog.MSG_VERSION, version);
        runner.setProperty(PutSyslog.MSG_TIMESTAMP, stamp);
        runner.setProperty(PutSyslog.MSG_HOSTNAME, host);
        runner.setProperty(PutSyslog.MSG_BODY, body);

        runner.enqueue("incoming data".getBytes(Charset.forName("UTF-8")));
        runner.run();

        runner.assertAllFlowFilesTransferred(PutSyslog.REL_SUCCESS, 1);
        Assert.assertEquals(1, sender.messages.size());
        Assert.assertEquals(expectedMessage, sender.messages.get(0));

        final List<ProvenanceEventRecord> events = runner.getProvenanceEvents();
        Assert.assertNotNull(events);
        Assert.assertEquals(1, events.size());

        final ProvenanceEventRecord event = events.get(0);
        Assert.assertEquals(ProvenanceEventType.SEND, event.getEventType());
        Assert.assertEquals("UDP://localhost:12345", event.getTransitUri());
    }

    @Test
    public void testValidMessageStaticPropertiesTcp() {
        final String pri = "34";
        final String version = "1";
        final String stamp = "2003-10-11T22:14:15.003Z";
        final String host = "mymachine.example.com";
        final String body = "su - ID47 - BOM'su root' failed for lonvick on /dev/pts/8";

        final String expectedMessage = "<" + pri + ">" + version + " " + stamp + " " + host + " " + body;

        runner.setProperty(PutSyslog.PROTOCOL, PutSyslog.TCP_VALUE);
        runner.setProperty(PutSyslog.MSG_PRIORITY, pri);
        runner.setProperty(PutSyslog.MSG_VERSION, version);
        runner.setProperty(PutSyslog.MSG_TIMESTAMP, stamp);
        runner.setProperty(PutSyslog.MSG_HOSTNAME, host);
        runner.setProperty(PutSyslog.MSG_BODY, body);

        runner.enqueue("incoming data".getBytes(Charset.forName("UTF-8")));
        runner.run();

        runner.assertAllFlowFilesTransferred(PutSyslog.REL_SUCCESS, 1);
        Assert.assertEquals(1, sender.messages.size());
        Assert.assertEquals(expectedMessage, sender.messages.get(0).replace("\n", ""));

        final List<ProvenanceEventRecord> events = runner.getProvenanceEvents();
        Assert.assertNotNull(events);
        Assert.assertEquals(1, events.size());

        final ProvenanceEventRecord event = events.get(0);
        Assert.assertEquals(ProvenanceEventType.SEND, event.getEventType());
        Assert.assertEquals("TCP://localhost:12345", event.getTransitUri());
    }

    @Test
    public void testValidELPropertiesTcp() {
        final String pri = "34";
        final String version = "1";
        final String stamp = "2003-10-11T22:14:15.003Z";
        final String host = "mymachine.example.com";
        final String body = "su - ID47 - BOM'su root' failed for lonvick on /dev/pts/8";

        final String expectedMessage = "<" + pri + ">" + version + " " + stamp + " " + host + " " + body;

        runner.setProperty(PutSyslog.HOSTNAME, "${'hostname'}");
        runner.setProperty(PutSyslog.PORT, "${port}");
        runner.setProperty(PutSyslog.CHARSET, "${charset}");
        runner.setProperty(PutSyslog.TIMEOUT, "${timeout}");
        runner.setProperty(PutSyslog.MAX_SOCKET_SEND_BUFFER_SIZE, "${maxSocketSenderBufferSize}");
        runner.setProperty(PutSyslog.IDLE_EXPIRATION, "${idleExpiration}");
        runner.setProperty(PutSyslog.PROTOCOL, PutSyslog.TCP_VALUE);
        runner.setProperty(PutSyslog.MSG_PRIORITY, pri);
        runner.setProperty(PutSyslog.MSG_VERSION, version);
        runner.setProperty(PutSyslog.MSG_TIMESTAMP, stamp);
        runner.setProperty(PutSyslog.MSG_HOSTNAME, host);
        runner.setProperty(PutSyslog.MSG_BODY, body);

        runner.assertValid();
        runner.setVariable("hostname", "hostname");
        runner.setVariable("port", "10443");
        runner.setVariable("charset", "UTF-8");
        runner.setVariable("timeout", "10 secs");
        runner.setVariable("maxSocketSenderBufferSize", "10 mb");
        runner.setVariable("idleExpiration", "10 secs");

        runner.enqueue("incoming data".getBytes(Charset.forName("UTF-8")));
        runner.run();

        runner.assertAllFlowFilesTransferred(PutSyslog.REL_SUCCESS, 1);
        Assert.assertEquals(1, sender.messages.size());
        Assert.assertEquals(expectedMessage, sender.messages.get(0).replace("\n", ""));

        final List<ProvenanceEventRecord> events = runner.getProvenanceEvents();
        Assert.assertNotNull(events);
        Assert.assertEquals(1, events.size());

        final ProvenanceEventRecord event = events.get(0);
        Assert.assertEquals(ProvenanceEventType.SEND, event.getEventType());
        Assert.assertEquals("TCP://hostname:10443", event.getTransitUri());
    }

    @Test
    public void testValidMessageStaticPropertiesNoVersion() {
        final String pri = "34";
        final String stamp = "2003-10-11T22:14:15.003Z";
        final String host = "mymachine.example.com";
        final String body = "su - ID47 - BOM'su root' failed for lonvick on /dev/pts/8";

        final String expectedMessage = "<" + pri + ">" + stamp + " " + host + " " + body;

        runner.setProperty(PutSyslog.MSG_PRIORITY, pri);
        runner.setProperty(PutSyslog.MSG_TIMESTAMP, stamp);
        runner.setProperty(PutSyslog.MSG_HOSTNAME, host);
        runner.setProperty(PutSyslog.MSG_BODY, body);

        runner.enqueue("incoming data".getBytes(Charset.forName("UTF-8")));
        runner.run();

        runner.assertAllFlowFilesTransferred(PutSyslog.REL_SUCCESS, 1);
        Assert.assertEquals(1, sender.messages.size());
        Assert.assertEquals(expectedMessage, sender.messages.get(0));
    }

    @Test
    public void testValidMessageELProperties() {
        final String pri = "34";
        final String stamp = "2003-10-11T22:14:15.003Z";
        final String host = "mymachine.example.com";
        final String body = "su - ID47 - BOM'su root' failed for lonvick on /dev/pts/8";

        final String expectedMessage = "<" + pri + ">" + stamp + " " + host + " " + body;

        runner.setProperty(PutSyslog.MSG_PRIORITY, "${syslog.priority}");
        runner.setProperty(PutSyslog.MSG_TIMESTAMP, "${syslog.timestamp}");
        runner.setProperty(PutSyslog.MSG_HOSTNAME, "${syslog.hostname}");
        runner.setProperty(PutSyslog.MSG_BODY, "${syslog.body}");

        final Map<String,String> attributes = new HashMap<>();
        attributes.put("syslog.priority", pri);
        attributes.put("syslog.timestamp", stamp);
        attributes.put("syslog.hostname", host);
        attributes.put("syslog.body", body);

        runner.enqueue("incoming data".getBytes(Charset.forName("UTF-8")), attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred(PutSyslog.REL_SUCCESS, 1);
        Assert.assertEquals(1, sender.messages.size());
        Assert.assertEquals(expectedMessage, sender.messages.get(0));
    }

    @Test
    public void testInvalidMessageELProperties() {
        final String pri = "34";
        final String stamp = "not-a-timestamp";
        final String host = "mymachine.example.com";
        final String body = "su - ID47 - BOM'su root' failed for lonvick on /dev/pts/8";

        runner.setProperty(PutSyslog.MSG_PRIORITY, "${syslog.priority}");
        runner.setProperty(PutSyslog.MSG_TIMESTAMP, "${syslog.timestamp}");
        runner.setProperty(PutSyslog.MSG_HOSTNAME, "${syslog.hostname}");
        runner.setProperty(PutSyslog.MSG_BODY, "${syslog.body}");

        final Map<String,String> attributes = new HashMap<>();
        attributes.put("syslog.priority", pri);
        attributes.put("syslog.timestamp", stamp);
        attributes.put("syslog.hostname", host);
        attributes.put("syslog.body", body);

        runner.enqueue("incoming data".getBytes(Charset.forName("UTF-8")), attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred(PutSyslog.REL_INVALID, 1);
        Assert.assertEquals(0, sender.messages.size());
    }

    @Test
    public void testIOExceptionOnSend() throws IOException {
        final String pri = "34";
        final String version = "1";
        final String stamp = "2003-10-11T22:14:15.003Z";
        final String host = "mymachine.example.com";
        final String body = "su - ID47 - BOM'su root' failed for lonvick on /dev/pts/8";

        proc = new MockPutSyslog(new MockErrorSender());
        runner = TestRunners.newTestRunner(proc);
        runner.setProperty(PutSyslog.HOSTNAME, "localhost");
        runner.setProperty(PutSyslog.PORT, "12345");
        runner.setProperty(PutSyslog.MSG_PRIORITY, pri);
        runner.setProperty(PutSyslog.MSG_VERSION, version);
        runner.setProperty(PutSyslog.MSG_TIMESTAMP, stamp);
        runner.setProperty(PutSyslog.MSG_HOSTNAME, host);
        runner.setProperty(PutSyslog.MSG_BODY, body);

        runner.enqueue("incoming data".getBytes(Charset.forName("UTF-8")));
        runner.run();

        runner.assertAllFlowFilesTransferred(PutSyslog.REL_FAILURE, 1);
        Assert.assertEquals(0, sender.messages.size());
    }

    @Test
    public void testIOExceptionCreatingConnection() throws IOException {
        final String pri = "34";
        final String version = "1";
        final String stamp = "2003-10-11T22:14:15.003Z";
        final String host = "mymachine.example.com";
        final String body = "su - ID47 - BOM'su root' failed for lonvick on /dev/pts/8";

        Processor proc = new MockCreationErrorPutSyslog(new MockErrorSender(), 1);
        runner = TestRunners.newTestRunner(proc);
        runner.setProperty(PutSyslog.HOSTNAME, "localhost");
        runner.setProperty(PutSyslog.PORT, "12345");
        runner.setProperty(PutSyslog.BATCH_SIZE, "1");
        runner.setProperty(PutSyslog.MSG_PRIORITY, pri);
        runner.setProperty(PutSyslog.MSG_VERSION, version);
        runner.setProperty(PutSyslog.MSG_TIMESTAMP, stamp);
        runner.setProperty(PutSyslog.MSG_HOSTNAME, host);
        runner.setProperty(PutSyslog.MSG_BODY, body);

        // the first run will throw IOException when calling send so the connection won't be re-qeued
        // the second run will try to create a new connection but throw an exception which should be caught and route files to failure
        runner.enqueue("incoming data".getBytes(Charset.forName("UTF-8")));
        runner.enqueue("incoming data".getBytes(Charset.forName("UTF-8")));
        runner.run(2);

        runner.assertAllFlowFilesTransferred(PutSyslog.REL_FAILURE, 2);
        Assert.assertEquals(0, sender.messages.size());
    }

    @Test
    public void testLargeMessageFailure() {
        final String pri = "34";
        final String stamp = "2015-10-15T22:14:15.003Z";
        final String host = "mymachine.example.com";

        final StringBuilder bodyBuilder = new StringBuilder(4096);
        for (int i=0; i < 4096; i++) {
            bodyBuilder.append("a");
        }

        runner.setProperty(PutSyslog.MSG_PRIORITY, "${syslog.priority}");
        runner.setProperty(PutSyslog.MSG_TIMESTAMP, "${syslog.timestamp}");
        runner.setProperty(PutSyslog.MSG_HOSTNAME, "${syslog.hostname}");
        runner.setProperty(PutSyslog.MSG_BODY, "${syslog.body}");

        final Map<String,String> attributes = new HashMap<>();
        attributes.put("syslog.priority", pri);
        attributes.put("syslog.timestamp", stamp);
        attributes.put("syslog.hostname", host);
        attributes.put("syslog.body", bodyBuilder.toString());

        runner.enqueue("incoming data".getBytes(Charset.forName("UTF-8")), attributes);
        runner.run();

        // should have dynamically created a larger buffer
        runner.assertAllFlowFilesTransferred(PutSyslog.REL_SUCCESS, 1);
        Assert.assertEquals(1, sender.messages.size());
    }

    @Test
    public void testNoIncomingData() {
        runner.setProperty(PutSyslog.MSG_PRIORITY, "10");
        runner.setProperty(PutSyslog.MSG_VERSION, "1");
        runner.setProperty(PutSyslog.MSG_TIMESTAMP, "2003-10-11T22:14:15.003Z");
        runner.setProperty(PutSyslog.MSG_HOSTNAME, "localhost");
        runner.setProperty(PutSyslog.MSG_BODY, "test");

        // queue one file but run several times to test no incoming data
        runner.enqueue("incoming data".getBytes(Charset.forName("UTF-8")));
        runner.run(5);

        runner.assertAllFlowFilesTransferred(PutSyslog.REL_SUCCESS, 1);
    }

    @Test
    public void testBatchingFlowFiles() {
        runner.setProperty(PutSyslog.BATCH_SIZE, "10");
        runner.setProperty(PutSyslog.MSG_PRIORITY, "${syslog.priority}");
        runner.setProperty(PutSyslog.MSG_TIMESTAMP, "${syslog.timestamp}");
        runner.setProperty(PutSyslog.MSG_HOSTNAME, "${syslog.hostname}");
        runner.setProperty(PutSyslog.MSG_BODY, "${syslog.body}");

        final Map<String,String> attributes = new HashMap<>();
        attributes.put("syslog.priority", "10");
        attributes.put("syslog.timestamp", "2015-10-11T22:14:15.003Z");
        attributes.put("syslog.hostname", "my.host.name");
        attributes.put("syslog.body", "blah blah blah");

        for (int i=0; i < 15; i++) {
            runner.enqueue("incoming data".getBytes(Charset.forName("UTF-8")), attributes);
        }

        runner.run();
        runner.assertAllFlowFilesTransferred(PutSyslog.REL_SUCCESS, 10);
        Assert.assertEquals(10, sender.messages.size());

        runner.run();
        runner.assertAllFlowFilesTransferred(PutSyslog.REL_SUCCESS, 15);
        Assert.assertEquals(15, sender.messages.size());
    }

    // Mock processor to return a MockCollectingSender
    static class MockPutSyslog extends PutSyslog {

        ChannelSender mockSender;

        public MockPutSyslog(ChannelSender sender) {
            this.mockSender = sender;
        }

        @Override
        protected ChannelSender createSender(SSLContextService sslContextService, String protocol, String host,
                                             int port, int maxSendBuffer, int timeout)
                throws IOException {
            return mockSender;
        }
    }

    // Mock processor to test exception when creating new senders
    static class MockCreationErrorPutSyslog extends PutSyslog {

        int numSendersCreated;
        int numSendersAllowed;
        ChannelSender mockSender;

        public MockCreationErrorPutSyslog(ChannelSender sender, int numSendersAllowed) {
            this.mockSender = sender;
            this.numSendersAllowed = numSendersAllowed;
        }

        @Override
        protected ChannelSender createSender(SSLContextService sslContextService, String protocol, String host,
                                             int port, int maxSendBuffer, int timeout)
                throws IOException {
            if (numSendersCreated >= numSendersAllowed) {
                throw new IOException("too many senders");
            }
            numSendersCreated++;
            return mockSender;
        }
    }

    // Mock sender that saves any messages passed to send()
    static class MockCollectingSender extends ChannelSender {

        List<String> messages = new ArrayList<>();

        public MockCollectingSender() throws IOException {
            super("myhost", 0, 0, null);
        }

        @Override
        public void open() throws IOException {

        }

        @Override
        public void send(String message, Charset charset) throws IOException {
            messages.add(message);
            super.send(message, charset);
        }

        @Override
        protected void write(byte[] buffer) throws IOException {

        }

        @Override
        public boolean isConnected() {
            return true;
        }

        @Override
        public void close() {

        }
    }

    // Mock sender that throws IOException on calls to write() or send()
    static class MockErrorSender extends ChannelSender {

        public MockErrorSender() throws IOException {
            super(null, 0, 0, null);
        }

        @Override
        public void open() throws IOException {

        }

        @Override
        public void send(String message, Charset charset) throws IOException {
            throw new IOException("error");
        }

        @Override
        protected void write(byte[] data) throws IOException {
            throw new IOException("error");
        }

        @Override
       public boolean isConnected() {
            return false;
        }

        @Override
        public void close() {

        }
    }

}
