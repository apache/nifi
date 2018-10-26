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

package org.apache.nifi.controller.queue.clustered.client.async.nio;

import org.apache.nifi.controller.MockFlowFileRecord;
import org.apache.nifi.controller.queue.LoadBalanceCompression;
import org.apache.nifi.controller.queue.clustered.FlowFileContentAccess;
import org.apache.nifi.controller.queue.clustered.SimpleLimitThreshold;
import org.apache.nifi.controller.queue.clustered.client.StandardLoadBalanceFlowFileCodec;
import org.apache.nifi.controller.queue.clustered.client.async.TransactionFailureCallback;
import org.apache.nifi.controller.queue.clustered.protocol.LoadBalanceProtocolConstants;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.zip.CRC32;
import java.util.zip.CheckedOutputStream;
import java.util.zip.Checksum;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestLoadBalanceSession {

    private final TransactionFailureCallback NOP_FAILURE_CALLBACK = new TransactionFailureCallback() {
        @Override
        public void onTransactionFailed(final List<FlowFileRecord> flowFiles, final Exception cause, final TransactionPhase transactionPhase) {
        }

        @Override
        public boolean isRebalanceOnFailure() {
            return false;
        }
    };

    private ByteArrayOutputStream received;
    private ServerSocket serverSocket;
    private int port;

    @Before
    public void setup() throws IOException {
        received = new ByteArrayOutputStream();

        serverSocket = new ServerSocket(0);
        port = serverSocket.getLocalPort();

        final Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try (final Socket socket = serverSocket.accept()) {
                    final InputStream in = socket.getInputStream();
                    int data;

                    socket.getOutputStream().write(LoadBalanceProtocolConstants.VERSION_ACCEPTED);
                    socket.getOutputStream().write(LoadBalanceProtocolConstants.SPACE_AVAILABLE);
                    socket.getOutputStream().write(LoadBalanceProtocolConstants.CONFIRM_CHECKSUM);
                    socket.getOutputStream().write(LoadBalanceProtocolConstants.CONFIRM_COMPLETE_TRANSACTION);

                    while ((data = in.read()) != -1) {
                        received.write(data);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                    Assert.fail();
                }
            }
        });
        thread.setDaemon(true);
        thread.start();
    }

    @After
    public void shutdown() throws IOException {
        serverSocket.close();
    }

    @Test(timeout = 10000)
    public void testSunnyCase() throws InterruptedException, IOException {
        final Queue<FlowFileRecord> flowFiles = new LinkedList<>();
        final FlowFileRecord flowFile1 = new MockFlowFileRecord(5);
        final FlowFileRecord flowFile2 = new MockFlowFileRecord(8);
        flowFiles.offer(flowFile1);
        flowFiles.offer(flowFile2);

        final Map<FlowFileRecord, InputStream> contentMap = new HashMap<>();
        contentMap.put(flowFile1, new ByteArrayInputStream("hello".getBytes()));
        contentMap.put(flowFile2, new ByteArrayInputStream("good-bye".getBytes()));

        final FlowFileContentAccess contentAccess = contentMap::get;

        final RegisteredPartition partition = new RegisteredPartition("unit-test-connection", () -> false,
            flowFiles::poll, NOP_FAILURE_CALLBACK, (ff, nodeId) -> {}, () -> LoadBalanceCompression.DO_NOT_COMPRESS, () -> true);

        final SocketChannel socketChannel = SocketChannel.open(new InetSocketAddress("localhost", port));

        socketChannel.configureBlocking(false);
        final PeerChannel peerChannel = new PeerChannel(socketChannel, null, "unit-test");
        final LoadBalanceSession transaction = new LoadBalanceSession(partition, contentAccess, new StandardLoadBalanceFlowFileCodec(), peerChannel, 30000,
            new SimpleLimitThreshold(100, 10_000_000));

        Thread.sleep(100L);

        while (transaction.communicate()) {
        }

        assertTrue(transaction.isComplete());
        socketChannel.close();

        final Checksum expectedChecksum = new CRC32();
        final ByteArrayOutputStream expectedOut = new ByteArrayOutputStream();
        expectedOut.write(1); // Protocol Version

        final DataOutputStream expectedDos = new DataOutputStream(new CheckedOutputStream(expectedOut, expectedChecksum));
        expectedDos.writeUTF("unit-test-connection");

        expectedDos.write(LoadBalanceProtocolConstants.CHECK_SPACE);
        expectedDos.write(LoadBalanceProtocolConstants.MORE_FLOWFILES);
        expectedDos.writeInt(68); // metadata length
        expectedDos.writeInt(1); // 1 attribute
        expectedDos.writeInt(4); // length of attribute
        expectedDos.write("uuid".getBytes());
        expectedDos.writeInt(flowFile1.getAttribute("uuid").length());
        expectedDos.write(flowFile1.getAttribute("uuid").getBytes());
        expectedDos.writeLong(flowFile1.getLineageStartDate()); // lineage start date
        expectedDos.writeLong(flowFile1.getEntryDate()); // entry date
        expectedDos.write(LoadBalanceProtocolConstants.DATA_FRAME_FOLLOWS);
        expectedDos.writeShort(5);
        expectedDos.write("hello".getBytes());
        expectedDos.write(LoadBalanceProtocolConstants.NO_DATA_FRAME);

        expectedDos.write(LoadBalanceProtocolConstants.MORE_FLOWFILES);
        expectedDos.writeInt(68); // metadata length
        expectedDos.writeInt(1); // 1 attribute
        expectedDos.writeInt(4); // length of attribute
        expectedDos.write("uuid".getBytes());
        expectedDos.writeInt(flowFile2.getAttribute("uuid").length());
        expectedDos.write(flowFile2.getAttribute("uuid").getBytes());
        expectedDos.writeLong(flowFile2.getLineageStartDate()); // lineage start date
        expectedDos.writeLong(flowFile2.getEntryDate()); // entry date
        expectedDos.write(LoadBalanceProtocolConstants.DATA_FRAME_FOLLOWS);
        expectedDos.writeShort(8);
        expectedDos.write("good-bye".getBytes());
        expectedDos.write(LoadBalanceProtocolConstants.NO_DATA_FRAME);

        expectedDos.write(LoadBalanceProtocolConstants.NO_MORE_FLOWFILES);
        expectedDos.writeLong(expectedChecksum.getValue());
        expectedDos.write(LoadBalanceProtocolConstants.COMPLETE_TRANSACTION);

        final byte[] expectedSent = expectedOut.toByteArray();

        while (received.size() < expectedSent.length) {
            Thread.sleep(10L);
        }
        final byte[] dataSent = received.toByteArray();

        assertArrayEquals(expectedSent, dataSent);

        assertEquals(Arrays.asList(flowFile1, flowFile2), transaction.getFlowFilesSent());
    }


    @Test(timeout = 10000)
    public void testLargeContent() throws InterruptedException, IOException {
        final byte[] content = new byte[66000];
        for (int i=0; i < 66000; i++) {
            content[i] = 'A';
        }

        final Queue<FlowFileRecord> flowFiles = new LinkedList<>();
        final FlowFileRecord flowFile1 = new MockFlowFileRecord(content.length);
        flowFiles.offer(flowFile1);

        final Map<FlowFileRecord, InputStream> contentMap = new HashMap<>();
        contentMap.put(flowFile1, new ByteArrayInputStream(content));

        final FlowFileContentAccess contentAccess = contentMap::get;

        final RegisteredPartition partition = new RegisteredPartition("unit-test-connection", () -> false,
            flowFiles::poll, NOP_FAILURE_CALLBACK, (ff, nodeId) -> {}, () -> LoadBalanceCompression.DO_NOT_COMPRESS, () -> true);

        final SocketChannel socketChannel = SocketChannel.open(new InetSocketAddress("localhost", port));

        socketChannel.configureBlocking(false);
        final PeerChannel peerChannel = new PeerChannel(socketChannel, null, "unit-test");
        final LoadBalanceSession transaction = new LoadBalanceSession(partition, contentAccess, new StandardLoadBalanceFlowFileCodec(), peerChannel, 30000,
            new SimpleLimitThreshold(100, 10_000_000));

        Thread.sleep(100L);

        while (transaction.communicate()) {
        }

        socketChannel.close();

        final Checksum expectedChecksum = new CRC32();
        final ByteArrayOutputStream expectedOut = new ByteArrayOutputStream();
        expectedOut.write(1); // Protocol Version

        final DataOutputStream expectedDos = new DataOutputStream(new CheckedOutputStream(expectedOut, expectedChecksum));

        expectedDos.writeUTF("unit-test-connection");

        expectedDos.write(LoadBalanceProtocolConstants.CHECK_SPACE);
        expectedDos.write(LoadBalanceProtocolConstants.MORE_FLOWFILES);
        expectedDos.writeInt(68); // metadata length
        expectedDos.writeInt(1); // 1 attribute
        expectedDos.writeInt(4); // length of attribute
        expectedDos.write("uuid".getBytes());
        expectedDos.writeInt(flowFile1.getAttribute("uuid").length());
        expectedDos.write(flowFile1.getAttribute("uuid").getBytes());
        expectedDos.writeLong(flowFile1.getLineageStartDate()); // lineage start date
        expectedDos.writeLong(flowFile1.getEntryDate()); // entry date

        // first data frame
        expectedDos.write(LoadBalanceProtocolConstants.DATA_FRAME_FOLLOWS);
        expectedDos.writeShort(LoadBalanceSession.MAX_DATA_FRAME_SIZE);
        expectedDos.write(Arrays.copyOfRange(content, 0, LoadBalanceSession.MAX_DATA_FRAME_SIZE));

        // second data frame
        expectedDos.write(LoadBalanceProtocolConstants.DATA_FRAME_FOLLOWS);
        expectedDos.writeShort(content.length - LoadBalanceSession.MAX_DATA_FRAME_SIZE);
        expectedDos.write(Arrays.copyOfRange(content, LoadBalanceSession.MAX_DATA_FRAME_SIZE, content.length));
        expectedDos.write(LoadBalanceProtocolConstants.NO_DATA_FRAME);

        expectedDos.write(LoadBalanceProtocolConstants.NO_MORE_FLOWFILES);
        expectedDos.writeLong(expectedChecksum.getValue());
        expectedDos.write(LoadBalanceProtocolConstants.COMPLETE_TRANSACTION);

        final byte[] expectedSent = expectedOut.toByteArray();

        while (received.size() < expectedSent.length) {
            Thread.sleep(10L);
        }
        final byte[] dataSent = received.toByteArray();

        assertArrayEquals(expectedSent, dataSent);

        assertEquals(Arrays.asList(flowFile1), transaction.getFlowFilesSent());
    }
}
