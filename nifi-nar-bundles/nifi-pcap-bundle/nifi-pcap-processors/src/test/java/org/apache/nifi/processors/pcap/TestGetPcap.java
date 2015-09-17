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
package org.apache.nifi.processors.pcap;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.pcap4j.core.PacketListener;
import org.pcap4j.core.PcapHandle;
import org.pcap4j.core.PcapNativeException;
import org.pcap4j.core.PcapNetworkInterface;
import org.pcap4j.packet.ExceptionPacket;
import org.pcap4j.packet.IllegalRawDataException;
import org.pcap4j.packet.Packet;
import org.pcap4j.packet.TestPacket;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import static org.junit.Assert.assertEquals;

@RunWith(PowerMockRunner.class)
@PrepareForTest({PcapNetworkInterface.class,PcapHandle.class})
public class TestGetPcap {

    @Test
    public void testPacketPerFlowFile() throws UnsupportedEncodingException, IllegalRawDataException {
        final Packet packet1 = new TestPacket("payload data1".getBytes("UTF-8"), 0, 13);
        final Packet packet2 = new TestPacket("payload data2".getBytes("UTF-8"), 0, 13);

        final List<Packet> packets = new ArrayList<>();
        packets.add(packet1);
        packets.add(packet2);

        final TestableProcessor processor = new TestableProcessor(packets);
        final TestRunner testRunner = TestRunners.newTestRunner(processor);
        testRunner.setProperty(GetPcap.INTERFACE_NAME, "lo0");

        testRunner.run(2);
        testRunner.assertAllFlowFilesTransferred(GetPcap.SUCCESS, 2);

        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(GetPcap.SUCCESS);
        final MockFlowFile flowFile1 = flowFiles.get(0);
        final MockFlowFile flowFile2 = flowFiles.get(1);
        assertEquals(new String(packet1.getRawData()), new String(flowFile1.toByteArray()));
        assertEquals(new String(packet2.getRawData()), new String(flowFile2.toByteArray()));
        assertEquals(0, processor.getPacketQueueSize());
    }

    @Test
    public void testRequeueOnError() throws UnsupportedEncodingException {
        final Packet packet1 = new ExceptionPacket("payload data1".getBytes("UTF-8"), 0, 13);

        final List<Packet> packets = new ArrayList<>();
        packets.add(packet1);

        final TestableProcessor processor = new TestableProcessor(packets);
        final TestRunner testRunner = TestRunners.newTestRunner(processor);
        testRunner.setProperty(GetPcap.INTERFACE_NAME, "lo0");

        testRunner.run();
        testRunner.assertTransferCount(GetPcap.SUCCESS, 0);
        assertEquals(1, processor.getPacketQueueSize());
    }

    // Use PowerMock to mock pcap4j classes that are final, and inject provided packets into the queue
    private class TestableProcessor extends GetPcap {

        final List<Packet> packets;
        BlockingQueue<Packet> packetQueue;

        public TestableProcessor(final List<Packet> packets) {
            this.packets = packets;
        }

        @Override
        protected PacketListener createPacketListener(final BlockingQueue<Packet> packetQueue) {
            this.packetQueue = packetQueue;
            for (Packet packet : packets) {
                this.packetQueue.offer(packet);
            }
            return super.createPacketListener(packetQueue);
        }

        @Override
        protected PcapHandle getPcapHandle(String mode, int snapLen, int readTimeout, PcapNetworkInterface nif)
                throws PcapNativeException {
            return PowerMockito.mock(PcapHandle.class);
        }

        @Override
        protected PcapNetworkInterface getDevByName(String interfaceName) throws PcapNativeException {
            return PowerMockito.mock(PcapNetworkInterface.class);
        }

        public int getPacketQueueSize() {
            if (this.packetQueue == null) {
                return 0;
            } else {
                return this.packetQueue.size();
            }
        }
    }

}
