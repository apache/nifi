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


package org.apache.nifi.processors.network;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.apache.nifi.processors.network.util.PCAP;
import org.apache.nifi.processors.network.util.PCAP.Header;
import org.apache.nifi.processors.network.util.PCAP.Packet;


public class TestSplitPCAP {

    private Header hdr;
    private Packet validPacket;
    private Packet invalidPacket;

    @BeforeEach
    public void init(){
        // Create a header for the test PCAP
        this.hdr = new Header(
            new byte[]{(byte) 0xa1, (byte) 0xb2, (byte) 0xc3, (byte) 0xd4},
            2,
            4,
            0,
            (long) 0,
            (long) 40,
            (long) 1 // ETHERNET
        );

        this.validPacket = new Packet(
            (long) 1713184965,
            (long) 1000,
            (long) 30,
            (long) 30,
            new byte[]{
                0,  1,  2,  3,  4,  5,  6,  7,  8,  9,
                10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
                20, 21, 22, 23, 24, 25, 26, 27, 28, 29,
            }
        );

        this.invalidPacket = new Packet(
            (long) 1713184965,
            (long) 1000,
            (long) 10,
            (long) 10,
            new byte[]{
                0,  1,  2,  3,  4,  5,  6,  7,  8,  9,
                10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
                20, 21, 22, 23, 24, 25, 26, 27, 28, 29,
            }
        );

    }

    @Test
    public void testValidPackets() throws IOException {
        TestRunner runner = TestRunners.newTestRunner(SplitPCAP.class);
        runner.setProperty(SplitPCAP.PCAP_MAX_SIZE, "100");

        ArrayList<Packet> packets = new ArrayList<>();
        for (var loop = 0; loop < 3; loop++){
            packets.add(this.validPacket);
        }

        PCAP testPcap = new PCAP(this.hdr, packets);

        runner.enqueue(testPcap.readBytesFull());

        runner.run();

        runner.assertTransferCount(SplitPCAP.REL_SPLIT, 3);
        runner.assertTransferCount(SplitPCAP.REL_ORIGINAL, 1);
        runner.assertQueueEmpty();
    }

    @Test
    public void testInvalidPackets() throws IOException {
        TestRunner runner = TestRunners.newTestRunner(SplitPCAP.class);
        runner.setProperty(SplitPCAP.PCAP_MAX_SIZE, "50");

        ArrayList<Packet> packets = new ArrayList<>();
        for (var loop = 0; loop < 3; loop++){
            packets.add(this.invalidPacket);
        }

        PCAP testPcap = new PCAP(this.hdr, packets);

        runner.enqueue(testPcap.readBytesFull());

        runner.run();

        runner.assertAllFlowFilesTransferred(SplitPCAP.REL_FAILURE, 1);
        runner.assertQueueEmpty();
    }

    @Test
    public void testPacketsTooBig() throws IOException {
        TestRunner runner = TestRunners.newTestRunner(SplitPCAP.class);
        runner.setProperty(SplitPCAP.PCAP_MAX_SIZE, "10");

        ArrayList<Packet> packets = new ArrayList<>();
        for (var loop = 0; loop < 3; loop++){
            packets.add(this.validPacket);
        }

        PCAP testPcap = new PCAP(this.hdr, packets);

        runner.enqueue(testPcap.readBytesFull());

        runner.run();

        runner.assertAllFlowFilesTransferred(SplitPCAP.REL_FAILURE, 1);
        runner.assertQueueEmpty();
    }

    @Test
    public void testOneInvalidPacket() throws IOException {
        TestRunner runner = TestRunners.newTestRunner(SplitPCAP.class);
        runner.setProperty(SplitPCAP.PCAP_MAX_SIZE, "10");

        ArrayList<Packet> packets = new ArrayList<>();
        for (var loop = 0; loop < 3; loop++){
            packets.add(this.validPacket);
        }

        packets.add(this.invalidPacket);

        PCAP testPcap = new PCAP(this.hdr, packets);

        runner.enqueue(testPcap.readBytesFull());

        runner.run();

        runner.assertAllFlowFilesTransferred(SplitPCAP.REL_FAILURE, 1);
        runner.assertQueueEmpty();
    }
}
