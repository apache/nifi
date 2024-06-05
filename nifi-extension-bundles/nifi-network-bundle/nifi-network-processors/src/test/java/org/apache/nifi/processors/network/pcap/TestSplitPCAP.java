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


package org.apache.nifi.processors.network.pcap;

import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.util.Map;

import org.apache.nifi.processor.Relationship;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;



class TestSplitPCAP {

    private Header hdr;
    private Packet validPacket;
    private Packet invalidPacket;

    @BeforeEach
    void init() {
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
            (long) 50,
            (long) 10,
            new byte[]{
                0,  1,  2,  3,  4,  5,  6,  7,  8,  9,
                10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
                20, 21, 22, 23, 24, 25, 26, 27, 28, 29,
            }
        );

    }


    void executeTest(String pcapMaxSize, List<Packet> packets, Map<Relationship, Integer> expectedRelations) {
        TestRunner runner = TestRunners.newTestRunner(SplitPCAP.class);
        runner.setProperty(SplitPCAP.PCAP_MAX_SIZE, pcapMaxSize);

        PCAP testPcap = new PCAP(this.hdr, packets);

        runner.enqueue(testPcap.readBytesFull());

        runner.run();

        for (Map.Entry<Relationship, Integer> entry : expectedRelations.entrySet()) {
            runner.assertTransferCount(entry.getKey(), entry.getValue());
        }

        runner.assertQueueEmpty();
    }

    @Test
    void testSuccesses()  {
        executeTest(
            "100B",
            Collections.nCopies(3, this.validPacket),
            Map.of(
                SplitPCAP.REL_SPLIT, 3,
                SplitPCAP.REL_ORIGINAL, 1
            )
        );
        executeTest(
            "50B",
            Collections.nCopies(3, this.validPacket),
            Map.of(
                SplitPCAP.REL_SPLIT, 4,
                SplitPCAP.REL_ORIGINAL, 1
            )
        );
    }

    @Test
    void testFailures()  {
        executeTest(
            "50B",
            Collections.nCopies(3, this.invalidPacket),
            Map.of(SplitPCAP.REL_FAILURE, 1)
        );
        executeTest(
            "10B",
            Collections.nCopies(3, this.validPacket),
            Map.of(SplitPCAP.REL_FAILURE, 1)
        );

        List<Packet> mixedValidityPackets = new ArrayList<>(Collections.nCopies(3, this.validPacket));
        mixedValidityPackets.add(this.invalidPacket);
        executeTest(
            "50B",
            mixedValidityPackets,
            Map.of(SplitPCAP.REL_FAILURE, 1)
        );
    }
}
