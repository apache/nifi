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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestPCAP {
    private static final byte[] PACKET_DATA = new byte[]{
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
            10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
            20, 21, 22, 23, 24, 25, 26, 27, 28, 29,
    };

    private static final int[][] PCAP_HEADER_VALUES = new int[][]{
            new int[]{2, 2},
            new int[]{4, 2},
            new int[]{0, 4},
            new int[]{0, 4},
            new int[]{40, 4},
            new int[]{1, 4}
    };

    private static final Map<String, Long> packetHeaderValues = Map.of(
            "tsSec", 1713184965L,
            "tsUsec", 1000L,
            "inclLen", 30L,
            "origLen", 30L
    );

    @Test
    void testReadBytesFull() {

        // Create a header for the test PCAP
        ByteBuffer headerBuffer = ByteBuffer.allocate(PCAPHeader.PCAP_HEADER_LENGTH);
        headerBuffer.put(new byte[]{(byte) 0xa1, (byte) 0xb2, (byte) 0xc3, (byte) 0xd4});
        for (int[] value : PCAP_HEADER_VALUES) {
            headerBuffer.put(PCAP.readIntToNBytes(value[0], value[1]));
        }
        PCAPHeader hdr = new PCAPHeader(new ByteBufferReader(headerBuffer.array()));
        // Create a sample packet
        List<Packet> packets = new ArrayList<>();
        packets.add(new Packet(
                packetHeaderValues.get("tsSec"),
                packetHeaderValues.get("tsUsec"),
                packetHeaderValues.get("inclLen"),
                packetHeaderValues.get("origLen"),
                PACKET_DATA
        ));

        // create test PCAP
        PCAP testPcap = new PCAP(hdr, packets);

        // Call the readBytesFull method
        byte[] result = testPcap.toByteArray();

        // Assert the expected byte array length
        assertEquals(70, result.length);

        // Assert the expected byte array values
        ByteBuffer buffer = ByteBuffer.wrap(result);
        assertEquals(0xa1b2c3d4, buffer.getInt());
        ByteBuffer litteEndianBuffer = ByteBuffer.wrap(result).order(ByteOrder.LITTLE_ENDIAN);
        litteEndianBuffer.position(4);

        for (int[] value : PCAP_HEADER_VALUES) {
            if (value[1] == 2) {
                assertEquals(value[0], litteEndianBuffer.getShort());
            } else {
                assertEquals(value[0], litteEndianBuffer.getInt());
            }
        }

        assertEquals(packetHeaderValues.get("tsSec"), litteEndianBuffer.getInt());
        assertEquals(packetHeaderValues.get("tsUsec"), litteEndianBuffer.getInt());
        assertEquals(packetHeaderValues.get("inclLen"), litteEndianBuffer.getInt());
        assertEquals(packetHeaderValues.get("origLen"), litteEndianBuffer.getInt());
        byte[] body = new byte[30];
        litteEndianBuffer.get(40, body, 0, 30);
        assertArrayEquals(body, PACKET_DATA);
    }
}