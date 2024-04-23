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


package org.apache.nifi.processors.standard.util;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;

import org.apache.nifi.processors.standard.util.Pcap.Packet;
import org.apache.nifi.processors.standard.util.Pcap.Header;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.junit.Assert;

public class TestPcap {

    @BeforeEach
    public void init() {

    }

    @Test
    public void testReadBytesFull() {

        // Create a header for the test PCAP
        Header hdr = new Header(
            new byte[]{(byte) 0xa1, (byte) 0xb2, (byte) 0xc3, (byte) 0xd4},
            2,
            4,
            0,
            (long) 0,
            (long) 40,
            "ETHERNET"
        );

        // Create a sample packet
        ArrayList<Packet> packets = new ArrayList<>();
        packets.add(new Packet(
            (long) 1713184965,
            (long) 1000,
            (long) 30,
            (long) 30,
            new byte[]{
                0,  1,  2,  3,  4,  5,  6,  7,  8,  9,
                10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
                20, 21, 22, 23, 24, 25, 26, 27, 28, 29,
            },
            "ETHERNET"));

        // create test PCAP

        Pcap testPcap = new Pcap(hdr, packets);

        // Call the readBytesFull method
        byte[] result = testPcap.readBytesFull();

        // Assert the expected byte array length
        Assert.assertEquals(70, result.length);

        // Assert the expected byte array values
        ByteBuffer buffer = ByteBuffer.wrap(result);//.order(ByteOrder.LITTLE_ENDIAN);
        Assert.assertEquals(0xa1b2c3d4, buffer.getInt());
        ByteBuffer LEBuffer = ByteBuffer.wrap(result).order(ByteOrder.LITTLE_ENDIAN);
        LEBuffer.position(4);
        Assert.assertEquals(2, LEBuffer.getShort());
        Assert.assertEquals(4, LEBuffer.getShort());
        Assert.assertEquals(0, LEBuffer.getInt());
        Assert.assertEquals(0, LEBuffer.getInt());
        Assert.assertEquals(40, LEBuffer.getInt());
        Assert.assertEquals(1, LEBuffer.getInt());


        Assert.assertEquals(1713184965, LEBuffer.getInt());
        Assert.assertEquals(1000, LEBuffer.getInt());
        Assert.assertEquals(30, LEBuffer.getInt());
        Assert.assertEquals(30, LEBuffer.getInt());
        byte[] bodyArray = new byte[30];
        LEBuffer.get(40, bodyArray, 0, 30).array();
        Assert.assertArrayEquals(new byte[]{
            0,  1,  2,  3,  4,  5,  6,  7,  8,  9,
            10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
            20, 21, 22, 23, 24, 25, 26, 27, 28, 29,
        }, bodyArray);
    }
}
