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

/**
 * PCAP (named after libpcap / winpcap) is a popular format for saving
 * network traffic grabbed by network sniffers. It is typically
 * produced by tools like [tcpdump](<a href="https://www.tcpdump.org/">...</a>) or
 * [Wireshark](<a href="https://www.wireshark.org/">...</a>).
 *
 * @see <a href=
 * "https://wiki.wireshark.org/Development/LibpcapFileFormat">Source</a>
 */
public class PCAP {
    private final PCAPHeader hdr;
    private final List<Packet> packets;

    public PCAP(ByteBufferReader io) {
        this.hdr = new PCAPHeader(io);
        this.packets = new ArrayList<>();
        while (io.hasRemaining()) {
            this.packets.add(new Packet(io, this));
        }
    }

    public PCAP(PCAPHeader hdr, List<Packet> packets) {
        this.hdr = hdr;
        this.packets = packets;
    }

    public byte[] toByteArray() {
        int headerBufferSize = PCAPHeader.PCAP_HEADER_LENGTH;
        ByteBuffer headerBuffer = ByteBuffer.allocate(headerBufferSize);
        headerBuffer.order(ByteOrder.LITTLE_ENDIAN);

        headerBuffer.put(this.hdr.magicNumber());
        headerBuffer.put(readIntToNBytes(this.hdr.versionMajor(), 2));
        headerBuffer.put(readIntToNBytes(this.hdr.versionMinor(), 2));
        headerBuffer.put(readIntToNBytes(this.hdr.thiszone(), 4));
        headerBuffer.put(readLongToNBytes(this.hdr.sigfigs(), 4, true));
        headerBuffer.put(readLongToNBytes(this.hdr.snaplen(), 4, true));
        headerBuffer.put(readLongToNBytes(this.hdr.network(), 4, true));

        List<byte[]> packetByteArrays = new ArrayList<>();

        int packetBufferSize = 0;

        for (Packet currentPacket : packets) {
            int currentPacketTotalLength = Packet.PACKET_HEADER_LENGTH + currentPacket.rawBody().length;

            ByteBuffer currentPacketBytes = ByteBuffer.allocate(currentPacketTotalLength);
            currentPacketBytes.put(readLongToNBytes(currentPacket.tsSec(), 4, false));
            currentPacketBytes.put(readLongToNBytes(currentPacket.tsUsec(), 4, false));
            currentPacketBytes.put(readLongToNBytes(currentPacket.inclLen(), 4, false));
            currentPacketBytes.put(readLongToNBytes(currentPacket.origLen(), 4, false));
            currentPacketBytes.put(currentPacket.rawBody());

            packetByteArrays.add(currentPacketBytes.array());
            packetBufferSize += currentPacketTotalLength;
        }

        ByteBuffer packetBuffer = ByteBuffer.allocate(packetBufferSize);
        packetBuffer.order(ByteOrder.LITTLE_ENDIAN);

        for (byte[] packetByteArray : packetByteArrays) {
            packetBuffer.put(packetByteArray);
        }

        ByteBuffer allBytes = ByteBuffer.allocate(headerBufferSize + packetBufferSize);
        allBytes.order(ByteOrder.LITTLE_ENDIAN);

        allBytes.put(headerBuffer.array());
        allBytes.put(packetBuffer.array());

        return allBytes.array();
    }

    protected static byte[] readIntToNBytes(int input, int numberOfBytes) {
        byte[] output = new byte[numberOfBytes];
        output[0] = (byte) (input & 0xff);
        for (int loop = 1; loop < numberOfBytes; loop++) {
            output[loop] = (byte) (input >>> (8 * loop));
        }
        return output;
    }

    private byte[] readLongToNBytes(long input, int numberOfBytes, boolean isSigned) {
        byte[] output = new byte[numberOfBytes];
        output[0] = (byte) (input & 0xff);
        for (int loop = 1; loop < numberOfBytes; loop++) {
            if (isSigned) {
                output[loop] = (byte) (input >> (8 * loop));
            } else {
                output[loop] = (byte) (input >>> (8 * loop));
            }
        }
        return output;
    }

    public PCAPHeader getHeader() {
        return hdr;
    }

    public List<Packet> getPackets() {
        return packets;
    }
}