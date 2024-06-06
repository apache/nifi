// MIT License

// Copyright (c) 2015-2024 Kaitai Project

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

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
 *      "https://wiki.wireshark.org/Development/LibpcapFileFormat">Source</a>
 */
public class PCAP {
    static final int PCAP_HEADER_LENGTH = 24;
    private ByteBufferInterface io;
    private Header hdr;
    private List<Packet> packets;

    public PCAP(ByteBufferInterface io) {
        this.io = io;
        read();
    }

    public PCAP(Header hdr, List<Packet> packets) {
        this.hdr = hdr;
        this.packets = packets;
    }

    public byte[] readBytesFull() {

        int headerBufferSize = 20 + this.hdr().magicNumber().length;
        ByteBuffer headerBuffer = ByteBuffer.allocate(headerBufferSize);
        headerBuffer.order(ByteOrder.LITTLE_ENDIAN);

        headerBuffer.put(this.hdr().magicNumber());
        headerBuffer.put(this.readIntToNBytes(this.hdr().versionMajor(), 2, false));
        headerBuffer.put(this.readIntToNBytes(this.hdr().versionMinor(), 2, false));
        headerBuffer.put(this.readIntToNBytes(this.hdr().thiszone(), 4, false));
        headerBuffer.put(this.readLongToNBytes(this.hdr().sigfigs(), 4, true));
        headerBuffer.put(this.readLongToNBytes(this.hdr().snaplen(), 4, true));
        headerBuffer.put(this.readLongToNBytes(this.hdr().network(), 4, true));

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

    private byte[] readIntToNBytes(int input, int numberOfBytes, boolean isSigned) {
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

    private void read() {
        this.hdr = new Header(this.io);
        this.packets = new ArrayList<>();
        while (!this.io.isEof()) {
            this.packets.add(new Packet(this.io, this));
        }
    }

    public Header hdr() {
        return hdr;
    }

    public List<Packet> packets() {
        return packets;
    }
}
