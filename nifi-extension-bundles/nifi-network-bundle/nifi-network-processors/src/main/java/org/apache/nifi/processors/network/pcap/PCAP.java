// MIT License

// Copyright (c) 2015-2023 Kaitai Project

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ListIterator;
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
    private ByteBufferInterface io;

    public PCAP(ByteBufferInterface io) {
        this(io, null, null);
    }

    public PCAP(ByteBufferInterface io, Object parent, PCAP root) {

        this.parent = parent;
        this.root = root == null ? this : root;
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

        ListIterator<Packet> packetsIterator = packets.listIterator();

        int packetBufferSize = 0;

        for (int loop = 0; loop < packets.size(); loop++) {
            Packet currentPacket = packetsIterator.next();
            ByteBuffer currentPacketBytes = ByteBuffer.allocate(16 + currentPacket.rawBody().length);
            currentPacketBytes.put(readLongToNBytes(currentPacket.tsSec, 4, false));
            currentPacketBytes.put(readLongToNBytes(currentPacket.tsUsec, 4, false));
            currentPacketBytes.put(readLongToNBytes(currentPacket.inclLen, 4, false));
            currentPacketBytes.put(readLongToNBytes(currentPacket.origLen, 4, false));
            currentPacketBytes.put(currentPacket.rawBody());

            packetByteArrays.add(currentPacketBytes.array());
            packetBufferSize += 16 + currentPacket.rawBody().length;
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
        this.hdr = new Header(this.io, this, root);
        this.packets = new ArrayList<>();
        {
            while (!this.io.isEof()) {
                this.packets.add(new Packet(this.io, this, root));
            }
        }
    }

    public static class ByteBufferInterface {

        public ByteBufferInterface(byte[] byteArray) {
            this.buffer = ByteBuffer.wrap(byteArray);
        }

        public static class ValidationNotEqualError extends Exception {
            public ValidationNotEqualError(String message) {
                super(message);
            }
        }

        public ByteBuffer buffer;

        public int readU2be() {
            buffer.order(ByteOrder.BIG_ENDIAN);
            return (buffer.getShort() & 0xffff);
        }

        public int readU2le() {
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            return (buffer.getShort() & 0xffff);
        }

        public long readU4be() {
            buffer.order(ByteOrder.BIG_ENDIAN);
            return ((long) buffer.getInt() & 0xffffffffL);
        }

        public long readU4le() {
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            return ((long) buffer.getInt() & 0xffffffffL);
        }

        public int readS4be() {
            buffer.order(ByteOrder.BIG_ENDIAN);
            return buffer.getInt();
        }

        public int readS4le() {
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            return buffer.getInt();
        }

        public byte[] readBytes(int n) {
            byte[] output = new byte[n];
            buffer.get(output);
            return output;
        }

        public byte[] readBytes(long n) {
            byte[] output = new byte[(int) n];
            buffer.get(output);
            return output;
        }

        public boolean isEof() {
            return !buffer.hasRemaining();
        }
    }

    /**
     * @see <a href=
     *      "https://wiki.wireshark.org/Development/LibpcapFileFormat#Global_Header">Source</a>
     */
    public static class Header {
        private ByteBufferInterface io;
        private final Logger logger = LoggerFactory.getLogger(Header.class);

        public Header(ByteBufferInterface io, PCAP parent, PCAP root) {

            this.parent = parent;
            this.root = root;
            this.io = io;

            try {
                read();
            } catch (ByteBufferInterface.ValidationNotEqualError e) {
                this.logger.error("PCAP file header could not be parsed due to {}", e);
            }
        }

        public Header(byte[] magicNumber, int versionMajor, int versionMinor, int thiszone, long sigfigs, long snaplen,
                long network) {

            this.magicNumber = magicNumber;
            this.versionMajor = versionMajor;
            this.versionMinor = versionMinor;
            this.thiszone = thiszone;
            this.sigfigs = sigfigs;
            this.snaplen = snaplen;
            this.network = network;
        }

        public ByteBufferInterface io() {
            return io;
        }

        private void read()
                throws ByteBufferInterface.ValidationNotEqualError {
            this.magicNumber = this.io.readBytes(4);
            if (this.magicNumber == new byte[] { (byte) 0xd4, (byte) 0xc3, (byte) 0xb2, (byte) 0xa1 }) {
                // have to swap the bits
                this.versionMajor = this.io.readU2be();
                if (!(versionMajor() == 2)) {

                    throw new ByteBufferInterface.ValidationNotEqualError("Packet major version is not 2.");
                }
                this.versionMinor = this.io.readU2be();
                this.thiszone = this.io.readS4be();
                this.sigfigs = this.io.readU4be();
                this.snaplen = this.io.readU4be();
                this.network = this.io.readU4be();
            } else {
                this.versionMajor = this.io.readU2le();
                if (!(versionMajor() == 2)) {
                    throw new ByteBufferInterface.ValidationNotEqualError("Packet major version is not 2.");
                }
                this.versionMinor = this.io.readU2le();
                this.thiszone = this.io.readS4le();
                this.sigfigs = this.io.readU4le();
                this.snaplen = this.io.readU4le();
                this.network = this.io.readU4le();
            }
        }

        private byte[] magicNumber;
        private int versionMajor;
        private int versionMinor;
        private int thiszone;
        private long sigfigs;
        private long snaplen;
        private long network;
        private PCAP root;
        private PCAP parent;

        public byte[] magicNumber() {
            return magicNumber;
        }

        public int versionMajor() {
            return versionMajor;
        }

        public int versionMinor() {
            return versionMinor;
        }

        /**
         * Correction time in seconds between UTC and the local
         * timezone of the following packet header timestamps.
         */
        public int thiszone() {
            return thiszone;
        }

        /**
         * In theory, the accuracy of time stamps in the capture; in
         * practice, all tools set it to 0.
         */
        public long sigfigs() {
            return sigfigs;
        }

        /**
         * The "snapshot length" for the capture (typically 65535 or
         * even more, but might be limited by the user), see: incl_len
         * vs. orig_len.
         */
        public long snaplen() {
            return snaplen;
        }

        /**
         * Link-layer header type, specifying the type of headers at
         * the beginning of the packet.
         */
        public long network() {
            return network;
        }

        public PCAP root() {
            return root;
        }

        public PCAP parent() {
            return parent;
        }
    }

    /**
     * @see <a href=
     *      "https://wiki.wireshark.org/Development/LibpcapFileFormat#Record_.28Packet.29_Header">Source</a>
     */
    public static class Packet {
        public ByteBufferInterface io;

        public Packet(ByteBufferInterface io, PCAP parent, PCAP root) {

            this.parent = parent;
            this.root = root;
            this.io = io;
            read();
        }

        public Packet(long tSSec, long tSUsec, long inclLen, long origLen, byte[] rawBody) {

            this.tsSec = tSSec;
            this.tsUsec = tSUsec;
            this.inclLen = inclLen;
            this.origLen = origLen;
            this.rawBody = rawBody;
        }

        private void read() {
            this.tsSec = this.io.readU4le();
            this.tsUsec = this.io.readU4le();
            this.inclLen = this.io.readU4le();
            this.origLen = this.io.readU4le();
            this.rawBody = this.io.readBytes((inclLen() < root().hdr().snaplen() ? inclLen() : root().hdr().snaplen()));
        }

        private long tsSec;
        private long tsUsec;
        private long inclLen;
        private long origLen;
        private PCAP root;
        private PCAP parent;
        private byte[] rawBody;

        public long tsSec() {
            return tsSec;
        }

        public long tsUsec() {
            return tsUsec;
        }

        /**
         * Number of bytes of packet data actually captured and saved in the file.
         */
        public long inclLen() {
            return inclLen;
        }

        /**
         * Length of the packet as it appeared on the network when it was captured.
         */
        public long origLen() {
            return origLen;
        }

        /**
         * @see <a href=
         *      "https://wiki.wireshark.org/Development/LibpcapFileFormat#Packet_Data">Source</a>
         */
        public PCAP root() {
            return root;
        }

        public PCAP parent() {
            return parent;
        }

        public byte[] rawBody() {
            return rawBody;
        }
    }

    private Header hdr;
    private List<Packet> packets;
    private PCAP root;
    private Object parent;

    public Header hdr() {
        return hdr;
    }

    public List<Packet> packets() {
        return packets;
    }

    public PCAP root() {
        return root;
    }

    public Object parent() {
        return parent;
    }
}
