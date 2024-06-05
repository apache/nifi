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

public class Packet {
    static final int PACKET_HEADER_LENGTH = 16;
    private ByteBufferInterface io;
    private long tsSec;
    private long tsUsec;
    private long inclLen;
    private long origLen;
    private long expectedLength;
    private long totalLength;
    private PCAP root;
    private byte[] rawBody;

    public Packet(ByteBufferInterface io, PCAP root) {

        this.root = root;
        this.io = io;
        read();
    }

    public Packet(long tSSec, long tSUsec, long inclLen, long origLen, byte[] rawBody) {

        this.tsSec = tSSec;
        this.tsUsec = tSUsec;
        this.inclLen = inclLen;
        this.origLen = origLen;

        this.expectedLength = inclLen();

        this.totalLength = PACKET_HEADER_LENGTH + rawBody.length;
        this.rawBody = rawBody;
    }

    public Packet(byte[] headerArray, PCAP root) {
        this.root = root;
        this.io = new ByteBufferInterface(headerArray);
        read();
    }

    private void read() {
        this.tsSec = this.io.readU4le();
        this.tsUsec = this.io.readU4le();
        this.inclLen = this.io.readU4le();
        this.origLen = this.io.readU4le();

        this.expectedLength = Math.min(inclLen(), root().hdr().snaplen());

        if (!this.io.isEof() && this.io.bytesLeft() >= expectedLength) {
            this.rawBody = this.io.readBytes(expectedLength);
        } else {
            this.rawBody = new byte[0];
        }
        this.totalLength = PACKET_HEADER_LENGTH + this.rawBody.length;
    }

    public boolean isInvalid() {
        return (this.rawBody.length == 0
        && this.inclLen > this.origLen
        && this.origLen > 0
        && this.inclLen > 0);
    }

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

    public byte[] rawBody() {
        return rawBody;
    }

    public int expectedLength() {
        return (int) expectedLength;
    }

    public int totalLength() {
        return (int) totalLength;
    }

    public void setBody(byte[] newBody) {
        this.rawBody = newBody;
        this.totalLength = PACKET_HEADER_LENGTH + rawBody.length;
    }
}