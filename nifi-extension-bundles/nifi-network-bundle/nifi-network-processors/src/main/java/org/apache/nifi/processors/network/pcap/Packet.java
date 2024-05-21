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

public class Packet {
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
        this.rawBody = this.io.readBytes((Math.min(inclLen(), root().hdr().snaplen())));
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