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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Header {
    private ByteBufferInterface io;
    private final Logger logger = LoggerFactory.getLogger(Header.class);

    public Header(ByteBufferInterface io, PCAP parent, PCAP root) {

        this.parent = parent;
        this.root = root;
        this.io = io;

        try {
            read();
        } catch (ByteBufferInterface.ValidationNotEqualError e) {
            this.logger.error("PCAP file header could not be parsed due to ", e);
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
        if (this.magicNumber == new byte[] {(byte) 0xd4, (byte) 0xc3, (byte) 0xb2, (byte) 0xa1 }) {
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