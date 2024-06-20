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

public class Packet {
    static final int PACKET_HEADER_LENGTH = 16;

    private ByteBufferReader io;
    private long tsSec;
    private long tsUsec;
    private long inclLen;
    private long origLen;
    private long expectedLength;
    private int totalLength;
    private PCAP root;
    private byte[] rawBody;
    private String invalidityReason;

    public Packet(ByteBufferReader io, PCAP root) {
        this.root = root;
        this.io = io;
        read();
    }

    public Packet(byte[] headerArray, PCAP root) {
        this.root = root;
        this.io = new ByteBufferReader(headerArray);
        read();
    }

    public Packet(long tSSec, long tSUsec, long inclLen, long origLen, byte[] rawBody) {
        // packet header properties
        this.tsSec = tSSec;
        this.tsUsec = tSUsec;
        this.inclLen = inclLen;
        this.origLen = origLen;

        // packet calculated properties
        this.expectedLength = inclLen;
        this.totalLength = PACKET_HEADER_LENGTH + rawBody.length;
        this.rawBody = rawBody;
        this.setValidity();
    }

    private void read() {
        this.tsSec = this.io.readU4();
        this.tsUsec = this.io.readU4();
        this.inclLen = this.io.readU4();
        this.origLen = this.io.readU4();

        this.expectedLength = Math.min(inclLen(), root().getHeader().snaplen());

        if (this.io.bytesLeft() >= expectedLength) {
            this.rawBody = this.io.readBytes(expectedLength);
        } else {
            this.rawBody = new byte[0];
        }
        this.setValidity();
        this.totalLength = PACKET_HEADER_LENGTH + this.rawBody.length;
    }

    private void setValidity() {
        this.invalidityReason = null;
        if (this.rawBody.length == 0) {
            this.invalidityReason = "Packet body is empty";
        } else if (this.inclLen > this.origLen) {
            this.invalidityReason = "The reported length of this packet exceeds the reported length of the original packet "
                    + "as sent on the network; (" + this.inclLen + " > " + this.origLen + ")";
        } else if (this.origLen == 0) {
            this.invalidityReason = "The reported original length of this packet as send on the network is 0.";
        } else if (this.inclLen == 0) {
            this.invalidityReason = "The reported length of this packet is 0.";
        }
    }

    public boolean isInvalid() {
        return this.invalidityReason != null;
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
     * "https://wiki.wireshark.org/Development/LibpcapFileFormat#Packet_Data">Source</a>
     */
    public PCAP root() {
        return root;
    }

    public byte[] rawBody() {
        return rawBody;
    }

    public long expectedLength() {
        return expectedLength;
    }

    public int totalLength() {
        return totalLength;
    }

    public String invalidityReason() {
        return invalidityReason;
    }

    public void setBody(byte[] newBody) {
        this.rawBody = newBody;
        this.setValidity();
        this.totalLength = PACKET_HEADER_LENGTH + rawBody.length;
    }
}