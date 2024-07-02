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

public class PCAPHeader {
    static final int PCAP_HEADER_LENGTH = 24;
    private final byte[] magicNumber;
    private final int versionMajor;
    private final int versionMinor;
    private final int thiszone;
    private final long sigfigs;
    private final long snaplen;
    private final long network;

    public PCAPHeader(ByteBufferReader io) {
        this.magicNumber = io.readBytes(4);
        this.versionMajor = io.readU2();
        this.versionMinor = io.readU2();
        this.thiszone = io.readS4();
        this.sigfigs = io.readU4();
        this.snaplen = io.readU4();
        this.network = io.readU4();
    }

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
}