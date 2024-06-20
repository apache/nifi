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

public class ByteBufferReader {
    final private ByteBuffer buffer;

    public ByteBufferReader(byte[] byteArray) {
        this.buffer = ByteBuffer.wrap(byteArray);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
    }

    public int readU2() {
        return (buffer.getShort() & 0xffff);
    }

    public long readU4() {
        return ((long) buffer.getInt() & 0xffffffffL);
    }

    public int readS4() {
        return buffer.getInt();
    }

    public byte[] readBytes(int n) {
        byte[] output = new byte[n];
        buffer.get(output);
        return output;
    }

    public byte[] readBytes(long n) {
        return readBytes((int) n);
    }

    public int bytesLeft() {
        return buffer.remaining();
    }

    public boolean hasRemaining() {
        return buffer.hasRemaining();
    }
}