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

public class ByteBufferInterface {
   final private ByteBuffer buffer;

    public ByteBufferInterface(byte[] byteArray) {
        this.buffer = ByteBuffer.wrap(byteArray);
    }

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

    public int bytesLeft() {
        return buffer.remaining();
    }

    public boolean isEof() {
        return !buffer.hasRemaining();
    }
}