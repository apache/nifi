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
package org.apache.nifi.remote.io;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

public class CompressionInputStream extends InputStream {

    private final InputStream in;
    private final Inflater inflater;

    private byte[] compressedBuffer;
    private byte[] buffer;

    private int bufferIndex;
    private boolean eos = false;    // whether or not we've reached the end of stream
    private boolean allDataRead = false;    // different from eos b/c eos means allDataRead == true && buffer is empty

    private final byte[] fourByteBuffer = new byte[4];

    public CompressionInputStream(final InputStream in) {
        this.in = in;
        inflater = new Inflater();

        buffer = new byte[0];
        compressedBuffer = new byte[0];
        bufferIndex = 1;
    }

    private String toHex(final byte[] array) {
        final StringBuilder sb = new StringBuilder("0x");
        for (final byte b : array) {
            final String hex = Integer.toHexString(b).toUpperCase();
            if (hex.length() == 1) {
                sb.append("0");
            }
            sb.append(hex);
        }
        return sb.toString();
    }

    protected void readChunkHeader() throws IOException {
        // Ensure that we have a valid SYNC chunk
        fillBuffer(fourByteBuffer);
        if (!Arrays.equals(CompressionOutputStream.SYNC_BYTES, fourByteBuffer)) {
            throw new IOException("Invalid CompressionInputStream. Expected first 4 bytes to be 'SYNC' but were " + toHex(fourByteBuffer));
        }

        // determine the size of the decompressed buffer
        fillBuffer(fourByteBuffer);
        buffer = new byte[toInt(fourByteBuffer)];

        // determine the size of the compressed buffer
        fillBuffer(fourByteBuffer);
        compressedBuffer = new byte[toInt(fourByteBuffer)];

        bufferIndex = buffer.length;  // indicate that buffer is empty
    }

    private int toInt(final byte[] data) {
        return ((data[0] & 0xFF) << 24)
                | ((data[1] & 0xFF) << 16)
                | ((data[2] & 0xFF) << 8)
                | (data[3] & 0xFF);
    }

    protected void bufferAndDecompress() throws IOException {
        if (allDataRead) {
            eos = true;
            return;
        }

        readChunkHeader();
        fillBuffer(compressedBuffer);

        inflater.setInput(compressedBuffer);
        try {
            inflater.inflate(buffer);
        } catch (final DataFormatException e) {
            throw new IOException(e);
        }
        inflater.reset();

        bufferIndex = 0;
        final int moreDataByte = in.read();
        if (moreDataByte < 1) {
            allDataRead = true;
        } else if (moreDataByte > 1) {
            throw new IOException("Expected indicator of whether or not more data was to come (-1, 0, or 1) but got " + moreDataByte);
        }
    }

    private void fillBuffer(final byte[] buffer) throws IOException {
        int len;
        int bytesLeft = buffer.length;
        int bytesRead = 0;
        while (bytesLeft > 0 && (len = in.read(buffer, bytesRead, bytesLeft)) > 0) {
            bytesLeft -= len;
            bytesRead += len;
        }

        if (bytesRead < buffer.length) {
            throw new EOFException();
        }
    }

    private boolean isBufferEmpty() {
        return bufferIndex >= buffer.length;
    }

    @Override
    public int read() throws IOException {
        if (eos) {
            return -1;
        }

        if (isBufferEmpty()) {
            bufferAndDecompress();
        }

        if (isBufferEmpty()) {
            eos = true;
            return -1;
        }

        return buffer[bufferIndex++] & 0xFF;
    }

    @Override
    public int read(final byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
        if (eos) {
            return -1;
        }

        if (isBufferEmpty()) {
            bufferAndDecompress();
        }

        if (isBufferEmpty()) {
            eos = true;
            return -1;
        }

        final int free = buffer.length - bufferIndex;
        final int bytesToTransfer = Math.min(len, free);
        System.arraycopy(buffer, bufferIndex, b, off, bytesToTransfer);
        bufferIndex += bytesToTransfer;

        return bytesToTransfer;
    }

    /**
     * Calls {@link Inflater#end()} to free acquired memory to prevent OutOfMemory error.
     * However, does NOT close underlying InputStream.
     *
     * @throws java.io.IOException for any issues closing underlying stream
     */
    @Override
    public void close() throws IOException {
        inflater.end();
    }
}
