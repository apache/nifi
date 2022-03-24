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

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.Deflater;

public class CompressionOutputStream extends OutputStream {

    public static final byte[] SYNC_BYTES = new byte[]{'S', 'Y', 'N', 'C'};

    public static final int DEFAULT_COMPRESSION_LEVEL = 1;
    public static final int DEFAULT_BUFFER_SIZE = 64 << 10;
    public static final int MIN_BUFFER_SIZE = 8 << 10;

    private final OutputStream out;
    private final Deflater deflater;

    private final byte[] buffer;
    private final byte[] compressed;

    private int bufferIndex = 0;
    private boolean dataWritten = false;

    public CompressionOutputStream(final OutputStream outStream) {
        this(outStream, DEFAULT_BUFFER_SIZE);
    }

    public CompressionOutputStream(final OutputStream outStream, final int bufferSize) {
        this(outStream, bufferSize, DEFAULT_COMPRESSION_LEVEL, Deflater.DEFAULT_STRATEGY);
    }

    public CompressionOutputStream(final OutputStream outStream, final int bufferSize, final int level, final int strategy) {
        if (bufferSize < MIN_BUFFER_SIZE) {
            throw new IllegalArgumentException("Buffer size must be at least " + MIN_BUFFER_SIZE);
        }

        this.out = outStream;
        this.deflater = new Deflater(level);
        this.deflater.setStrategy(strategy);
        buffer = new byte[bufferSize];
        compressed = new byte[bufferSize + 64];
    }

    /**
     * Compresses the currently buffered chunk of data and sends it to the output stream
     *
     * @throws IOException if issues occur writing to stream
     */
    protected void compressAndWrite() throws IOException {
        if (bufferIndex <= 0) {
            return;
        }

        deflater.setInput(buffer, 0, bufferIndex);
        deflater.finish();
        final int compressedBytes = deflater.deflate(compressed);

        writeChunkHeader(compressedBytes);
        out.write(compressed, 0, compressedBytes);

        bufferIndex = 0;
        deflater.reset();
    }

    private void writeChunkHeader(final int compressedBytes) throws IOException {
        // If we have already written data, write out a '1' to indicate that we have more data; when we close
        // the stream, we instead write a '0' to indicate that we are finished sending data.
        if (dataWritten) {
            out.write(1);
        }
        out.write(SYNC_BYTES);
        dataWritten = true;

        writeInt(out, bufferIndex);
        writeInt(out, compressedBytes);
    }

    private void writeInt(final OutputStream out, final int val) throws IOException {
        out.write(val >>> 24);
        out.write(val >>> 16);
        out.write(val >>> 8);
        out.write(val);
    }

    protected boolean bufferFull() {
        return bufferIndex >= buffer.length;
    }

    @Override
    public void write(final int b) throws IOException {
        buffer[bufferIndex++] = (byte) (b & 0xFF);
        if (bufferFull()) {
            compressAndWrite();
        }
    }

    @Override
    public void write(final byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    @Override
    public void write(final byte[] b, final int off, final int len) throws IOException {
        int bytesLeft = len;
        while (bytesLeft > 0) {
            final int free = buffer.length - bufferIndex;
            final int bytesThisIteration = Math.min(bytesLeft, free);
            System.arraycopy(b, off + len - bytesLeft, buffer, bufferIndex, bytesThisIteration);
            bufferIndex += bytesThisIteration;

            bytesLeft -= bytesThisIteration;
            if (bufferFull()) {
                compressAndWrite();
            }
        }
    }

    @Override
    public void flush() throws IOException {
        compressAndWrite();
        super.flush();
    }

    /**
     * Flushes remaining buffer and calls {@link Deflater#end()} to free acquired memory to prevent OutOfMemory error.
     * @throws IOException for any issues closing underlying stream
     */
    @Override
    public void close() throws IOException {
        compressAndWrite();
        out.write(0);   // indicate that the stream is finished.
        out.flush();
        deflater.end();
    }
}
