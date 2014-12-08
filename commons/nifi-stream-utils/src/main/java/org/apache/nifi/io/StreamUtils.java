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
package org.apache.nifi.io;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.nifi.io.exception.BytePatternNotFoundException;
import org.apache.nifi.io.util.NonThreadSafeCircularBuffer;

public class StreamUtils {

    public static long copy(final InputStream source, final OutputStream destination) throws IOException {
        final byte[] buffer = new byte[8192];
        int len;
        long totalCount = 0L;
        while ((len = source.read(buffer)) > 0) {
            destination.write(buffer, 0, len);
            totalCount += len;
        }
        return totalCount;
    }

    /**
     * Copies <code>numBytes</code> from <code>source</code> to
     * <code>destination</code>. If <code>numBytes</code> are not available from
     * <code>source</code>, throws EOFException
     *
     * @param source
     * @param destination
     * @param numBytes
     * @throws IOException
     */
    public static void copy(final InputStream source, final OutputStream destination, final long numBytes) throws IOException {
        final byte[] buffer = new byte[8192];
        int len;
        long bytesLeft = numBytes;
        while ((len = source.read(buffer, 0, (int) Math.min(bytesLeft, buffer.length))) > 0) {
            destination.write(buffer, 0, len);
            bytesLeft -= len;
        }

        if (bytesLeft > 0) {
            throw new EOFException("Attempted to copy " + numBytes + " bytes but only " + (numBytes - bytesLeft) + " bytes were available");
        }
    }

    /**
     * Reads data from the given input stream, copying it to the destination
     * byte array. If the InputStream has less data than the given byte array,
     * throws an EOFException
     *
     * @param source
     * @param destination
     * @throws IOException
     */
    public static void fillBuffer(final InputStream source, final byte[] destination) throws IOException {
        fillBuffer(source, destination, true);
    }

    /**
     * Reads data from the given input stream, copying it to the destination
     * byte array. If the InputStream has less data than the given byte array,
     * throws an EOFException if <code>ensureCapacity</code> is true and
     * otherwise returns the number of bytes copied
     *
     * @param source
     * @param destination
     * @param ensureCapacity whether or not to enforce that the InputStream have
     * at least as much data as the capacity of the destination byte array
     * @return 
     * @throws IOException
     */
    public static int fillBuffer(final InputStream source, final byte[] destination, final boolean ensureCapacity) throws IOException {
        int bytesRead = 0;
        int len;
        while (bytesRead < destination.length) {
            len = source.read(destination, bytesRead, destination.length - bytesRead);
            if (len < 0) {
                if (ensureCapacity) {
                    throw new EOFException();
                } else {
                    break;
                }
            }

            bytesRead += len;
        }

        return bytesRead;
    }

    /**
     * Copies data from in to out until either we are out of data (returns null)
     * or we hit one of the byte patterns identified by the
     * <code>stoppers</code> parameter (returns the byte pattern matched). The
     * bytes in the stopper will be copied.
     *
     * @param in
     * @param out
     * @param maxBytes
     * @param stoppers
     * @return the byte array matched, or null if end of stream was reached
     * @throws IOException
     */
    public static byte[] copyInclusive(final InputStream in, final OutputStream out, final int maxBytes, final byte[]... stoppers) throws IOException {
        if (stoppers.length == 0) {
            return null;
        }

        final List<NonThreadSafeCircularBuffer> circularBuffers = new ArrayList<NonThreadSafeCircularBuffer>();
        for (final byte[] stopper : stoppers) {
            circularBuffers.add(new NonThreadSafeCircularBuffer(stopper));
        }

        long bytesRead = 0;
        while (true) {
            final int next = in.read();
            if (next == -1) {
                return null;
            } else if (maxBytes > 0 && ++bytesRead >= maxBytes) {
                throw new BytePatternNotFoundException("Did not encounter any byte pattern that was expected; data does not appear to be in the expected format");
            }

            out.write(next);

            for (final NonThreadSafeCircularBuffer circ : circularBuffers) {
                if (circ.addAndCompare((byte) next)) {
                    return circ.getByteArray();
                }
            }
        }
    }

    /**
     * Copies data from in to out until either we are out of data (returns null)
     * or we hit one of the byte patterns identified by the
     * <code>stoppers</code> parameter (returns the byte pattern matched). The
     * byte pattern matched will NOT be copied to the output and will be un-read
     * from the input.
     *
     * @param in
     * @param out
     * @param maxBytes
     * @param stoppers
     * @return the byte array matched, or null if end of stream was reached
     * @throws IOException
     */
    public static byte[] copyExclusive(final InputStream in, final OutputStream out, final int maxBytes, final byte[]... stoppers) throws IOException {
        if (stoppers.length == 0) {
            return null;
        }

        int longest = 0;
        NonThreadSafeCircularBuffer longestBuffer = null;
        final List<NonThreadSafeCircularBuffer> circularBuffers = new ArrayList<NonThreadSafeCircularBuffer>();
        for (final byte[] stopper : stoppers) {
            final NonThreadSafeCircularBuffer circularBuffer = new NonThreadSafeCircularBuffer(stopper);
            if (stopper.length > longest) {
                longest = stopper.length;
                longestBuffer = circularBuffer;
                circularBuffers.add(0, circularBuffer);
            } else {
                circularBuffers.add(circularBuffer);
            }
        }

        long bytesRead = 0;
        while (true) {
            final int next = in.read();
            if (next == -1) {
                return null;
            } else if (maxBytes > 0 && bytesRead++ > maxBytes) {
                throw new BytePatternNotFoundException("Did not encounter any byte pattern that was expected; data does not appear to be in the expected format");
            }

            for (final NonThreadSafeCircularBuffer circ : circularBuffers) {
                if (circ.addAndCompare((byte) next)) {
                    // The longest buffer has some data that may not have been written out yet; we need to make sure
                    // that we copy out those bytes.
                    final int bytesToCopy = longest - circ.getByteArray().length;
                    for (int i = 0; i < bytesToCopy; i++) {
                        final int oldestByte = longestBuffer.getOldestByte();
                        if (oldestByte != -1) {
                            out.write(oldestByte);
                            longestBuffer.addAndCompare((byte) 0);
                        }
                    }

                    return circ.getByteArray();
                }
            }

            if (longestBuffer.isFilled()) {
                out.write(longestBuffer.getOldestByte());
            }
        }
    }

    /**
     * Skips the specified number of bytes from the InputStream
     *
     * If unable to skip that number of bytes, throws EOFException
     *
     * @param stream
     * @param bytesToSkip
     * @throws IOException
     */
    public static void skip(final InputStream stream, final long bytesToSkip) throws IOException {
        if (bytesToSkip <= 0) {
            return;
        }
        long totalSkipped = 0L;

        // If we have a FileInputStream, calling skip(1000000) will return 1000000 even if the file is only
        // 3 bytes. As a result, we will skip 1 less than the number requested, and then read the last
        // byte in order to make sure that we've consumed the number of bytes requested. We then check that
        // the final byte, which we read, is not -1.
        final long actualBytesToSkip = bytesToSkip - 1;
        while (totalSkipped < actualBytesToSkip) {
            final long skippedThisIteration = stream.skip(actualBytesToSkip - totalSkipped);
            if (skippedThisIteration == 0) {
                final int nextByte = stream.read();
                if (nextByte == -1) {
                    throw new EOFException();
                } else {
                    totalSkipped++;
                }
            }

            totalSkipped += skippedThisIteration;
        }

        final int lastByte = stream.read();
        if (lastByte == -1) {
            throw new EOFException();
        }
    }
}
