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
package org.apache.nifi.lookup;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;

/**
 * An InputStream wrapper that preserves streaming from a source InputStream while providing
 * robust mark/reset support. Bytes read from the source are spooled to an on-disk temporary
 * file so that reset() can reposition the stream to the last marked location regardless of
 * how many bytes were read after mark().
 * Behavior notes:
 * - mark() records the current absolute position.
 * - reset() replays bytes starting from the last marked position. If additional bytes are requested
 *   beyond the current replay window, the implementation transparently continues reading from the
 *   underlying source while spooling, preserving the streaming behavior.
 * - The temporary file is deleted when the stream is closed.
 */
class SpoolingMarkedInputStream extends FilterInputStream implements Closeable {
    private static final int DEFAULT_COPY_BUFFER = 8192;


    private final OutputStream spoolOut;
    private final File spoolFile;

    // Absolute positions from the beginning of the stream
    private long absoluteReadPos = 0L;        // next absolute byte index to be read from the consumer perspective
    private long absoluteSpoolPos = 0L;       // number of bytes written to spool

    // Replay state
    private boolean marked = false;
    private long markPos = 0L;
    private FileInputStream replayIn;         // when not null, we are replaying from the spool file

    SpoolingMarkedInputStream(final InputStream source) throws IOException {
        super(source);
        this.spoolFile = Files.createTempFile("nifi-restlookup-spool", ".tmp").toFile();
        this.spoolFile.deleteOnExit();
        this.spoolOut = new FileOutputStream(spoolFile);
    }

    @Override
    public synchronized void mark(final int readlimit) {
        // record absolute position; we do not rely on read-limit
        marked = true;
        markPos = absoluteReadPos;
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public synchronized void reset() throws IOException {
        if (!marked) {
            throw new IOException("reset() called without a preceding mark()");
        }
        ensureReplayOpenAt(markPos);
        absoluteReadPos = markPos;
    }

    @Override
    public int read() throws IOException {
        final byte[] one = new byte[1];
        final int r = read(one, 0, 1);
        return r < 0 ? -1 : (one[0] & 0xFF);
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
        if (len == 0) {
            return 0;
        }

        int totalRead = 0;

        // First, if in replay mode, read from the spool file until we reach the current absoluteSpoolPos
        if (replayIn != null) {
            final int availableForReplay = (int) Math.min(len, absoluteSpoolPos - absoluteReadPos);
            if (availableForReplay > 0) {
                final int r = replayIn.read(b, off, availableForReplay);
                if (r > 0) {
                    totalRead += r;
                    absoluteReadPos += r;
                }
            }

            if (absoluteReadPos >= absoluteSpoolPos) {
                // Finished replaying current window; close replay stream so subsequent reads come from source
                closeReplay();
            }
        }

        // Then, if more bytes requested, continue pulling from source and spooling them
        if (totalRead < len) {
            final int r = in.read(b, off + totalRead, len - totalRead);
            if (r > 0) {
                // Write the newly read bytes into the spool file to extend the replay window
                spoolOut.write(b, off + totalRead, r);
                spoolOut.flush();
                absoluteSpoolPos += r;
                absoluteReadPos += r;
                totalRead += r;
            }
        }

        return totalRead == 0 ? -1 : totalRead;
    }

    private void ensureReplayOpenAt(final long position) throws IOException {
        if (replayIn != null) {
            replayIn.close();
        }
        replayIn = new FileInputStream(spoolFile);
        long skipped = 0L;
        while (skipped < position) {
            final long s = replayIn.skip(position - skipped);
            if (s <= 0) {
                // In case skip cannot advance, read and discard
                final long remaining = position - skipped;
                final int toRead = (int) Math.min(remaining, DEFAULT_COPY_BUFFER);
                final byte[] buf = new byte[toRead];
                final int r = replayIn.read(buf);
                if (r < 0) {
                    break;
                }
                skipped += r;
            } else {
                skipped += s;
            }
        }
    }

    private void closeReplay() throws IOException {
        if (replayIn != null) {
            replayIn.close();
            replayIn = null;
        }
    }

    @Override
    public void close() throws IOException {
        IOException first = null;
        try {
            super.close();
        } catch (final IOException e) {
            first = e;
        }
        try {
            closeReplay();
            spoolOut.close();
        } catch (final IOException e) {
            if (first == null) first = e;
        } finally {
            // Attempt to delete spool file
            Files.deleteIfExists(spoolFile.toPath());
        }
        if (first != null) throw first;
    }
}
