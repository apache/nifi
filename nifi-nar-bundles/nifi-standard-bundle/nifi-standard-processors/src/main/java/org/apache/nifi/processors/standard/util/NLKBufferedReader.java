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
package org.apache.nifi.processors.standard.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;

//NLKBufferedReader = New Line Keeper Buffered Reader
public class NLKBufferedReader extends BufferedReader {

    private Reader in;
    private char cb[];
    private int nChars, nextChar;
    private static final int INVALIDATED = -2;
    private static final int UNMARKED = -1;
    private int markedChar = UNMARKED;
    private int readAheadLimit = 0; /* Valid only when markedChar > 0 */

    private static int defaultCharBufferSize = 8192;
    private static int defaultExpectedLineLength = 80;

    /**
     * Creates a buffering character-input stream that uses an input buffer of the specified size.
     *
     * @param in A Reader
     * @param sz Input-buffer size
     *
     * @exception IllegalArgumentException If sz is <= 0
     */
    public NLKBufferedReader(Reader in, int sz) {
        super(in, sz);
        this.in = in;
        cb = new char[sz];
        nextChar = nChars = 0;
    }

    /**
     * Creates a buffering character-input stream that uses a default-sized input buffer.
     *
     * @param in A Reader
     */
    public NLKBufferedReader(Reader in) {
        this(in, defaultCharBufferSize);
    }

    /**
     * Reads a line of text. A line is considered to be terminated by any one of a line feed ('\n'), a carriage return ('\r'), or a carriage return followed immediately by a linefeed.
     *
     * @return A String containing the contents of the line, including any line-termination characters, or null if the end of the stream has been reached
     *
     * @exception IOException If an I/O error occurs
     */
    @Override
    public String readLine() throws IOException {
        StringBuffer s = null;
        int startChar;

        synchronized (lock) {
            ensureOpen();

            bufferLoop:
            for (;;) {

                if (nextChar >= nChars) {
                    fill();
                }
                if (nextChar >= nChars) { /* EOF */

                    if (s != null && s.length() > 0) {
                        return s.toString();
                    } else {
                        return null;
                    }
                }
                boolean eol = false;
                char c = 0;
                int i;

                charLoop:
                for (i = nextChar; i < nChars; i++) {
                    c = cb[i];
                    if ((c == '\n') || (c == '\r')) {
                        if ((c == '\r') && (cb.length > i + 1) && cb[i + 1] == '\n') { // windows case '\r\n' here verify the next character i+1
                            i++;
                        }
                        eol = true;
                        break charLoop;
                    }
                }

                startChar = nextChar;
                nextChar = i;

                if (eol) {
                    String str;
                    if (s == null) {
                        str = new String(cb, startChar, (i + 1) - startChar);
                    } else {
                        s.append(cb, startChar, (i + 1) - startChar);
                        str = s.toString();
                    }
                    nextChar++;
                    return str;
                }

                if (s == null) {
                    s = new StringBuffer(defaultExpectedLineLength);
                }
                s.append(cb, startChar, i - startChar);
            }
        }
    }

    /**
     * Checks to make sure that the stream has not been closed
     */
    private void ensureOpen() throws IOException {
        if (in == null) {
            throw new IOException("Stream closed");
        }
    }

    /**
     * Fills the input buffer, taking the mark into account if it is valid.
     */
    private void fill() throws IOException {
        int dst;
        if (markedChar <= UNMARKED) {
            /* No mark */
            dst = 0;
        } else {
            /* Marked */
            int delta = nextChar - markedChar;
            if (delta >= readAheadLimit) {
                /* Gone past read-ahead limit: Invalidate mark */
                markedChar = INVALIDATED;
                readAheadLimit = 0;
                dst = 0;
            } else {
                if (readAheadLimit <= cb.length) {
                    /* Shuffle in the current buffer */
                    System.arraycopy(cb, markedChar, cb, 0, delta);
                    markedChar = 0;
                    dst = delta;
                } else {
                    /* Reallocate buffer to accommodate read-ahead limit */
                    char ncb[] = new char[readAheadLimit];
                    System.arraycopy(cb, markedChar, ncb, 0, delta);
                    cb = ncb;
                    markedChar = 0;
                    dst = delta;
                }
                nextChar = nChars = delta;
            }
        }

        int n;
        do {
            n = in.read(cb, dst, cb.length - dst);
        } while (n == 0);
        if (n > 0) {
            nChars = dst + n;
            nextChar = dst;
        }
    }
}
