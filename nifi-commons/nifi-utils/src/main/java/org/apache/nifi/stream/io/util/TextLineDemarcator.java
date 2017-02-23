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
package org.apache.nifi.stream.io.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;

/**
 * Implementation of demarcator of text lines in the provided
 * {@link InputStream}. It works similar to the {@link BufferedReader} and its
 * {@link BufferedReader#readLine()} methods except that it does not create a
 * String representing the text line and instead returns the offset info for the
 * computed text line. See {@link #nextOffsetInfo()} and
 * {@link #nextOffsetInfo(byte[])} for more details.
 * <p>
 * This class is NOT thread-safe.
 * </p>
 */
public class TextLineDemarcator {

    private final static int INIT_BUFFER_SIZE = 8192;

    private final InputStream is;

    private final int initialBufferSize;

    private byte[] buffer;

    private int index;

    private int mark;

    private long offset;

    private int bufferLength;

    /**
     * Constructs an instance of demarcator with provided {@link InputStream}
     * and default buffer size.
     */
    public TextLineDemarcator(InputStream is) {
        this(is, INIT_BUFFER_SIZE);
    }

    /**
     * Constructs an instance of demarcator with provided {@link InputStream}
     * and initial buffer size.
     */
    public TextLineDemarcator(InputStream is, int initialBufferSize) {
        if (is == null) {
            throw new IllegalArgumentException("'is' must not be null.");
        }
        if (initialBufferSize < 1) {
            throw new IllegalArgumentException("'initialBufferSize' must be > 0.");
        }
        this.is = is;
        this.initialBufferSize = initialBufferSize;
        this.buffer = new byte[initialBufferSize];
    }

    /**
     * Will compute the next <i>offset info</i> for a text line (line terminated
     * by either '\r', '\n' or '\r\n'). <br>
     * The <i>offset info</i> computed and returned as {@link OffsetInfo} where
     * {@link OffsetInfo#isStartsWithMatch()} will always return true.
     *
     * @return offset info
     */
    public OffsetInfo nextOffsetInfo() {
        return this.nextOffsetInfo(null);
    }

    /**
     * Will compute the next <i>offset info</i> for a text line (line terminated
     * by either '\r', '\n' or '\r\n'). <br>
     * The <i>offset info</i> computed and returned as {@link OffsetInfo} where
     * {@link OffsetInfo#isStartsWithMatch()} will return true if
     * <code>startsWith</code> was successfully matched with the stsarting bytes
     * of the text line.
     *
     * @return offset info
     */
    public OffsetInfo nextOffsetInfo(byte[] startsWith) {
        OffsetInfo offsetInfo = null;
        int lineLength = 0;
        byte[] token = null;
        lineLoop:
        while (this.bufferLength != -1) {
            if (this.index >= this.bufferLength) {
                this.fill();
            }
            if (this.bufferLength != -1) {
                int i;
                byte byteVal;
                for (i = this.index; i < this.bufferLength; i++) {
                    byteVal = this.buffer[i];
                    lineLength++;
                    int crlfLength = computeEol(byteVal, i + 1);
                    if (crlfLength > 0) {
                        i += crlfLength;
                        if (crlfLength == 2) {
                            lineLength++;
                        }
                        offsetInfo = new OffsetInfo(this.offset, lineLength, crlfLength);
                        if (startsWith != null) {
                            token = this.extractDataToken(lineLength);
                        }
                        this.mark = this.index;
                        break lineLoop;
                    }
                }
                this.index = i;
            }
        }
        // EOF where last char(s) are not CRLF.
        if (lineLength > 0 && offsetInfo == null) {
            offsetInfo = new OffsetInfo(this.offset, lineLength, 0);
            if (startsWith != null) {
                token = this.extractDataToken(lineLength);
            }
        }
        this.offset += lineLength;

        // checks if the new line starts with 'startsWith' chars
        if (startsWith != null) {
            for (int i = 0; i < startsWith.length; i++) {
                byte sB = startsWith[i];
                if (token != null && sB != token[i]) {
                    offsetInfo.setStartsWithMatch(0);
                    break;
                }
            }
        }
        return offsetInfo;
    }

    /**
     * Determines if the line terminates. Returns int specifying the length of
     * the CRLF (i.e., only CR or LF or CR and LF) and therefore can only have
     * values of:
     *   0 - not the end of the line
     *   1 - the end of the line either via CR or LF
     *   2 - the end of the line with both CR and LF
     *
     * It performs the read ahead on the buffer if need to.
     */
    private int computeEol(byte currentByte, int providedIndex) {
        int actualIndex = providedIndex - 1;
        boolean readAhead = false;
        int crlfLength = 0;
        if (currentByte == '\n') {
            crlfLength = 1;
        } else if (currentByte == '\r') {
            if (providedIndex >= this.bufferLength) {
                this.index = this.bufferLength;
                this.fill();
                providedIndex = this.index;
                readAhead = true;
            }
            crlfLength = 1;
            if (providedIndex < this.buffer.length - 1) {
                currentByte = this.buffer[providedIndex];
                crlfLength = currentByte == '\n' ? 2 : 1;
            }
        }

        if (crlfLength > 0) {
            this.index = readAhead ? this.index + (crlfLength - 1) : (actualIndex + crlfLength);
        }

        return crlfLength;
    }

    private byte[] extractDataToken(int length) {
        byte[] data = null;
        if (length > 0) {
            data = new byte[length];
            System.arraycopy(this.buffer, this.mark, data, 0, data.length);
        }
        return data;
    }

    /**
     * Will fill the current buffer from current 'index' position, expanding it
     * and or shuffling it if necessary
     */
    private void fill() {
        if (this.index >= this.buffer.length) {
            if (this.mark == 0) { // expand
                byte[] newBuff = new byte[this.buffer.length + this.initialBufferSize];
                System.arraycopy(this.buffer, 0, newBuff, 0, this.buffer.length);
                this.buffer = newBuff;
            } else { // shuffle
                int length = this.index - this.mark;
                System.arraycopy(this.buffer, this.mark, this.buffer, 0, length);
                this.index = length;
                this.mark = 0;
            }
        }

        try {
            int bytesRead;
            do {
                bytesRead = this.is.read(this.buffer, this.index, this.buffer.length - this.index);
            } while (bytesRead == 0);
            this.bufferLength = bytesRead != -1 ? this.index + bytesRead : -1;
        } catch (IOException e) {
            throw new IllegalStateException("Failed while reading InputStream", e);
        }
    }

    /**
     * Container to hold offset and meta info for a computed text line.
     * The offset and meta info is represented with the following 4 values:
     *  <ul>
     *    <li><i>startOffset</i> - the offset in the overall stream which represents the beginning of the text line</li>
     *    <li><i>length</i> - length of the text line including CRLF characters</li>
     *    <li><i>crlfLength</i> - the length of the CRLF.
     *                            Value 0 is returned if text line represents the last text line in the
     *                            {@link InputStream} (i.e., EOF) and such line does not terminate with CR or LF or the combination of the two.
     *                            Value 1 is returned if text line ends with '\n' or '\r'.
     *                            Value 2 is returned if line ends with '\r\n').</li>
     *    <li><i>startsWithMatch</i> - <code>true</code> by default unless <code>startWith</code> bytes are provided and not matched.
     *                                 See {@link #nextOffsetInfo(byte[])} for more info.</li>
     *  </ul>
     **/
    public static class OffsetInfo {
        private final long startOffset, length;
        private final int crlfLength;

        private boolean startsWithMatch = true;

        OffsetInfo(long startOffset, long length, int crlfLength) {
            this.startOffset = startOffset;
            this.length = length;
            this.crlfLength = crlfLength;
        }

        public long getStartOffset() {
            return startOffset;
        }

        public long getLength() {
            return length;
        }

        public int getCrlfLength() {
            return this.crlfLength;
        }

        public boolean isStartsWithMatch() {
            return this.startsWithMatch;
        }

        void setStartsWithMatch(int startsWithMatch) {
            this.startsWithMatch = startsWithMatch == 1 ? true : false;
        }
    }
}
