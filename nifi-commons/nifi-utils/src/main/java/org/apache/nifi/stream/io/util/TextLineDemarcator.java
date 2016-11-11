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
 * NOTE: Not intended for multi-thread usage hence not Thread-safe.
 * </p>
 */
public class TextLineDemarcator extends AbstractDemarcator {

    private static int CR = 13; // \r

    private static int LF = 10; // \n

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
        super(is, Integer.MAX_VALUE, initialBufferSize);
    }

    /**
     * Will compute the next <i>offset info</i> for a text line (line terminated
     * by either '\r', '\n' or '\r\n'). <br>
     * The <i>offset info</i> computed and returned as {@link OffsetInfo} where
     * {@link OffsetInfo#isStartsWithMatch()} will always return true.
     *
     * @return offset info
     */
    public OffsetInfo nextOffsetInfo() throws IOException {
        return this.nextOffsetInfo(null);
    }

    /**
     * Will compute the next <i>offset info</i> for a text line (line terminated
     * by either '\r', '\n' or '\r\n'). <br>
     * The <i>offset info</i> computed and returned as {@link OffsetInfo} where
     * {@link OffsetInfo#isStartsWithMatch()} will return true if
     * <code>startsWith</code> was successfully matched with the starting bytes
     * of the text line.
     *
     * NOTE: The reason for 2 'nextOffsetInfo(..)' operations is that the
     * 'startsWith' argument will force the actual token to be extracted and
     * then matched introducing the overhead for System.arrayCopy and matching
     * logic which is an optional scenario and is avoided all together if
     * 'startsWith' is not provided (i.e., null).
     *
     * @return offset info
     */
    public OffsetInfo nextOffsetInfo(byte[] startsWith) throws IOException {
        OffsetInfo offsetInfo = null;
        byte previousByteVal = 0;
        byte[] data = null;
        nextTokenLoop:
        while (data == null && this.availableBytesLength != -1) {
            if (this.index >= this.availableBytesLength) {
                this.fill();
            }
            int delimiterSize = 0;
            if (this.availableBytesLength != -1) {
                byte byteVal;
                int i;
                for (i = this.index; i < this.availableBytesLength; i++) {
                    byteVal = this.buffer[i];

                    if (byteVal == LF) {
                        delimiterSize = previousByteVal == CR ? 2 : 1;
                    } else if (previousByteVal == CR) {
                        delimiterSize = 1;
                        i--;
                    }
                    previousByteVal = byteVal;
                    if (delimiterSize > 0) {
                        this.index = i + 1;
                        int size = Math.max(1, this.index - this.mark);
                        offsetInfo = new OffsetInfo(this.offset, size, delimiterSize);
                        this.offset += size;
                        if (startsWith != null) {
                            data = this.extractDataToken(size);
                        }
                        this.mark = this.index;
                        break nextTokenLoop;
                    }
                }
                this.index = i;
            } else {
                delimiterSize = previousByteVal == CR || previousByteVal == LF ? 1 : 0;
                if (offsetInfo == null) {
                    int size = this.index - this.mark;
                    if (size > 0) {
                        offsetInfo = new OffsetInfo(this.offset, size, delimiterSize);
                        this.offset += size;
                    }
                }
                if (startsWith != null) {
                    data = this.extractDataToken(this.index - this.mark);
                }
            }
        }

        if (startsWith != null && data != null) {
            if (startsWith.length > data.length) {
                offsetInfo.setStartsWithMatch(false);
            } else {
                for (int i = 0; i < startsWith.length; i++) {
                    byte sB = startsWith[i];
                    if (sB != data[i]) {
                        offsetInfo.setStartsWithMatch(false);
                        break;
                    }
                }
            }
        }
        return offsetInfo;
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

        private OffsetInfo(long startOffset, long length, int crlfLength) {
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

        void setStartsWithMatch(boolean startsWithMatch) {
            this.startsWithMatch = startsWithMatch;
        }

        @Override
        public String toString() {
            return "offset:" + this.startOffset + "; length:" + this.length + "; crlfLength:" + this.crlfLength;
        }
    }
}
