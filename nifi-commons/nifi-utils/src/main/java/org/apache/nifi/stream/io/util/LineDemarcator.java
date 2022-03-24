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

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;

/**
 * A demarcator that scans an InputStream for line endings (carriage returns and new lines) and returns
 * lines of text one-at-a-time. This is similar to BufferedReader but with a very important distinction: while
 * BufferedReader returns the lines of text after stripping off any line endings, this class returns text including the
 * line endings. So, for example, if the following text is provided:
 *
 * <code>ABC\rXYZ\nABCXYZ\r\nhello</code>
 *
 * Then calls to {@link #nextLine()} will result in 4 String values being returned:
 *
 * <ul>
 *     <li><code>ABC\r</code></li>
 *     <li><code>XYZ\n</code></li>
 *     <li><code>ABCXYZ\r\n</code></li>
 *     <li><code>hello</code></li>
 * </ul>
 *
 * All subsequent calls to {@link #nextLine()} will return <code>null</code>.
 */
public class LineDemarcator extends AbstractTextDemarcator {
    private static final char CARRIAGE_RETURN = '\r';
    private static final char NEW_LINE = '\n';

    private char lastChar;

    public LineDemarcator(final InputStream in, final Charset charset, final int maxDataSize, final int initialBufferSize) {
        this(new InputStreamReader(in, charset), maxDataSize, initialBufferSize);
    }

    public LineDemarcator(final Reader reader, final int maxDataSize, final int initialBufferSize) {
        super(reader, maxDataSize, initialBufferSize);
    }

    /**
     * Will read the next line of text from the {@link InputStream} returning null
     * when it reaches the end of the stream.
     *
     * @throws IOException if unable to read from the stream
     */
    public String nextLine() throws IOException {
        while (this.availableBytesLength != -1) {
            if (this.index >= this.availableBytesLength) {
                this.fill();
            }

            if (this.availableBytesLength != -1) {
                char charVal;
                int i;
                for (i = this.index; i < this.availableBytesLength; i++) {
                    charVal = this.buffer[i];

                    try {
                        if (charVal == NEW_LINE) {
                            this.index = i + 1;

                            final int size = this.index - this.mark;
                            final String line =  new String(this.buffer, mark, size);

                            this.mark = this.index;
                            return line;
                        } else if (lastChar == CARRIAGE_RETURN) {
                            // Point this.index to i+1 because that's the next byte that we want to consume.
                            this.index = i + 1;

                            // Size is equal to where the line began, up to index-1 because we don't want to consume the last byte encountered.
                            final int size = this.index - 1 - this.mark;
                            final String line = new String(this.buffer, mark, size);

                            // set 'mark' to index - 1 because we don't want to consume the last byte that we've encountered, since we're basing our
                            // line on the previous byte.
                            this.mark = this.index - 1;
                            return line;
                        }
                    } finally {
                        lastChar = charVal;
                    }
                }

                this.index = i;
            } else {
                final int size = this.index - this.mark;
                if (size == 0) {
                    return null;
                }

                return new String(this.buffer, mark, size);
            }
        }

        return null;
    }
}
