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
    public NLKBufferedReader(Reader in, int sz) {
        super(in, sz);
    }

    public NLKBufferedReader(Reader in) {
        super(in);
    }

    /**
     * Reads a line of text in the same manner as {@link BufferedReader} except that any line-termination characters (\r and \n) are preserved in the String
     * that is returned from this reader, whereas {@link BufferedReader} will strip those out.
     *
     * @return A String containing the next line of text (including any line-termination characters) from the underlying Reader, or null if no more data is available
     *
     * @throws IOException If unable to read from teh underlying Reader
     */
    @Override
    public String readLine() throws IOException {
        final StringBuilder stringBuilder = new StringBuilder();

        int intchar = read();
        while (intchar != -1) {
            final char c = (char) intchar;
            stringBuilder.append(c);

            if (c == '\n') {
                break;
            } else if (c == '\r') {
                // Peek at next character, check if it's \n
                int charPeek = peek();
                if (charPeek == '\n') {
                    stringBuilder.append((char) read());
                }

                break;
            }

            intchar = read();
        }

        final String result = stringBuilder.toString();
        return (result.length() == 0) ? null : result;
    }

    public int peek() throws IOException {
        mark(1);
        int readByte = read();
        reset();

        return readByte;
    }
}
