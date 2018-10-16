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
    }

    /**
     * Creates a buffering character-input stream that uses a default-sized input buffer.
     *
     * @param in A Reader
     *
     */
    public NLKBufferedReader(Reader in) {
        super(in);
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
        StringBuilder stringBuilder = new StringBuilder();

        int intchar = read();
        while(intchar != -1){
            char c = (char) intchar;
            stringBuilder.append(c);

            if(c == '\n') {
                break;
            } else if(c == '\r'){
                // Peek at next character, check if it's \n
                int charPeek = peek();
                if(charPeek != -1 && (char)charPeek=='\n'){
                    stringBuilder.append((char)read());
                }
                break;
            }

            intchar = read();
        }

        String result = stringBuilder.toString();

        return result.length()==0?null:result;
    }

    public int peek() throws IOException {
        mark(1);
        int readByte = read();
        reset();

        return readByte;
    }
}
