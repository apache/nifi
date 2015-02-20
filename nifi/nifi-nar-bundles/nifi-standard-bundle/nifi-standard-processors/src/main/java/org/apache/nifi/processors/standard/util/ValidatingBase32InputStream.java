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

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import org.apache.commons.codec.binary.Base32;


/**
 * An InputStream that throws an IOException if any byte is read that is not a
 * valid Base32 character. Whitespace is considered valid.
 */
public class ValidatingBase32InputStream extends FilterInputStream {

    private final Base32 b32 = new Base32();

    public ValidatingBase32InputStream(InputStream in) {
        super(in);
    }

    @Override
    public int read(byte[] b, int offset, int len) throws IOException {
        int numRead = super.read(b, offset, len);
        if (numRead > 0) {
            byte[] copy = b;
            if (numRead < b.length) {
                // isBase32 checks the whole length of byte[], we need to limit it to numRead
                copy = Arrays.copyOf(b, numRead);
            }
            if (!b32.isInAlphabet(copy, true)) {
                throw new IOException("Data is not base32 encoded.");
            }
        }
        return numRead;
    }

    @Override
    public int read(byte[] b) throws IOException {
        int numRead = super.read(b);
        if (numRead > 0) {
            byte[] copy = b;
            if (numRead < b.length) {
                // isBase32 checks the whole length of byte[], we need to limit it to numRead
                copy = Arrays.copyOf(b, numRead);
            }
            if (!b32.isInAlphabet(copy, true)) {
                throw new IOException("Data is not base32 encoded.");
            }
        }
        return numRead;
    }

    @Override
    public int read() throws IOException {
        int data = super.read();
        if (!b32.isInAlphabet((byte) data)) {
            throw new IOException("Data is not base32 encoded.");
        }
        return super.read();
    }
}
