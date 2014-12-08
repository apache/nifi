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

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Wraps and InputStream so that the underlying InputStream cannot be closed.
 * This is used so that the InputStream can be wrapped with yet another
 * InputStream and prevent the outer layer from closing the inner InputStream
 */
public class NonCloseableInputStream extends FilterInputStream {

    private final InputStream toWrap;

    public NonCloseableInputStream(final InputStream toWrap) {
        super(toWrap);
        this.toWrap = toWrap;
    }

    @Override
    public int read() throws IOException {
        return toWrap.read();
    }

    @Override
    public int read(byte[] b) throws IOException {
        return toWrap.read(b);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return toWrap.read(b, off, len);
    }

    @Override
    public void close() throws IOException {
        // do nothing
    }
}
