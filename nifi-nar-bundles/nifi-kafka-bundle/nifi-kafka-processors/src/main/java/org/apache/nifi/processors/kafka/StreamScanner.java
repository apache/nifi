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
package org.apache.nifi.processors.kafka;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.util.Arrays;

import org.apache.nifi.stream.io.ByteArrayOutputStream;
import org.apache.nifi.stream.io.util.NonThreadSafeCircularBuffer;

/**
 *
 */
class StreamScanner {

    private final InputStream is;

    private final byte[] delimiter;

    private final NonThreadSafeCircularBuffer buffer;

    private final ByteArrayOutputStream baos;

    private byte[] data;

    private boolean eos;

    /**
     *
     */
    StreamScanner(InputStream is, String delimiter) {
        this.is = new BufferedInputStream(is);
        this.delimiter = delimiter.getBytes();
        buffer = new NonThreadSafeCircularBuffer(this.delimiter);
        baos = new ByteArrayOutputStream();
    }

    /**
     *
     */
    boolean hasNext() {
        this.data = null;
        if (!this.eos) {
            try {
                boolean keepReading = true;
                while (keepReading) {
                    byte b = (byte) this.is.read();
                    if (b > -1) {
                        baos.write(b);
                        if (buffer.addAndCompare(b)) {
                            this.data = Arrays.copyOfRange(baos.getUnderlyingBuffer(), 0, baos.size() - delimiter.length);
                            keepReading = false;
                        }
                    } else {
                        this.data = baos.toByteArray();
                        keepReading = false;
                        this.eos = true;
                    }
                }
                baos.reset();
            } catch (Exception e) {
                throw new IllegalStateException("Failed while reading InputStream", e);
            }
        }
        return this.data != null;
    }

    /**
     *
     */
    byte[] next() {
        return this.data;
    }

    void close() {
        this.baos.close();
    }
}
