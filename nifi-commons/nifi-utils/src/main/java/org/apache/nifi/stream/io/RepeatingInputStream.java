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
package org.apache.nifi.stream.io;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

public class RepeatingInputStream extends InputStream {
    private final byte[] toRepeat;
    private final int maxIterations;

    private InputStream bais;
    private int repeatCount;


    public RepeatingInputStream(final byte[] toRepeat, final int iterations) {
        if (iterations < 1) {
            throw new IllegalArgumentException();
        }
        if (Objects.requireNonNull(toRepeat).length == 0) {
            throw new IllegalArgumentException();
        }

        this.toRepeat = toRepeat;
        this.maxIterations = iterations;

        repeat();
        bais = new ByteArrayInputStream(toRepeat);
        repeatCount = 1;
    }

    @Override
    public int read() throws IOException {
        final int value = bais.read();
        if (value > -1) {
            return value;
        }

        final boolean repeated = repeat();
        if (repeated) {
            return bais.read();
        }

        return -1;
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
        final int value = bais.read(b, off, len);
        if (value > -1) {
            return value;
        }

        final boolean repeated = repeat();
        if (repeated) {
            return bais.read(b, off, len);
        }

        return -1;
    }

    @Override
    public int read(final byte[] b) throws IOException {
        final int value = bais.read(b);
        if (value > -1) {
            return value;
        }

        final boolean repeated = repeat();
        if (repeated) {
            return bais.read(b);
        }

        return -1;
    }

    private boolean repeat() {
        if (repeatCount >= maxIterations) {
            return false;
        }

        repeatCount++;
        bais = new ByteArrayInputStream(toRepeat);

        return true;
    }
}
