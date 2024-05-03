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

package org.apache.nifi.controller.repository.metrics;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class PerformanceTrackingOutputStream extends FilterOutputStream {
    private final PerformanceTracker performanceTracker;

    public PerformanceTrackingOutputStream(final OutputStream out, final PerformanceTracker performanceTracker) {
        super(out);
        this.performanceTracker = performanceTracker;
    }

    @Override
    public void write(final int b) throws IOException {
        performanceTracker.beginContentWrite();
        try {
            out.write(b);
        } finally {
            performanceTracker.endContentWrite();
        }
    }

    @Override
    public void write(final byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    @Override
    public void write(final byte[] b, final int off, final int len) throws IOException {
        performanceTracker.beginContentWrite();
        try {
            out.write(b, off, len);
        } finally {
            performanceTracker.endContentWrite();
        }
    }

    @Override
    public void close() throws IOException {
        performanceTracker.beginContentWrite();
        try {
            super.close();
        } finally {
            performanceTracker.endContentWrite();
        }
    }

    @Override
    public void flush() throws IOException {
        performanceTracker.beginContentWrite();
        try {
            super.flush();
        } finally {
            performanceTracker.endContentWrite();
        }
    }
}
