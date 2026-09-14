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

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

public class PerformanceTrackingInputStream extends FilterInputStream {
    private final PerformanceTracker performanceTracker;

    public PerformanceTrackingInputStream(final InputStream in, final PerformanceTracker performanceTracker) {
        super(in);
        this.performanceTracker = performanceTracker;
    }

    @Override
    public long skip(final long n) throws IOException {
        performanceTracker.beginContentRead();
        try {
            return super.skip(n);
        } finally {
            performanceTracker.endContentRead();
        }
    }

    @Override
    public int read() throws IOException {
        performanceTracker.beginContentRead();
        try {
            return super.read();
        } finally {
            performanceTracker.endContentRead();
        }
    }

    @Override
    public int read(final byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
        performanceTracker.beginContentRead();
        try {
            return super.read(b, off, len);
        } finally {
            performanceTracker.endContentRead();
        }
    }
}
