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

public class NanoTimePerformanceTracker implements PerformanceTracker {
    private final Timer contentReadTimer = new Timer();
    private final Timer contentWriteTimer = new Timer();
    private final Timer sessionCommitTimer = new Timer();
    private int writeDepth = 0;

    @Override
    public void beginContentRead() {
        contentReadTimer.start();
    }

    @Override
    public void endContentRead() {
        contentReadTimer.stop();
    }

    @Override
    public long getContentReadNanos() {
        return contentReadTimer.get();
    }

    @Override
    public void beginContentWrite() {
        // Increase the write depth by 1 and start timer if the depth becomes 1.
        // We do this because in many cases, we may call .beginContentWrite() multiple times, then .endContentWrite().
        // For example, if an OutputStream is used, calls to close() or write() may then call flush(), so we don't want to
        writeDepth++;
        if (writeDepth == 1) {
            contentWriteTimer.start();
        }
    }

    @Override
    public void endContentWrite() {
        writeDepth--;
        if (writeDepth == 0) {
            contentWriteTimer.stop();
        }
    }

    @Override
    public long getContentWriteNanos() {
        return contentWriteTimer.get();
    }

    @Override
    public void beginSessionCommit() {
        sessionCommitTimer.start();
    }

    @Override
    public void endSessionCommit() {
        sessionCommitTimer.stop();
    }

    @Override
    public long getSessionCommitNanos() {
        return sessionCommitTimer.get();
    }

    private static class Timer {
        private long start = -1L;
        private long total;

        public void start() {
            start = System.nanoTime();
        }

        public void stop() {
            if (start == -1) {
                return;
            }

            total += (System.nanoTime() - start);
            start = -1L;
        }

        public long get() {
            return total;
        }
    }
}
