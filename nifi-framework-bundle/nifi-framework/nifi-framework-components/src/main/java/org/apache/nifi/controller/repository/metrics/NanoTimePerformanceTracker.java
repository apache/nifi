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
        contentWriteTimer.start();
    }

    @Override
    public void endContentWrite() {
        contentWriteTimer.stop();
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
