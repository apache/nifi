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
package org.apache.nifi.io.nio;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author none
 */
public class BufferPool implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(BufferPool.class);
    final BlockingQueue<ByteBuffer> bufferPool;
    private final static double ONE_MB = 1 << 20;
    private Calendar lastRateSampleTime = Calendar.getInstance();
    private final Calendar startTime = Calendar.getInstance();
    double lastRateSampleMBps = -1.0;
    double overallMBps = -1.0;
    private long totalBytesExtracted = 0L;
    private long lastTotalBytesExtracted = 0L;
    final double maxRateMBps;

    public BufferPool(final int bufferCount, final int bufferCapacity, final boolean allocateDirect, final double maxRateMBps) {
        bufferPool = new LinkedBlockingDeque<>(BufferPool.createBuffers(bufferCount, bufferCapacity, allocateDirect));
        this.maxRateMBps = maxRateMBps;
    }

    /**
     * Returns the given buffer to the pool - and clears it.
     *
     * @param buffer
     * @param bytesProcessed
     * @return
     */
    public synchronized boolean returnBuffer(ByteBuffer buffer, final int bytesProcessed) {
        totalBytesExtracted += bytesProcessed;
        buffer.clear();
        return bufferPool.add(buffer);
    }

    //here we enforce the desired rate we want by restricting access to buffers when we're over rate
    public synchronized ByteBuffer poll() {
        computeRate();
        final double weightedAvg = (lastRateSampleMBps * 0.7) + (overallMBps * 0.3);
        if (overallMBps >= maxRateMBps || weightedAvg >= maxRateMBps) {
            return null;
        }
        return bufferPool.poll();
    }

    public int size() {
        return bufferPool.size();
    }

    private synchronized void computeRate() {
        final Calendar now = Calendar.getInstance();
        final long measurementDurationMillis = now.getTimeInMillis() - lastRateSampleTime.getTimeInMillis();
        final double duractionSecs = ((double) measurementDurationMillis) / 1000.0;
        if (duractionSecs >= 0.75) { //recompute every 3/4 second or when we're too fast
            final long totalDuractionMillis = now.getTimeInMillis() - startTime.getTimeInMillis();
            final double totalDurationSecs = ((double) totalDuractionMillis) / 1000.0;
            final long differenceBytes = totalBytesExtracted - lastTotalBytesExtracted;
            lastTotalBytesExtracted = totalBytesExtracted;
            lastRateSampleTime = now;
            final double bps = ((double) differenceBytes) / duractionSecs;
            final double totalBps = ((double) totalBytesExtracted / totalDurationSecs);
            lastRateSampleMBps = bps / ONE_MB;
            overallMBps = totalBps / ONE_MB;
        }
    }

    public static List<ByteBuffer> createBuffers(final int bufferCount, final int bufferCapacity, final boolean allocateDirect) {
        final List<ByteBuffer> buffers = new ArrayList<>();
        for (int i = 0; i < bufferCount; i++) {
            final ByteBuffer buffer = (allocateDirect) ? ByteBuffer.allocateDirect(bufferCapacity) : ByteBuffer.allocate(bufferCapacity);
            buffers.add(buffer);
        }
        return buffers;
    }

    private void logChannelReadRates() {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(String.format("Overall rate= %,.4f MB/s / Current Rate= %,.4f MB/s / Total Bytes Read= %d", overallMBps, lastRateSampleMBps, totalBytesExtracted));
        }
    }

    @Override
    public void run() {
        computeRate();
        logChannelReadRates();
    }
}
