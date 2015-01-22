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
package org.apache.nifi.util;

import java.util.concurrent.TimeUnit;

public final class StopWatch {

    private long startNanos = -1L;
    private long duration = -1L;

    /**
     * Creates a StopWatch but does not start it
     */
    public StopWatch() {
        this(false);
    }

    /**
     * @param autoStart whether or not the timer should be started automatically
     */
    public StopWatch(final boolean autoStart) {
        if (autoStart) {
            start();
        }
    }

    public void start() {
        this.startNanos = System.nanoTime();
        this.duration = -1L;
    }

    public void stop() {
        if (startNanos < 0) {
            throw new IllegalStateException("StopWatch has not been started");
        }
        this.duration = System.nanoTime() - startNanos;
        this.startNanos = -1L;
    }

    /**
     * Returns the amount of time that the StopWatch was running.
     *
     * @param timeUnit
     * @return
     *
     * @throws IllegalStateException if the StopWatch has not been stopped via
     * {@link #stop()}
     */
    public long getDuration(final TimeUnit timeUnit) {
        if (duration < 0) {
            throw new IllegalStateException("Cannot get duration until StopWatch has been stopped");
        }
        return timeUnit.convert(duration, TimeUnit.NANOSECONDS);
    }

    /**
     * Returns the amount of time that has elapsed since the timer was started.
     *
     * @param timeUnit
     * @return
     */
    public long getElapsed(final TimeUnit timeUnit) {
        return timeUnit.convert(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
    }

    public String calculateDataRate(final long bytes) {
        final double seconds = (double) duration / 1000000000.0D;
        final long dataSize = (long) (bytes / seconds);
        return FormatUtils.formatDataSize(dataSize) + "/sec";
    }

    public String getDuration() {
        final StringBuilder sb = new StringBuilder();

        long duration = this.duration;
        final long minutes = (duration > 60000000000L) ? (duration / 60000000000L) : 0L;
        duration -= TimeUnit.NANOSECONDS.convert(minutes, TimeUnit.MINUTES);

        final long seconds = (duration > 1000000000L) ? (duration / 1000000000L) : 0L;
        duration -= TimeUnit.NANOSECONDS.convert(seconds, TimeUnit.SECONDS);

        final long millis = (duration > 1000000L) ? (duration / 1000000L) : 0L;
        duration -= TimeUnit.NANOSECONDS.convert(millis, TimeUnit.MILLISECONDS);

        final long nanos = duration % 1000000L;

        if (minutes > 0) {
            sb.append(minutes).append(" minutes");
        }

        if (seconds > 0) {
            if (minutes > 0) {
                sb.append(", ");
            }

            sb.append(seconds).append(" seconds");
        }

        if (millis > 0) {
            if (seconds > 0) {
                sb.append(", ");
            }

            sb.append(millis).append(" millis");
        }
        if (seconds == 0 && millis == 0) {
            sb.append(nanos).append(" nanos");
        }

        return sb.toString();
    }
}
