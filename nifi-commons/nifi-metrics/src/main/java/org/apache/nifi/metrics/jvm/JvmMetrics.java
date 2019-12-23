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
package org.apache.nifi.metrics.jvm;

import org.apache.nifi.processor.DataUnit;

import java.io.OutputStream;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Provides methods for retrieving metrics from the JVM.
 */
public interface JvmMetrics {

    /**
     * Returns the total initial memory of the current JVM.
     *
     * @param dataUnit The {@link DataUnit} to which the metric will be converted
     *
     * @return total Heap and non-heap initial JVM memory in the given {@link DataUnit}
     */
    double totalInit(DataUnit dataUnit);

    /**
     * Returns the total memory currently used by the current JVM.
     *
     * @param dataUnit The {@link DataUnit} to which the metric will be converted
     *
     * @return total Heap and non-heap memory currently used by JVM in the given {@link DataUnit}
     */
    double totalUsed(DataUnit dataUnit);

    /**
     * Returns the total memory currently used by the current JVM.
     *
     * @param dataUnit The {@link DataUnit} to which the metric will be converted
     *
     * @return total Heap and non-heap memory currently used by JVM in the given {@link DataUnit}
     */
    double totalMax(DataUnit dataUnit);

    /**
     * Returns the total memory committed to the JVM.
     *
     * @param dataUnit The {@link DataUnit} to which the metric will be converted
     *
     * @return total Heap and non-heap memory currently committed to the JVM in the given {@link DataUnit}
     */
    double totalCommitted(DataUnit dataUnit);

    /**
     * Returns the heap initial memory of the current JVM.
     *
     * @param dataUnit The {@link DataUnit} to which the metric will be converted
     *
     * @return Heap initial JVM memory in the given {@link DataUnit}
     */
    double heapInit(DataUnit dataUnit);

    /**
     * Returns the heap memory currently used by the current JVM.
     *
     * @param dataUnit The {@link DataUnit} to which the metric will be converted
     *
     * @return Heap memory currently used by JVM in the given {@link DataUnit}
     */
    double heapUsed(DataUnit dataUnit);

    /**
     * Returns the heap memory currently used by the current JVM.
     *
     * @param dataUnit The {@link DataUnit} to which the metric will be converted
     *
     * @return Heap memory currently used by JVM in the given {@link DataUnit}
     */
    double heapMax(DataUnit dataUnit);

    /**
     * Returns the heap memory committed to the JVM.
     *
     * @param dataUnit The {@link DataUnit} to which the metric will be converted
     *
     * @return Heap memory currently committed to the JVM in the given {@link DataUnit}
     */
    double heapCommitted(DataUnit dataUnit);

    /**
     * Returns the percentage of the JVM's heap which is being used.
     *
     * @return the percentage of the JVM's heap which is being used
     */
    double heapUsage();

    /**
     * Returns the percentage of the JVM's non-heap memory (e.g., direct buffers) which is being
     * used.
     *
     * @return the percentage of the JVM's non-heap memory which is being used
     */
    double nonHeapUsage();

    /**
     * Returns a map of memory pool names to the percentage of that pool which is being used.
     *
     * @return a map of memory pool names to the percentage of that pool which is being used
     */
    Map<String, Double> memoryPoolUsage();

    /**
     * Returns the percentage of available file descriptors which are currently in use.
     *
     * @return the percentage of available file descriptors which are currently in use, or {@code
     *         NaN} if the running JVM does not have access to this information
     */
    double fileDescriptorUsage();

    /**
     * Returns the version of the currently-running jvm.
     *
     * @return the version of the currently-running jvm, eg "1.6.0_24"
     * @see <a href="http://java.sun.com/j2se/versioning_naming.html">J2SE SDK/JRE Version String
     *      Naming Convention</a>
     */
    String version();

    /**
     * Returns the name of the currently-running jvm.
     *
     * @return the name of the currently-running jvm, eg  "Java HotSpot(TM) Client VM"
     * @see <a href="http://download.oracle.com/javase/6/docs/api/java/lang/System.html#getProperties()">System.getProperties()</a>
     */
    String name();

    /**
     * Returns the number of seconds the JVM process has been running.
     *
     * @return the number of seconds the JVM process has been running
     */
    long uptime();

    /**
     * Returns the number of live threads (includes {@link #daemonThreadCount()}.
     *
     * @return the number of live threads
     */
    int threadCount();

    /**
     * Returns the number of live daemon threads.
     *
     * @return the number of live daemon threads
     */
    int daemonThreadCount();

    /**
     * Returns a map of garbage collector names to garbage collector information.
     *
     * @return a map of garbage collector names to garbage collector information
     */
    Map<String, GarbageCollectorStats> garbageCollectors();

    /**
     * Returns a set of strings describing deadlocked threads, if any are deadlocked.
     *
     * @return a set of any deadlocked threads
     */
    Set<String> deadlockedThreads();

    /**
     * Returns a map of thread states to the percentage of all threads which are in that state.
     *
     * @return a map of thread states to percentages
     */
    Map<Thread.State, Double> threadStatePercentages();

    /**
     * Dumps all of the threads' current information to an output stream.
     *
     * @param out an output stream
     */
    void threadDump(OutputStream out);

    Map<String, BufferPoolStats> getBufferPoolStats();

    /**
     * Per-GC statistics.
     */
    class GarbageCollectorStats {
        private final long runs, timeMS;

        GarbageCollectorStats(long runs, long timeMS) {
            this.runs = runs;
            this.timeMS = timeMS;
        }

        /**
         * Returns the number of times the garbage collector has run.
         *
         * @return the number of times the garbage collector has run
         */
        public long getRuns() {
            return runs;
        }

        /**
         * Returns the amount of time in the given unit the garbage collector has taken in total.
         *
         * @param unit    the time unit for the return value
         * @return the amount of time in the given unit the garbage collector
         */
        public long getTime(TimeUnit unit) {
            return unit.convert(timeMS, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * The management interface for a buffer pool, for example a pool of {@link
     * java.nio.ByteBuffer#allocateDirect direct} or {@link java.nio.MappedByteBuffer mapped}
     * buffers.
     */
    class BufferPoolStats {
        private final long count, memoryUsed, totalCapacity;

        BufferPoolStats(long count, long memoryUsed, long totalCapacity) {
            this.count = count;
            this.memoryUsed = memoryUsed;
            this.totalCapacity = totalCapacity;
        }

        /**
         * Returns an estimate of the number of buffers in the pool.
         *
         * @return An estimate of the number of buffers in this pool
         */
        public long getCount() {
            return count;
        }

        /**
         * Returns an estimate of the memory that the Java virtual machine is using for this buffer
         * pool. The value returned by this method may differ from the estimate of the total {@link
         * #getTotalCapacity capacity} of the buffers in this pool. This difference is explained by
         * alignment, memory allocator, and other implementation specific reasons.
         *
         * @return An estimate of the memory that the Java virtual machine is using for this buffer
         *         pool in bytes, or {@code -1L} if an estimate of the memory usage is not
         *         available
         */
        public long getMemoryUsed(DataUnit dataUnit) {
            return (long)dataUnit.convert(memoryUsed, DataUnit.B);
        }

        /**
         * Returns an estimate of the total capacity of the buffers in this pool. A buffer's
         * capacity is the number of elements it contains and the value returned by this method is
         * an estimate of the total capacity of buffers in the pool in bytes.
         *
         * @return An estimate of the total capacity of the buffers in this pool in bytes
         */
        public long getTotalCapacity(DataUnit dataUnit) {
            return totalCapacity;
        }
    }
}