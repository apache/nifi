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
package org.apache.nifi.processor.util.bin;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;

/**
 * Note: {@code Bin} objects are NOT thread safe. If multiple threads access a {@code Bin}, the caller must synchronize
 * access.
 */
public class Bin {
    private final ProcessSession session;
    private final long creationMomentEpochNs;
    private final long minimumSizeBytes;
    private final long maximumSizeBytes;

    private volatile int minimumEntries = 0;
    private volatile int maximumEntries = Integer.MAX_VALUE;
    private final String fileCountAttribute;

    final List<FlowFile> binContents = new ArrayList<>();
    long size;
    int successiveFailedOfferings = 0;

    /**
     * Constructs a new bin
     *
     * @param session the session
     * @param minSizeBytes min bytes
     * @param maxSizeBytes max bytes
     * @param minEntries min entries
     * @param maxEntries max entries
     * @param fileCountAttribute num files
     * @throws IllegalArgumentException if the min is not less than or equal to the max.
     */
    public Bin(final ProcessSession session, final long minSizeBytes, final long maxSizeBytes, final int minEntries, final int maxEntries, final String fileCountAttribute) {
        this.session = session;
        this.minimumSizeBytes = minSizeBytes;
        this.maximumSizeBytes = maxSizeBytes;
        this.minimumEntries = minEntries;
        this.maximumEntries = maxEntries;
        this.fileCountAttribute = fileCountAttribute;

        this.creationMomentEpochNs = System.nanoTime();
        if (minSizeBytes > maxSizeBytes) {
            throw new IllegalArgumentException();
        }
    }

    public ProcessSession getSession() {
        return session;
    }

    /**
     * Indicates whether the bin has enough items to be considered full. This is based on whether the current size of the bin is greater than the minimum size in bytes and based on having a number of
     * successive unsuccessful attempts to add a new item (because it is so close to the max or the size of the objects being attempted do not favor tight packing)
     *
     * @return true if considered full; false otherwise
     */
    public boolean isFull() {
        return (((size >= minimumSizeBytes) && binContents.size() >= minimumEntries) && (successiveFailedOfferings > 5))
                || (size >= maximumSizeBytes) || (binContents.size() >= maximumEntries);
    }

    /**
     * Indicates enough size exists to meet the minimum requirements
     *
     * @return true if full enough
     */
    public boolean isFullEnough() {
        return isFull() || (size >= minimumSizeBytes && (binContents.size() >= minimumEntries));
    }

    /**
     * Determines if this bin is older than the time specified.
     *
     * @param duration duration
     * @param unit unit
     * @return true if this bin is older than the length of time given; false otherwise
     */
    public boolean isOlderThan(final int duration, final TimeUnit unit) {
        final long ageInNanos = System.nanoTime() - creationMomentEpochNs;
        return ageInNanos > TimeUnit.NANOSECONDS.convert(duration, unit);
    }

    /**
     * Determines if this bin is older than the specified bin
     *
     * @param other other bin
     * @return true if this is older than given bin
     */
    public boolean isOlderThan(final Bin other) {
        return creationMomentEpochNs < other.creationMomentEpochNs;
    }

    /**
     * If this bin has enough room for the size of the given flow file then it is added otherwise it is not
     *
     * @param flowFile flowfile to offer
     * @param session the ProcessSession to which the FlowFile belongs
     * @return true if added; false otherwise
     */
    public boolean offer(final FlowFile flowFile, final ProcessSession session) {
        if (((size + flowFile.getSize()) > maximumSizeBytes) || (binContents.size() >= maximumEntries)) {
            successiveFailedOfferings++;
            return false;
        }

        if (fileCountAttribute != null) {
            final String countValue = flowFile.getAttribute(fileCountAttribute);
            final Integer count = toInteger(countValue);
            if (count != null) {
                int currentMaxEntries = this.maximumEntries;
                this.maximumEntries = Math.min(count, currentMaxEntries);
                this.minimumEntries = currentMaxEntries;
            }
        }

        size += flowFile.getSize();

        session.migrate(getSession(), Collections.singleton(flowFile));
        binContents.add(flowFile);
        successiveFailedOfferings = 0;
        return true;
    }

    private static final Pattern intPattern = Pattern.compile("\\d+");

    public Integer toInteger(final String value) {
        if (value == null) {
            return null;
        }
        if (!intPattern.matcher(value).matches()) {
            return null;
        }

        try {
            return Integer.parseInt(value);
        } catch (final Exception e) {
            return null;
        }
    }

    /**
     * @return the underlying list of flow files within this bin
     */
    public List<FlowFile> getContents() {
        return binContents;
    }

    public long getBinAge() {
        final long ageInNanos = System.nanoTime() - creationMomentEpochNs;
        return TimeUnit.MILLISECONDS.convert(ageInNanos, TimeUnit.NANOSECONDS);
    }
}
