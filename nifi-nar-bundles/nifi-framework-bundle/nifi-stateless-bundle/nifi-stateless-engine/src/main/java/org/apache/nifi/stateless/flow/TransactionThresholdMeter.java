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

package org.apache.nifi.stateless.flow;

import org.apache.nifi.processor.DataUnit;

import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;

public class TransactionThresholdMeter {
    private final TransactionThresholds thresholds;
    private long startNanos;
    private long bytes;
    private long flowFiles;

    public TransactionThresholdMeter(final TransactionThresholds thresholds) {
        this.thresholds = thresholds;
    }

    public void reset() {
        startNanos = System.nanoTime();
        bytes = 0L;
        flowFiles = 0;
    }

    public TransactionThresholds getThresholds() {
        return thresholds;
    }

    public void incrementFlowFiles(final long flowFiles) {
        this.flowFiles += flowFiles;
    }

    public void incrementBytes(final long bytes) {
        this.bytes += bytes;
    }

    public boolean isThresholdMet() {
        final OptionalLong maxFlowFiles = thresholds.getMaxFlowFiles();
        if (maxFlowFiles.isPresent() && flowFiles >= maxFlowFiles.getAsLong()) {
            return true;
        }

        final OptionalLong maxBytes = thresholds.getMaxContentSize(DataUnit.B);
        if (maxBytes.isPresent() && bytes >= maxBytes.getAsLong()) {
            return true;
        }

        final OptionalLong maxNanos = thresholds.getMaxTime(TimeUnit.NANOSECONDS);
        if (maxNanos.isPresent()) {
            final long now = System.nanoTime();
            final long nanosSinceStart = now - startNanos;
            return nanosSinceStart >= maxNanos.getAsLong();
        }

        return false;
    }

    public String toString() {
        final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        return "TransactionThresholdMeter[flowFiles=" + flowFiles + ", bytes=" + bytes + ", elapsedTime=" + millis + " millis]";
    }
}
