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

package org.apache.nifi.processors.standard.merge;

import java.util.Optional;

public class RecordBinThresholds {
    private final int minRecords;
    private final int maxRecords;
    private final long minBytes;
    private final long maxBytes;
    private final long maxBinMillis;
    private final String maxBinAge;
    private final Optional<String> recordCountAttribute;

    public RecordBinThresholds(final int minRecords, final int maxRecords, final long minBytes, final long maxBytes, final long maxBinMillis,
        final String maxBinAge, final String recordCountAttribute) {
        this.minRecords = minRecords;
        this.maxRecords = maxRecords;
        this.minBytes = minBytes;
        this.maxBytes = maxBytes;
        this.maxBinMillis = maxBinMillis;
        this.maxBinAge = maxBinAge;
        this.recordCountAttribute = Optional.ofNullable(recordCountAttribute);
    }

    public int getMinRecords() {
        return minRecords;
    }

    public int getMaxRecords() {
        return maxRecords;
    }

    public long getMinBytes() {
        return minBytes;
    }

    public long getMaxBytes() {
        return maxBytes;
    }

    public long getMaxBinMillis() {
        return maxBinMillis;
    }

    public String getMaxBinAge() {
        return maxBinAge;
    }

    public Optional<String> getRecordCountAttribute() {
        return recordCountAttribute;
    }
}
