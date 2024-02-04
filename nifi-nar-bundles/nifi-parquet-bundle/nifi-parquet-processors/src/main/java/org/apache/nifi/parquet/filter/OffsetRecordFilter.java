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

package org.apache.nifi.parquet.filter;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.parquet.filter.RecordFilter;
import org.apache.parquet.filter.UnboundRecordFilter;

/**
 * Filter to be used for 'jumping' to a specific record index.
 */
public class OffsetRecordFilter implements RecordFilter {

    private final AtomicLong skipsRemaining;

    public static UnboundRecordFilter offset(long startIndex) {
        final AtomicLong skipsRemaining = new AtomicLong(startIndex);
        return readers -> new OffsetRecordFilter(skipsRemaining);
    }

    private OffsetRecordFilter(AtomicLong skipsRemaining) {
        this.skipsRemaining = skipsRemaining;
    }

    @Override
    public boolean isMatch() {
        // Get current value, and decrement it until zero, in a single atomic operation.
        return skipsRemaining.getAndUpdate(l -> l > 0 ? l - 1 : l) == 0;
    }
}
