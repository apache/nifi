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

import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.filter.RecordFilter;
import org.apache.parquet.filter.UnboundRecordFilter;

public class OffsetRecordFilter implements RecordFilter, UnboundRecordFilter {

    private long skipsRemaining;

    public OffsetRecordFilter(long startIndex) {
        this.skipsRemaining = startIndex;
    }

    @Override
    public boolean isMatch() {
        if (skipsRemaining == 0) {
            return true;
        }
        skipsRemaining--;
        return false;
    }

    @Override
    public RecordFilter bind(Iterable<ColumnReader> readers) {
        return this;
    }
}
