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
package org.apache.nifi.service.cql.it;

import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.service.cql.api.service.CQLQueryCallback;

import java.util.ArrayList;
import java.util.List;

/**
 * The {@link CQLQueryCallback} the integration tests read result sets through: it accumulates every row the
 * service hands back, so a test can assert on what a query actually returned.
 *
 * <p>{@link #clear()} discards the accumulated rows, mirroring what a real callback does when the service
 * signals that a fatal error means the batch it has built up so far must be thrown away. The number of times
 * it was called is recorded rather than swallowed, so a test can assert that a failing query cleaned up after
 * itself - a no-op {@code clear()} would look identical to one that was never called at all.
 */
public class CollectingCqlQueryCallback implements CQLQueryCallback {
    private final List<Record> records = new ArrayList<>();
    private int clearCount;

    @Override
    public void receive(final long rowNumber, final Record result, final boolean hasMore) {
        records.add(result);
    }

    @Override
    public void clear() {
        records.clear();
        clearCount++;
    }

    /**
     * Always {@code false}: these tests call the service directly, so there is no incoming FlowFile for this
     * callback to have routed to {@code original}.
     */
    @Override
    public boolean hasSentOriginal() {
        return false;
    }

    /**
     * @return the rows received so far, in the order the service delivered them
     */
    public List<Record> getRecords() {
        return List.copyOf(records);
    }

    /**
     * @return how many times {@link #clear()} has been called
     */
    public int getClearCount() {
        return clearCount;
    }
}
