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

package org.apache.nifi.provenance.index.lucene;

import java.util.List;
import java.util.Optional;

import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.search.Query;
import org.apache.nifi.provenance.serialization.StorageSummary;
import org.apache.nifi.util.RingBuffer;

public class LatestEventsQuery implements CachedQuery {

    final RingBuffer<Long> latestRecords = new RingBuffer<>(1000);

    @Override
    public void update(final ProvenanceEventRecord event, final StorageSummary storageSummary) {
        latestRecords.add(storageSummary.getEventId());
    }

    @Override
    public Optional<List<Long>> evaluate(final Query query) {
        if (latestRecords.getSize() < query.getMaxResults()) {
            return Optional.empty();
        }

        if (query.getSearchTerms().isEmpty() && query.getStartDate() == null && query.getEndDate() == null) {
            final List<Long> eventList = latestRecords.asList();
            if (eventList.size() > query.getMaxResults()) {
                return Optional.of(eventList.subList(0, query.getMaxResults()));
            } else {
                return Optional.of(eventList);
            }
        } else {
            return Optional.empty();
        }
    }

    @Override
    public String toString() {
        return "Most Recent Events Query";
    }
}
