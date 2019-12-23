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
package org.apache.nifi.provenance.store.iterator;

import org.apache.nifi.provenance.ProvenanceEventRecord;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

public class AggregateEventIterator implements EventIterator {
    private final List<EventIterator> iteratorList;
    private final Iterator<EventIterator> iterators;
    private EventIterator currentIterator;

    public AggregateEventIterator(final List<EventIterator> eventIterators) {
        iteratorList = eventIterators;
        this.iterators = eventIterators.iterator();

        if (iterators.hasNext()) {
            currentIterator = iterators.next();
        }
    }


    @Override
    public Optional<ProvenanceEventRecord> nextEvent() throws IOException {
        while (true) {
            final Optional<ProvenanceEventRecord> optionalEvent = currentIterator.nextEvent();
            if (optionalEvent.isPresent()) {
                return optionalEvent;
            }

            if (iterators.hasNext()) {
                currentIterator = iterators.next();
            } else {
                return Optional.empty();
            }
        }
    }

    @Override
    public void close() throws IOException {
        for (final EventIterator iterator : iteratorList) {
            iterator.close();
        }
    }
}
