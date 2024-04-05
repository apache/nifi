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
package org.apache.nifi.questdb.util;

import org.apache.nifi.questdb.InsertRowContext;
import org.apache.nifi.questdb.InsertRowDataSource;
import org.apache.nifi.questdb.mapping.RequestMapping;
import org.apache.nifi.questdb.mapping.RequestMappingBuilder;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

public class QuestDbTestUtil {
    public static final String EVENT_TABLE_NAME = "event";
    public static final String EVENT2_TABLE_NAME = "event2";
    public static final String CREATE_EVENT_TABLE =
            "CREATE TABLE event (" +
                    "captured TIMESTAMP," +
                    "subject SYMBOL capacity 256 nocache," +
                    "value LONG" +
                    ") TIMESTAMP(captured) PARTITION BY DAY";
    // Deliberately similar to the Event table but different column order
    public static final String CREATE_EVENT2_TABLE =
            "CREATE TABLE event2 (" +
                    "subject SYMBOL capacity 256 nocache," +
                    "captured TIMESTAMP," +
                    "value LONG" +
                    ") TIMESTAMP(captured) PARTITION BY DAY";
    public static final String SELECT_QUERY = "SELECT * FROM event";
    public static final String SELECT_QUERY_2 = "SELECT * FROM event2";

    public static InsertRowDataSource getEventTableDataSource(final Collection<Event> events) {
        return new InsertRowDataSource() {
            private final Iterator<Event> eventsToInsert = events.iterator();

            @Override
            public boolean hasNextToInsert() {
                return eventsToInsert.hasNext();
            }

            @Override
            public void fillRowData(final InsertRowContext context) {
                final Event event = eventsToInsert.next();
                context.initializeRow(event.getCaptured())
                    .addString(1, event.getSubject())
                    .addLong(2, event.getValue());
            }
        };
    }

    public static InsertRowDataSource getEventTableDataSourceWithDifferentOrder(final Collection<Event> events) {
        return new InsertRowDataSource() {
            private final Iterator<Event> eventsToInsert = events.iterator();

            @Override
            public boolean hasNextToInsert() {
                return eventsToInsert.hasNext();
            }

            @Override
            public void fillRowData(final InsertRowContext context) {
                final Event event = eventsToInsert.next();
                context.initializeRow(event.getCaptured())
                        .addString(0, event.getSubject())
                        .addLong(2, event.getValue());
            }
        };
    }

    public static final RequestMapping<Event> EVENT_TABLE_REQUEST_MAPPING = RequestMappingBuilder.of(Event::new)
            .addInstantField(Event::setCaptured)
            .addStringField(Event::setSubject)
            .addLongField(Event::setValue)
            .build();

    public static final RequestMapping<Event> EVENT_TABLE_REQUEST_MAPPING_DIFFERENT_ORDER = RequestMappingBuilder.of(Event::new)
            .addStringField(Event::setSubject)
            .addInstantField(Event::setCaptured)
            .addLongField(Event::setValue)
            .build();

    public static List<Event> getTestData() {
        // Using Instant.new() will result some difference on nanosecond level
        final Event event1 = new Event(Instant.ofEpochMilli(System.currentTimeMillis()), "subject1", 1);
        final Event event2 = new Event(Instant.ofEpochMilli(System.currentTimeMillis()), "subject2", 2);
        final Event event3 = new Event(Instant.ofEpochMilli(System.currentTimeMillis()), "subject1", 3);
        return Arrays.asList(event1, event2, event3);
    }

    public static List<Event> getRandomTestData() {
        final List<Event> result = new ArrayList<>(100);

        for (int i = 0; i <= 100; i++) {
            result.add(new Event(Instant.ofEpochMilli(System.currentTimeMillis()), UUID.randomUUID().toString(), i));
        }

        return result;
    }
}
