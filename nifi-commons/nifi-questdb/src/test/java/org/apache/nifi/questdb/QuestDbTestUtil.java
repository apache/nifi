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
package org.apache.nifi.questdb;

import java.io.File;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class QuestDbTestUtil {
    static final File TEST_DB_PATH = new File("src/test/resources/testdb");
    static final String EVENT_TABLE_NAME = "event";
    static final String EVENT2_TABLE_NAME = "event2";
    static final String CREATE_EVENT_TABLE =
            "CREATE TABLE event (" +
                    "capturedAt TIMESTAMP," +
                    "subject SYMBOL capacity 256 nocache," +
                    "value LONG" +
                    ") TIMESTAMP(capturedAt) PARTITION BY DAY";
    // Deliberately similar to the Event table but different column order
    static final String CREATE_EVENT2_TABLE =
            "CREATE TABLE event2 (" +
                    "subject SYMBOL capacity 256 nocache," +
                    "capturedAt TIMESTAMP," +
                    "value LONG" +
                    ") TIMESTAMP(capturedAt) PARTITION BY DAY";
    static final String SELECT_QUERY = "SELECT * FROM event";
    static final String SELECT_QUERY_2 = "SELECT * FROM event2";

    static final InsertMapping<Event> EVENT_TABLE_INSERT_MAPPING = InsertMappingBuilder.of(Event.class)
            .addInstantField(Event::getCapturedAt)
            .addStringField(Event::getSubject)
            .addLongField(Event::getValue)
            .timestampAt(0)
            .build();
    static final InsertMapping<Event> EVENT_TABLE_INSERT_MAPPING_DIFFERENT_ORDER = InsertMappingBuilder.of(Event.class)
            .addStringField(Event::getSubject)
            .addInstantField(Event::getCapturedAt)
            .addLongField(Event::getValue)
            .timestampAt(1)
            .build();

    static final RequestMapping<Event> EVENT_TABLE_REQUEST_MAPPING = RequestMappingBuilder.of(Event::new)
            .addInstantField(Event::setCapturedAt)
            .addStringField(Event::setSubject)
            .addLongField(Event::setValue)
            .build();

    static final RequestMapping<Event> EVENT_TABLE_REQUEST_MAPPING_DIFFERENT_ORDER = RequestMappingBuilder.of(Event::new)
            .addStringField(Event::setSubject)
            .addInstantField(Event::setCapturedAt)
            .addLongField(Event::setValue)
            .build();

    static List<Event> getTestData() {
        // Using Instant.new() will result some difference on nanosecond level
        final Event event1 = new Event(Instant.ofEpochMilli(System.currentTimeMillis()), "subject1", 1);
        final Event event2 = new Event(Instant.ofEpochMilli(System.currentTimeMillis()), "subject2", 2);
        final Event event3 = new Event(Instant.ofEpochMilli(System.currentTimeMillis()), "subject1", 3);
        return Arrays.asList(event1, event2, event3);
    }

    static List<Event> getRandomTestData() {
        final List<Event> result = new ArrayList<>(100);

        for (int i = 0; i <= 100; i++) {
            result.add(new Event(Instant.ofEpochMilli(System.currentTimeMillis()), UUID.randomUUID().toString(), i));
        }

        return result;
    }
}
