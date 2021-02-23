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
package org.apache.nifi.controller.status.history.storage;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.commons.math3.util.Pair;
import org.apache.nifi.controller.status.history.GarbageCollectionHistory;
import org.apache.nifi.controller.status.history.GarbageCollectionStatus;
import org.apache.nifi.controller.status.history.StandardGarbageCollectionHistory;
import org.apache.nifi.controller.status.history.StandardGarbageCollectionStatus;
import org.apache.nifi.controller.status.history.questdb.QuestDbContext;
import org.apache.nifi.controller.status.history.questdb.QuestDbEntityReadingTemplate;
import org.apache.nifi.controller.status.history.questdb.QuestDbEntityWritingTemplate;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Optional;

public class QuestDbGarbageCollectionStatusStorage implements GarbageCollectionStatusStorage {
    private static final FastDateFormat DATE_FORMAT = FastDateFormat.getInstance(StatusStorage.CAPTURE_DATE_FORMAT);
    private static final String TABLE_NAME = "garbageCollectionStatus";

    private static final String QUERY =
        "SELECT * FROM garbageCollectionStatus " +
        "WHERE capturedAt > to_timestamp('%s', '" + StatusStorage.CAPTURE_DATE_FORMAT + "') " +
        "AND capturedAt < to_timestamp('%s', '" + StatusStorage.CAPTURE_DATE_FORMAT +  "') " +
        "ORDER BY capturedAt ASC";

    private static final QuestDbEntityWritingTemplate<GarbageCollectionStatus> WRITING_TEMPLATE =  new QuestDbEntityWritingTemplate<>(
        TABLE_NAME,
        (statusEntry, row) -> {
            row.putSym(1, statusEntry.getMemoryManagerName());
            row.putLong(2, statusEntry.getCollectionCount());
            row.putLong(3, statusEntry.getCollectionMillis());
        });


    private static final QuestDbEntityReadingTemplate<GarbageCollectionStatus, GarbageCollectionHistory> READING_TEMPLATE = new QuestDbEntityReadingTemplate<>(
        QUERY,
        record ->
            new StandardGarbageCollectionStatus(new StringBuilder(record.getSym(1)).toString(), new Date(record.getTimestamp(0)), record.getLong(2), record.getLong(3)),
        garbageCollectionStatuses -> {
            final StandardGarbageCollectionHistory result = new StandardGarbageCollectionHistory();
            garbageCollectionStatuses.forEach(status -> result.addGarbageCollectionStatus(status));
            return result;
        },
        e -> new StandardGarbageCollectionHistory()
    );

    private final QuestDbContext context;

    public QuestDbGarbageCollectionStatusStorage(final QuestDbContext context) {
        this.context = context;
    }

    @Override
    public GarbageCollectionHistory read(final Date start, final Date end) {
        final String formattedStart = DATE_FORMAT.format(Optional.ofNullable(start).orElse(DateUtils.addDays(new Date(), -1)));
        final String formattedEnd = DATE_FORMAT.format(Optional.ofNullable(end).orElse(new Date()));
        return READING_TEMPLATE.read(context.getEngine(), context.getSqlExecutionContext(), Arrays.asList(formattedStart, formattedEnd));
    }

    @Override
    public void store(final List<Pair<Date, GarbageCollectionStatus>> statusEntries) {
        WRITING_TEMPLATE.insert(context.getEngine(), context.getSqlExecutionContext(), statusEntries);
    }
}
