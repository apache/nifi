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
package org.apache.nifi.controller.status.history.questdb;

import io.questdb.cairo.TableWriter;
import org.apache.commons.lang3.tuple.Pair;

import java.time.Instant;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * Writes entry to the database with the given measurement time.
 *
 * @param <E> Entry type.
 */
public class QuestDbEntityWritingTemplate<E> extends QuestDbWritingTemplate<Pair<Instant, E>> {
    private final BiConsumer<E, TableWriter.Row> fillRow;

    /**
     * @param tableName Name of the target table.
     * @param fillRow Responsible for filling a row based on the entry.
     */
    public QuestDbEntityWritingTemplate(final String tableName, final BiConsumer<E, TableWriter.Row> fillRow) {
        super(tableName);
        this.fillRow = fillRow;
    }

    @Override
    protected void addRows(final TableWriter tableWriter, final Collection<Pair<Instant, E>> entries) {
        entries.forEach(statusEntry -> {
            final long capturedAt = TimeUnit.MILLISECONDS.toMicros(statusEntry.getLeft().toEpochMilli());
            final TableWriter.Row row = tableWriter.newRow(capturedAt);
            fillRow.accept(statusEntry.getRight(), row);
            row.append();
        });
    }
}
