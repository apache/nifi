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
package org.apache.nifi.controller.status.history;

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContext;
import org.apache.nifi.controller.status.history.questdb.QuestDbContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * QuestDB does not provide the possibility for deleting individual lines. Instead there is the option to drop older
 * partitions. In order to clean up older status information, the partitions are outside of the scope of data we intend
 * to keep will be deleted.
 */
public class EmbeddedQuestDbRolloverHandler implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddedQuestDbRolloverHandler.class);

    // Drop keyword is intentionally not uppercase as the query parser only recognizes it in this way
    private static final String DELETION_QUERY = "ALTER TABLE %s drop PARTITION '%s'";
    // Distinct keyword is not recognized if the date mapping is not within an inner query
    static final String SELECTION_QUERY = "SELECT DISTINCT * FROM (SELECT (to_str(capturedAt, 'yyyy-MM-dd')) AS partitionName FROM %s)";

    static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneId.systemDefault());

    private final QuestDbContext dbContext;
    private final List<String> tables = new ArrayList<>();
    private final int daysToKeepData;

    public EmbeddedQuestDbRolloverHandler(final Collection<String> tables, final int daysToKeepData, final QuestDbContext dbContext) {
        this.tables.addAll(tables);
        this.dbContext = dbContext;
        this.daysToKeepData = daysToKeepData;
    }

    @Override
    public void run() {
        LOGGER.debug("Starting rollover");
        tables.forEach(tableName -> rolloverTable(tableName));
        LOGGER.debug("Finishing rollover");
    }

    private void rolloverTable(final CharSequence tableName) {
        try {
            final Set<String> partitions = getPartitions(tableName);
            final Set<String> partitionToKeep = getPartitionsToKeep();

            for (final String partition : partitions) {
                if (!partitionToKeep.contains(partition)) {
                    deletePartition(tableName, partition);
                }
            }
        } catch (final Exception e) {
            LOGGER.error("Could not rollover table " + tableName, e);
        }
    }

    private void deletePartition(final CharSequence tableName, final String partition) {
        try (final SqlCompiler compiler = dbContext.getCompiler()) {
            compiler.compile(String.format(DELETION_QUERY, new Object[]{tableName, partition}), dbContext.getSqlExecutionContext());
        } catch (final Exception e) {
            LOGGER.error("Dropping partition " + partition + " of table " + tableName + " failed", e);
        }
    }

    private Set<String> getPartitions(final CharSequence tableName) throws Exception {
        final SqlExecutionContext executionContext = dbContext.getSqlExecutionContext();
        final Set<String> result = new HashSet<>();

        try (
            final SqlCompiler compiler = dbContext.getCompiler();
            final RecordCursorFactory recordCursorFactory = compiler.compile(String.format(SELECTION_QUERY, new Object[]{tableName}), executionContext).getRecordCursorFactory();
            final RecordCursor cursor = recordCursorFactory.getCursor(executionContext);
        ) {
            while (cursor.hasNext()) {
                final Record record = cursor.getRecord();
                result.add(new StringBuilder(record.getStr(0)).toString());
            }
        }

        return result;
    }

    private Set<String> getPartitionsToKeep() {
        final Instant now = Instant.now();

        // Note: as only full partitions might be deleted and the status history repository works with day based partitions,
        // a partition must remain until any part of it might be the subject of request.
        final Set<String> result = new HashSet<>();
        for (int i = 0; i < daysToKeepData + 1; i++) {
            result.add(DATE_FORMATTER.format(now.minus(i, ChronoUnit.DAYS)));
        }

        return result;
    }
}
