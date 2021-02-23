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

import io.questdb.MessageBusImpl;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContext;
import org.apache.nifi.controller.status.history.questdb.QuestDbContext;
import org.apache.nifi.util.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class EmbeddedQuestDbRolloverHandlerTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddedQuestDbRolloverHandlerTest.class);

    private static final String PATH_BASE = "target/questdb";
    private String CREATE_TABLE = "CREATE TABLE measurements (capturedAt TIMESTAMP, value INT) TIMESTAMP(capturedAt) PARTITION BY DAY";

    final Instant now = Instant.now();

    private String path;
    private QuestDbContext dbContext;
    private EmbeddedQuestDbRolloverHandler testSubject;

    @Before
    public void setUp() throws Exception {
        path = PATH_BASE + System.currentTimeMillis();
        FileUtils.ensureDirectoryExistAndCanReadAndWrite(new File(path));

        dbContext = givenDbContext();
        testSubject = new EmbeddedQuestDbRolloverHandler(Collections.singletonList("measurements"), 2, dbContext);
    }

    @After
    public void tearDown() throws Exception {
        try {
            FileUtils.deleteFile(new File(path), true);
        } catch (final Exception e) {
            LOGGER.error("Could not delete database directory", e);
        }
    }

    @Test
    public void testRollOverWhenWithEmptyDatabase() throws Exception {
        // given
        givenTableIsCreated(dbContext);

        // when
        whenRollOverIsExecuted();

        // then
        thenRemainingPartitionsAre(Arrays.asList());
    }

    @Test
    public void testRollOverWhenLessPartitionThanNeeded() throws Exception {
        // given
        givenTableIsCreated(dbContext);
        givenTableIsPopulated(givenMeasurementTimes(Arrays.asList(0, 1)));

        // when
        whenRollOverIsExecuted();

        // then
        thenRemainingPartitionsAre(Arrays.asList(0, 1));
    }

    @Test
    public void testRollOverWhenNoPartitionToDrop() throws Exception {
        // given
        givenTableIsCreated(dbContext);
        givenTableIsPopulated(givenMeasurementTimes(Arrays.asList(0, 1, 2)));

        // when
        whenRollOverIsExecuted();

        // then
        thenRemainingPartitionsAre(Arrays.asList(0, 1, 2));
    }

    @Test
    public void testRollOverWhenOldPartitionsPresent() throws Exception {
        // given
        givenTableIsCreated(dbContext);
        givenTableIsPopulated(givenMeasurementTimes(Arrays.asList(0, 1, 2, 3, 4)));

        // when
        whenRollOverIsExecuted();

        // then
        thenRemainingPartitionsAre(Arrays.asList(0, 1, 2));
    }

    @Test
    // This scenario might occurs when the NiFi was stopped and the persistens storage remaing
    public void testRollOverWhenNonconsecutivePartitionsPresent() throws Exception {
        // given
        givenTableIsCreated(dbContext);
        givenTableIsPopulated(givenMeasurementTimes(Arrays.asList(0, 1, 7, 8, 9)));

        // when
        whenRollOverIsExecuted();

        // then
        thenRemainingPartitionsAre(Arrays.asList(0, 1));
    }

    private QuestDbContext givenDbContext() {
        final CairoConfiguration configuration = new DefaultCairoConfiguration(path);
        final CairoEngine engine = new CairoEngine(configuration);
        return new QuestDbContext(engine, new MessageBusImpl());
    }

    private void givenTableIsCreated(final QuestDbContext dbContext) throws Exception {
        dbContext.getCompiler().compile(CREATE_TABLE, dbContext.getSqlExecutionContext());
    }

    private void givenTableIsPopulated(final List<Long> givenMeasurementTimes) {
        final SqlExecutionContext executionContext = dbContext.getSqlExecutionContext();
        final TableWriter tableWriter = dbContext.getEngine().getWriter(executionContext.getCairoSecurityContext(), "measurements");

        for (int i = 0; i < givenMeasurementTimes.size(); i++) {
            final TableWriter.Row row = tableWriter.newRow(TimeUnit.MILLISECONDS.toMicros(givenMeasurementTimes.get(i)));
            row.putTimestamp(0, TimeUnit.MILLISECONDS.toMicros(givenMeasurementTimes.get(i)));
            row.putInt(1, i);
            row.append();
        }

        tableWriter.commit();
        tableWriter.close();
    }

    private List<Long> givenMeasurementTimes(final List<Integer> daysBack) {
        final List<Long> result = new LinkedList<>();

        for (final Integer day : daysBack) {
            result.add(now.minus(day, ChronoUnit.DAYS).toEpochMilli());
        }

        result.sort((l1, l2) -> l1.compareTo(l2));
        return result;
    }

    private void whenRollOverIsExecuted() {
        testSubject.run();
    }

    private void thenRemainingPartitionsAre(final List<Integer> expectedDays) throws Exception {
        final List<String> expectedPartitions = new ArrayList<>(expectedDays.size());

        for (final Integer expectedDay : expectedDays) {
            expectedPartitions.add(EmbeddedQuestDbRolloverHandler.DATE_FORMATTER.format(now.minus(expectedDay, ChronoUnit.DAYS)));
        }

        final SqlExecutionContext executionContext = dbContext.getSqlExecutionContext();
        final RecordCursorFactory cursorFactory = dbContext.getCompiler()
                .compile(String.format(EmbeddedQuestDbRolloverHandler.SELECTION_QUERY, "measurements"), executionContext).getRecordCursorFactory();
        final RecordCursor cursor = cursorFactory.getCursor(executionContext);

        final List<String> existingPartitions = new LinkedList<>();

        while (cursor.hasNext()) {
            final Record record = cursor.getRecord();
            existingPartitions.add(new StringBuilder(record.getStr(0)).toString());
        }

        Assert.assertEquals(expectedPartitions.size(), existingPartitions.size());

        for (final String expectedPartition : expectedPartitions) {
            Assert.assertTrue("Partition " + expectedPartition + " is expected", existingPartitions.contains(expectedPartition));
        }
    }
}