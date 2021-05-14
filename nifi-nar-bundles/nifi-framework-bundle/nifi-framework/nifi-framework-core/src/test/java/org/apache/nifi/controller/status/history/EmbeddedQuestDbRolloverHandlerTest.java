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
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

@Ignore("Buggy tests depend on time of day")
public class EmbeddedQuestDbRolloverHandlerTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddedQuestDbRolloverHandlerTest.class);

    private static final String PATH_BASE = "target/questdb";
    private static final String CREATE_TABLE = "CREATE TABLE measurements (capturedAt TIMESTAMP, value INT) TIMESTAMP(capturedAt) PARTITION BY DAY";

    private static final String UTC_MAR_1_1200 = "03/01/2021 12:00:00 UTC";
    private static final String UTC_MAR_2_1200 = "03/02/2021 12:00:00 UTC";
    private static final String UTC_MAR_5_1200 = "03/05/2021 12:00:00 UTC";
    private static final String UTC_MAR_6_1200 = "03/06/2021 12:00:00 UTC";
    private static final String UTC_MAR_7_1200 = "03/07/2021 12:00:00 UTC";
    private static final String UTC_MAR_8_1200 = "03/08/2021 12:00:00 UTC";
    private static final String UTC_MAR_8_1700 = "03/08/2021 17:00:00 UTC";

    private static final String EST_MAR_5_1200 = "03/05/2021 12:00:00 EST"; // UTC: 03/05/2021 17:00:00
    private static final String EST_MAR_6_1200 = "03/06/2021 12:00:00 EST"; // UTC: 03/06/2021 17:00:00
    private static final String EST_MAR_7_1200 = "03/07/2021 12:00:00 EST"; // UTC: 03/07/2021 17:00:00
    private static final String EST_MAR_8_1200 = "03/08/2021 12:00:00 EST"; // UTC: 03/08/2021 17:00:00
    private static final String EST_MAR_8_1600 = "03/08/2021 16:00:00 EST"; // UTC: 03/08/2021 21:00:00
    private static final String EST_MAR_8_1700 = "03/08/2021 17:00:00 EST"; // UTC: 03/09/2021 22:00:00
    private static final String EST_MAR_8_2200 = "03/08/2021 22:00:00 EST"; // UTC: 03/09/2021 03:00:00
    private static final String EST_MAR_8_2300 = "03/08/2021 23:00:00 EST"; // UTC: 03/09/2021 04:00:00

    private static final String SGT_MAR_4_1200 = "03/04/2021 12:00:00 SGT"; // UTC: 03/04/2021 04:00:00
    private static final String SGT_MAR_5_1200 = "03/05/2021 12:00:00 SGT"; // UTC: 03/05/2021 04:00:00
    private static final String SGT_MAR_6_1200 = "03/06/2021 12:00:00 SGT"; // UTC: 03/06/2021 04:00:00
    private static final String SGT_MAR_7_1200 = "03/07/2021 12:00:00 SGT"; // UTC: 03/07/2021 04:00:00
    private static final String SGT_MAR_8_1200 = "03/08/2021 12:00:00 SGT"; // UTC: 03/08/2021 04:00:00
    private static final String SGT_MAR_8_1300 = "03/08/2021 13:00:00 SGT"; // UTC: 03/08/2021 05:00:00
    private static final String SGT_MAR_8_2300 = "03/08/2021 23:00:00 SGT"; // UTC: 03/08/2021 15:00:00

    private String path;
    private QuestDbContext dbContext;

    @Before
    public void setUp() throws Exception {
        path = PATH_BASE + System.currentTimeMillis();
        FileUtils.ensureDirectoryExistAndCanReadAndWrite(new File(path));

        dbContext = givenDbContext();
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
    public void testNoOffsetTimeZoneWhenPartitionNeedsToBeRolled() throws Exception {
        // given
        givenTableIsCreated(dbContext);
        givenTableIsPopulated(UTC_MAR_5_1200, UTC_MAR_6_1200, UTC_MAR_7_1200, UTC_MAR_8_1200);

        // when
        whenRollOverIsExecuted(UTC_MAR_8_1700);

        // then
        thenTheRemainingPartitionsAre("2021-03-06", "2021-03-07", "2021-03-08");
    }

    @Test
    // This scenario might occur when the NiFi was stopped and the persistent storage remains in place
    public void testNoOffsetTimeZoneAndNonConsecutive() throws Exception {
        // given
        givenTableIsCreated(dbContext);
        givenTableIsPopulated(UTC_MAR_1_1200, UTC_MAR_2_1200, UTC_MAR_7_1200, UTC_MAR_8_1200);

        // when
        whenRollOverIsExecuted(UTC_MAR_8_1700);

        // then
        thenTheRemainingPartitionsAre("2021-03-07", "2021-03-08");
    }

    @Test
    public void testNoOffsetTimeWhenNoPartitionsNeedToBeDropped() throws Exception {
        // given
        givenTableIsCreated(dbContext);
        givenTableIsPopulated(UTC_MAR_6_1200, UTC_MAR_7_1200, UTC_MAR_8_1200);

        // when
        whenRollOverIsExecuted(UTC_MAR_8_1700);

        // then
        thenTheRemainingPartitionsAre("2021-03-06", "2021-03-07", "2021-03-08");
    }

    @Test
    public void testNoOffsetTimeZoneAndLessPartitionThanNeeded() throws Exception {
        // given
        givenTableIsCreated(dbContext);
        givenTableIsPopulated(UTC_MAR_7_1200, UTC_MAR_8_1200);

        // when
        whenRollOverIsExecuted(UTC_MAR_8_1700);

        // then
        thenTheRemainingPartitionsAre("2021-03-07", "2021-03-08");
    }

    @Test
    public void testNoOffsetTimeZoneAndOldPartitionsOnly() throws Exception {
        // given
        givenTableIsCreated(dbContext);
        givenTableIsPopulated(UTC_MAR_1_1200, UTC_MAR_2_1200);

        // when
        whenRollOverIsExecuted(UTC_MAR_8_1700);

        // then - QuestDB will not remove the active partition if presents
        thenTheRemainingPartitionsAre("2021-03-02");
    }

    @Test
    public void testNoOffsetTimeZoneAndEmptyDatabase() throws Exception {
        // given
        givenTableIsCreated(dbContext);

        // when
        whenRollOverIsExecuted(UTC_MAR_8_1700);

        // then
        thenNoPartitionsExpected();
    }

    @Test
    public void testNegativeOffsetTimeZoneWhenOverlaps() throws Exception {
        // given
        givenTableIsCreated(dbContext);
        givenTableIsPopulated(EST_MAR_5_1200, EST_MAR_6_1200, EST_MAR_7_1200, EST_MAR_8_1200, EST_MAR_8_1600);

        // when
        whenRollOverIsExecuted(EST_MAR_8_1700);

        // then
        thenTheRemainingPartitionsAre("2021-03-06", "2021-03-07", "2021-03-08");
    }

    @Test
    public void testNegativeOffsetTimeZoneWhenOverlapsAndRolledLater() throws Exception {
        // given
        givenTableIsCreated(dbContext);
        givenTableIsPopulated(EST_MAR_5_1200, EST_MAR_6_1200, EST_MAR_7_1200, EST_MAR_8_1200, EST_MAR_8_1600);

        // when
        whenRollOverIsExecuted(EST_MAR_8_2300);

        // then (there is no data inserted into the time range after the partition 2021-03-08, so 2021-03-09 is not created)
        thenTheRemainingPartitionsAre("2021-03-07", "2021-03-08");
    }

    @Test
    public void testNegativeOffsetTimeZoneWhenHangsOver() throws Exception {
        // given
        givenTableIsCreated(dbContext);
        givenTableIsPopulated(EST_MAR_6_1200, EST_MAR_7_1200, EST_MAR_8_1200, EST_MAR_8_2200);

        // when
        whenRollOverIsExecuted(EST_MAR_8_2300);

        // then
        thenTheRemainingPartitionsAre("2021-03-07", "2021-03-08", "2021-03-09");
    }

    @Test
    public void testPositiveOffsetTimeZoneWhenOverlaps() throws Exception {
        // given
        givenTableIsCreated(dbContext);
        givenTableIsPopulated(SGT_MAR_4_1200, SGT_MAR_5_1200, SGT_MAR_6_1200, SGT_MAR_7_1200, SGT_MAR_8_1200);

        // when
        whenRollOverIsExecuted(SGT_MAR_8_1300);

        // then
        thenTheRemainingPartitionsAre("2021-03-06", "2021-03-07", "2021-03-08");
    }

    @Test
    public void testPositiveOffsetTimeZoneWhenHangsOver() throws Exception {
        // given
        givenTableIsCreated(dbContext);
        givenTableIsPopulated(SGT_MAR_4_1200, SGT_MAR_5_1200, SGT_MAR_6_1200, SGT_MAR_7_1200, SGT_MAR_8_1200);

        // when
        whenRollOverIsExecuted(SGT_MAR_8_2300);

        // then
        thenTheRemainingPartitionsAre("2021-03-06", "2021-03-07", "2021-03-08");
    }

    private QuestDbContext givenDbContext() {
        final CairoConfiguration configuration = new DefaultCairoConfiguration(path);
        final CairoEngine engine = new CairoEngine(configuration);
        return new QuestDbContext(engine, new MessageBusImpl());
    }

    private void givenTableIsCreated(final QuestDbContext dbContext) throws Exception {
        dbContext.getCompiler().compile(CREATE_TABLE, dbContext.getSqlExecutionContext());
    }

    private void givenTableIsPopulated(final String... dates) throws Exception {
        int value = 0;

        for (final String date : dates) {
            givenTableIsPopulated(date, value++);
        }
    }

    private void givenTableIsPopulated(final String date, final int value) throws Exception {
        final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss z");
        final ZonedDateTime parsedDate = ZonedDateTime.parse(date, formatter);

        final SqlExecutionContext executionContext = dbContext.getSqlExecutionContext();
        final TableWriter tableWriter = dbContext.getEngine().getWriter(executionContext.getCairoSecurityContext(), "measurements");

        final TableWriter.Row row = tableWriter.newRow(TimeUnit.MILLISECONDS.toMicros(parsedDate.toInstant().toEpochMilli()));
        row.putInt(1, value);
        row.append();

        tableWriter.commit();
        tableWriter.close();
    }

    private void whenRollOverIsExecuted(final String executedAt) throws Exception {
        final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss z");
        final ZonedDateTime executionTime = ZonedDateTime.parse(executedAt, formatter);

        final Supplier<ZonedDateTime> timeSource = () -> executionTime;
        final EmbeddedQuestDbRolloverHandler testSubject = new EmbeddedQuestDbRolloverHandler(timeSource, Collections.singletonList("measurements"), 2, dbContext);
        testSubject.run();
    }

    private void thenTheRemainingPartitionsAre(final String... expectedPartitions) throws Exception {
        final SqlExecutionContext executionContext = dbContext.getSqlExecutionContext();
        final RecordCursorFactory cursorFactory = dbContext.getCompiler()
                .compile(String.format(EmbeddedQuestDbRolloverHandler.SELECTION_QUERY, "measurements"), executionContext).getRecordCursorFactory();
        final RecordCursor cursor = cursorFactory.getCursor(executionContext);

        final List<String> existingPartitions = new LinkedList<>();

        while (cursor.hasNext()) {
            final Record record = cursor.getRecord();
            existingPartitions.add(new StringBuilder(record.getStr(0)).toString());
        }

        Assert.assertEquals(expectedPartitions.length, existingPartitions.size());

        for (final String expectedPartition : expectedPartitions) {
            Assert.assertTrue("Partition " + expectedPartition + " is expected", existingPartitions.contains(expectedPartition));
        }
    }

    private void thenNoPartitionsExpected() throws Exception {
        thenTheRemainingPartitionsAre();
    }
}
