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

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class JsonNodeStatusHistoryDumpFactoryTest {

    private static final String EXPECTED_EXCEPTION_MESSAGE = "The number of days shall be greater than 0. The current value is %s.";

    @Test
    public void testJsonNodeStatusDumpFactory() {
        final int days = 3;
        final StatusHistoryRepository statusHistoryRepository = mock(StatusHistoryRepository.class);
        final ArgumentCaptor<Date> fromArgumentCaptor = ArgumentCaptor.forClass(Date.class);
        final ArgumentCaptor<Date> toArgumentCaptor = ArgumentCaptor.forClass(Date.class);

        JsonNodeStatusHistoryDumpFactory factory = new JsonNodeStatusHistoryDumpFactory();
        factory.setStatusHistoryRepository(statusHistoryRepository);

        factory.create(days);

        verify(statusHistoryRepository).getNodeStatusHistory(fromArgumentCaptor.capture(), toArgumentCaptor.capture());

        final LocalDateTime endOfToday = LocalDateTime.now().with(LocalTime.MAX);
        final LocalDateTime startOfDaysBefore = endOfToday.minusDays(days).with(LocalTime.MIN);

        final Date endOfTodayDate = Date.from(endOfToday.atZone(ZoneId.systemDefault()).toInstant());
        final Date startOfDaysBeforeDate = Date.from(startOfDaysBefore.atZone(ZoneId.systemDefault()).toInstant());

        assertEquals(endOfTodayDate, toArgumentCaptor.getValue());
        assertEquals(startOfDaysBeforeDate, fromArgumentCaptor.getValue());
    }

    @Test
    public void testJsonNodeStatusDumpFactoryWithLessThanOneDayThrowsException() {
        final int zeroDays = 0;
        final int negativeDays = -1;
        final StatusHistoryRepository statusHistoryRepository = mock(StatusHistoryRepository.class);

        JsonNodeStatusHistoryDumpFactory factory = new JsonNodeStatusHistoryDumpFactory();
        factory.setStatusHistoryRepository(statusHistoryRepository);

        final IllegalArgumentException zeroDaysException = assertThrows(IllegalArgumentException.class,
                () -> factory.create(zeroDays)
        );

        assertEquals(String.format(EXPECTED_EXCEPTION_MESSAGE, zeroDays), zeroDaysException.getMessage());

        final IllegalArgumentException negativeDaysException = assertThrows(IllegalArgumentException.class,
                () -> factory.create(negativeDays)
        );

        assertEquals(String.format(EXPECTED_EXCEPTION_MESSAGE, negativeDays), negativeDaysException.getMessage());
    }

}
