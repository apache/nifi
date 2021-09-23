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

import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class JsonNodeStatusHistoryDumpFactoryTest {

    private static final int DAYS = 3;

    @Test
    public void testJsonNodeStatusDumpFactory() {
        final StatusHistoryRepository statusHistoryRepository = mock(StatusHistoryRepository.class);
        final ArgumentCaptor<Date> fromArgumentCaptor = ArgumentCaptor.forClass(Date.class);
        final ArgumentCaptor<Date> toArgumentCaptor = ArgumentCaptor.forClass(Date.class);

        JsonNodeStatusHistoryDumpFactory factory = new JsonNodeStatusHistoryDumpFactory();
        factory.setStatusHistoryRepository(statusHistoryRepository);

        factory.create(DAYS);

        verify(statusHistoryRepository).getNodeStatusHistory(fromArgumentCaptor.capture(), toArgumentCaptor.capture());

        final LocalDateTime endOfToday = LocalDateTime.now().with(LocalTime.MAX);
        final LocalDateTime startOfDaysBefore = endOfToday.minusDays(DAYS).with(LocalTime.MIN);

        final Date endOfTodayDate = Date.from(endOfToday.atZone(ZoneId.systemDefault()).toInstant());
        final Date startOfDaysBeforeDate = Date.from(startOfDaysBefore.atZone(ZoneId.systemDefault()).toInstant());

        assertEquals(endOfTodayDate, toArgumentCaptor.getValue());
        assertEquals(startOfDaysBeforeDate, fromArgumentCaptor.getValue());
    }

}
