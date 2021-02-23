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

import org.apache.nifi.util.NiFiProperties;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.List;

import static junit.framework.TestCase.assertTrue;
import static org.testng.AssertJUnit.assertEquals;

/**
 * This class verifies the VolatileComponentStatusRepository getConnectionStatusHistory method
 * honors the start/end/preferredDataPoints variables by testing the filterDates function.
 */
public class VolatileComponentStatusRepositoryForComponentsTest {

  private static VolatileComponentStatusRepository filledRepo;
  private static VolatileComponentStatusRepository partiallyFilledRepo;
  private static VolatileComponentStatusRepository emptyRepo;
  private static final int FIVE_MINUTES = 300000;
  private static int BUFSIZE3 = 10;

  @BeforeClass
  public static void createBuffers() {
    // Fill the repo1 buffer completely with Date objects at five-minute intervals
    // This provides dates up to around Jul 1979
    filledRepo = initRepo(1_000_000, 0);

    // Verify partially filled buffers work as expected.
    partiallyFilledRepo = initRepo(1000, 10);

    emptyRepo = createRepo(BUFSIZE3);
  }

  private static VolatileComponentStatusRepository initRepo(int bufferSize, int offset) {
    VolatileComponentStatusRepository repo = createRepo(bufferSize);

    for (long i = 0; i < bufferSize - offset; i++) {
      repo.timestamps.add(new Date(i * FIVE_MINUTES));
    }
    assertEquals(bufferSize - offset, repo.timestamps.getSize());

    return repo;
  }

  private static VolatileComponentStatusRepository createRepo(int bufferSize) {
    final NiFiProperties niFiProperties = Mockito.mock(NiFiProperties.class);
    Mockito.when(niFiProperties.getIntegerProperty(VolatileComponentStatusRepository.NUM_DATA_POINTS_PROPERTY, VolatileComponentStatusRepository.DEFAULT_NUM_DATA_POINTS)).thenReturn(bufferSize);
    return new VolatileComponentStatusRepository(niFiProperties);
  }

  private static Date asDate(LocalDateTime localDateTime) {
    return Date.from(localDateTime.toInstant(ZoneOffset.UTC));
  }

  @Test
  public void testFilterDatesReturnAll() {
    testFilterDatesReturnAll(filledRepo);
    testFilterDatesReturnAll(partiallyFilledRepo);
  }

  private void testFilterDatesReturnAll(VolatileComponentStatusRepository repo) {
    List<Date> dates = repo.filterDates(null, null, Integer.MAX_VALUE);
    assert repo.timestamps != null;
    assertEquals(repo.timestamps.getSize(), dates.size());
    assertTrue(dates.equals(repo.timestamps.asList()));
    repo.timestamps.add(new Date());
  }

  @Test
  public void testFilterDatesUsingPreferredDataPoints() {
    List<Date> dates = filledRepo.filterDates(null, null, 1);
    assertEquals(1, dates.size());
    assertEquals(filledRepo.timestamps.getNewestElement(), dates.get(0));

    testFilterDatesUsingPreferredDataPoints(filledRepo, 14);
    testFilterDatesUsingPreferredDataPoints(partiallyFilledRepo, 22);
  }

  private void testFilterDatesUsingPreferredDataPoints(VolatileComponentStatusRepository repo, int numPoints) {
    List<Date> dates = repo.filterDates(null, null, numPoints);
    assertEquals(numPoints, dates.size());
    assertEquals(repo.timestamps.getNewestElement(), dates.get(dates.size() - 1));
    assertEquals(repo.timestamps.asList().get(repo.timestamps.getSize() - numPoints), dates.get(0));
  }

  @Test
  public void testFilterDatesUsingStartFilter() {
    // Filter with date that exactly matches an entry in timestamps buffer
    Date start = asDate(LocalDateTime.of(1978, 1, 1, 0, 45, 0));
    List<Date> dates = filledRepo.filterDates(start, null, Integer.MAX_VALUE);
    assertEquals(start, dates.get(0));
    assertEquals(filledRepo.timestamps.getNewestElement(), dates.get(dates.size()-1));

    // filter using a date that does not exactly match the time, i.e., not on a five-minute mark
    start = asDate(LocalDateTime.of(1974, 1, 1, 3, 2, 0));
    dates = filledRepo.filterDates(start, null, Integer.MAX_VALUE);
    assertTrue(start.getTime() < dates.get(0).getTime());
    assertTrue(dates.get(0).getTime() < (start.getTime() + FIVE_MINUTES));
    assertEquals(filledRepo.timestamps.getNewestElement(), dates.get(dates.size()-1));

    start = asDate(LocalDateTime.of(1970, 1, 1, 0, 0, 0));
    dates = partiallyFilledRepo.filterDates(start, null, Integer.MAX_VALUE);
    assertEquals(start, dates.get(0));
    assertEquals(partiallyFilledRepo.timestamps.getNewestElement(), dates.get(dates.size()-1));
  }

  @Test
  public void testFilterDatesUsingEndFilter() {
    // Filter with date that exactly matches an entry in timestamps buffer
    Date end = asDate(LocalDateTime.of(1970, 2, 1,1, 10, 0));
    List<Date> dates = filledRepo.filterDates(null, end, Integer.MAX_VALUE);
    assertEquals(end, dates.get(dates.size()-1));
    assertEquals(filledRepo.timestamps.getOldestElement(), dates.get(0));

    // filter using a date that does not exactly match the times in buffer
    end = asDate(LocalDateTime.of(1970, 2, 1,1, 7, 0));
    dates = filledRepo.filterDates(null, end, Integer.MAX_VALUE);
    assertTrue(dates.get(dates.size()-1).getTime() < end.getTime());
    assertTrue((end.getTime() - FIVE_MINUTES) < dates.get(dates.size()-1).getTime());
    assertEquals(dates.get(0), filledRepo.timestamps.getOldestElement());

    end = asDate(LocalDateTime.of(1970, 1, 2,1, 7, 0));
    dates = partiallyFilledRepo.filterDates(null, end, Integer.MAX_VALUE);
    assertTrue(dates.get(dates.size()-1).getTime() < end.getTime());
    assertTrue((end.getTime() - FIVE_MINUTES) < dates.get(dates.size()-1).getTime());
    assertEquals(partiallyFilledRepo.timestamps.asList().get(0), dates.get(0));
  }

  @Test
  public void testFilterDatesUsingStartAndEndFilter() {
    // Filter with dates that exactly matches entries in timestamps buffer
    Date start = asDate(LocalDateTime.of(1975, 3, 1, 3, 15, 0));
    Date end = asDate(LocalDateTime.of(1978, 4, 2,4, 25, 0));
    List<Date> dates = filledRepo.filterDates(start, end, Integer.MAX_VALUE);
    assertEquals(start, dates.get(0));
    assertEquals(end, dates.get(dates.size()-1));

    // Filter with dates that do not exactly matches entries in timestamps buffer
    start = asDate(LocalDateTime.of(1975, 3, 1, 3, 3, 0));
    end = asDate(LocalDateTime.of(1977, 4, 2,4, 8, 0));
    dates = filledRepo.filterDates(start, end, Integer.MAX_VALUE);
    assertTrue(start.getTime() < dates.get(0).getTime());
    assertTrue(dates.get(0).getTime() < (start.getTime() + FIVE_MINUTES));
    assertTrue(dates.get(dates.size()-1).getTime() < end.getTime());
    assertTrue((end.getTime() - FIVE_MINUTES) < dates.get(dates.size()-1).getTime());

    start = asDate(LocalDateTime.of(1970, 1, 1, 3, 15, 0));
    end = asDate(LocalDateTime.of(1970, 1, 2,4, 25, 0));
    dates = filledRepo.filterDates(start, end, Integer.MAX_VALUE);
    assertEquals(start, dates.get(0));
    assertEquals(end, dates.get(dates.size()-1));
  }

  @Test
  public void testFilterDatesUsingStartEndAndPreferredFilter() {
    // Filter with dates that exactly matches entries in timestamps buffer
    int numPoints = 5;
    Date start = asDate(LocalDateTime.of(1977, 1, 1, 0, 30, 0));
    Date end = asDate(LocalDateTime.of(1977, 2, 1,1, 0, 0));
    List<Date> dates = filledRepo.filterDates(start, end, numPoints);
    assertEquals(numPoints, dates.size());
    assertEquals(dates.get(dates.size()-1), end);
    assertEquals(dates.get(dates.size()-numPoints), new Date(end.getTime() - (numPoints-1)*FIVE_MINUTES));

    // Filter with dates that do not exactly matches entries in timestamps buffer
    start = asDate(LocalDateTime.of(1975, 1, 1, 0, 31, 0));
    end = asDate(LocalDateTime.of(1978, 2, 1,1, 59, 0));
    dates = filledRepo.filterDates(start, end, numPoints);
    assertTrue(dates.get(0).getTime() < new Date(end.getTime() - (numPoints-1)*FIVE_MINUTES).getTime());
    assertTrue(new Date(end.getTime() - (numPoints * FIVE_MINUTES)).getTime() < dates.get(0).getTime());
    assertTrue(dates.get(dates.size()-1).getTime() < end.getTime());
    assertTrue((end.getTime() - FIVE_MINUTES) < dates.get(dates.size() - 1).getTime());
    assertEquals(numPoints, dates.size());

    start = asDate(LocalDateTime.of(1970, 1, 1, 0, 31, 0));
    end = asDate(LocalDateTime.of(1970, 1, 1,1, 59, 0));
    dates = partiallyFilledRepo.filterDates(start, end, numPoints);
    assertTrue(dates.get(0).getTime() < new Date(end.getTime() - (numPoints-1)*FIVE_MINUTES).getTime());
    assertTrue(new Date(end.getTime() - (numPoints * FIVE_MINUTES)).getTime() < dates.get(0).getTime());
    assertTrue(dates.get(dates.size()-1).getTime() < end.getTime());
    assertTrue((end.getTime() - FIVE_MINUTES) < dates.get(dates.size() - 1).getTime());
    assertEquals(numPoints, dates.size());
  }

  @Test
  public void testFilterWorksWithCircularBuffer() {
    // Fill repo3 with Date objects at five-minute intervals
    // This repository is used to verify circular actions behave as expected.
    for (int i = 0; i < BUFSIZE3 + 15; i++) {
      emptyRepo.timestamps.add(new Date(i * FIVE_MINUTES));
      List<Date> dates = emptyRepo.filterDates(null, null, Integer.MAX_VALUE);
      if (i < BUFSIZE3 - 1) {
        assertEquals(null, emptyRepo.timestamps.getOldestElement());
      } else {
        assertEquals(emptyRepo.timestamps.getOldestElement(), dates.get(0));
      }
      assertEquals(emptyRepo.timestamps.asList().get(0), dates.get(0));
      assertEquals(emptyRepo.timestamps.getNewestElement(), dates.get(dates.size() - 1));
    }
  }
}