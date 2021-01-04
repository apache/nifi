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

import org.apache.nifi.controller.status.NodeStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.StorageStatus;
import org.apache.nifi.util.NiFiProperties;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.testng.Assert;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;

import static junit.framework.TestCase.assertTrue;
import static org.apache.nifi.controller.status.history.VolatileComponentStatusRepository.DEFAULT_NUM_DATA_POINTS;
import static org.apache.nifi.controller.status.history.VolatileComponentStatusRepository.NUM_DATA_POINTS_PROPERTY;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertEquals;

/**
 * This class verifies the VolatileComponentStatusRepository getConnectionStatusHistory method
 * honors the start/end/preferredDataPoints variables by testing the filterDates function.
 */
public class VolatileComponentStatusRepositoryTest {

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
    NiFiProperties props = mock(NiFiProperties.class);

    when(props.getIntegerProperty(NUM_DATA_POINTS_PROPERTY, DEFAULT_NUM_DATA_POINTS)).thenReturn(bufferSize);

    VolatileComponentStatusRepository repo = new VolatileComponentStatusRepository(props);

    return repo;
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

  @Test
  public void testNodeStatusHistory() {
    // given
    final VolatileComponentStatusRepository testSubject = createRepo(BUFSIZE3);
    final List<NodeStatus> nodeStatuses = Arrays.asList(
        givenNodeStatus(0),
        givenNodeStatus(1)
    );

    testSubject.capture(nodeStatuses.get(0), givenProcessGroupStatus(), givenGarbageCollectionStatus(1, 100, 2, 300));
    testSubject.capture(nodeStatuses.get(1), givenProcessGroupStatus(), givenGarbageCollectionStatus(1, 100, 5, 700));

    // when
    final StatusHistory result = testSubject.getNodeStatusHistory();

    // then
    // checking on snapshots
    Assert.assertEquals(nodeStatuses.size(), result.getStatusSnapshots().size());;

    // metrics based on NodeStatus
    for (int i = 0; i < result.getStatusSnapshots().size(); i++) {
      final StatusSnapshot snapshot = result.getStatusSnapshots().get(i);
      final NodeStatus nodeStatus = nodeStatuses.get(i);

      Assert.assertEquals(nodeStatus.getFreeHeap(), snapshot.getStatusMetric(NodeStatusDescriptor.FREE_HEAP.getDescriptor()).longValue());
      Assert.assertEquals(nodeStatus.getUsedHeap(), snapshot.getStatusMetric(NodeStatusDescriptor.USED_HEAP.getDescriptor()).longValue());
      Assert.assertEquals(nodeStatus.getHeapUtilization(), snapshot.getStatusMetric(NodeStatusDescriptor.HEAP_UTILIZATION.getDescriptor()).longValue());
      Assert.assertEquals(nodeStatus.getFreeNonHeap(), snapshot.getStatusMetric(NodeStatusDescriptor.FREE_NON_HEAP.getDescriptor()).longValue());
      Assert.assertEquals(nodeStatus.getUsedNonHeap(), snapshot.getStatusMetric(NodeStatusDescriptor.USED_NON_HEAP.getDescriptor()).longValue());
      Assert.assertEquals(nodeStatus.getOpenFileHandlers(), snapshot.getStatusMetric(NodeStatusDescriptor.OPEN_FILE_HANDLES.getDescriptor()).longValue());
      Assert.assertEquals(
              Double.valueOf(nodeStatus.getProcessorLoadAverage() * MetricDescriptor.FRACTION_MULTIPLIER).longValue(),
              snapshot.getStatusMetric(NodeStatusDescriptor.PROCESSOR_LOAD_AVERAGE.getDescriptor()).longValue());
      Assert.assertEquals(nodeStatus.getTotalThreads(), snapshot.getStatusMetric(NodeStatusDescriptor.TOTAL_THREADS.getDescriptor()).longValue());
      Assert.assertEquals(nodeStatus.getEventDrivenThreads(), snapshot.getStatusMetric(NodeStatusDescriptor.EVENT_DRIVEN_THREADS.getDescriptor()).longValue());
      Assert.assertEquals(nodeStatus.getTimerDrivenThreads(), snapshot.getStatusMetric(NodeStatusDescriptor.TIME_DRIVEN_THREADS.getDescriptor()).longValue());
      Assert.assertEquals(nodeStatus.getFlowFileRepositoryFreeSpace(), snapshot.getStatusMetric(NodeStatusDescriptor.FLOW_FILE_REPOSITORY_FREE_SPACE.getDescriptor()).longValue());
      Assert.assertEquals(nodeStatus.getFlowFileRepositoryUsedSpace(), snapshot.getStatusMetric(NodeStatusDescriptor.FLOW_FILE_REPOSITORY_USED_SPACE.getDescriptor()).longValue());
      Assert.assertEquals(
              nodeStatus.getContentRepositories().stream().map(r -> r.getFreeSpace()).reduce(0L, (a, b) -> a + b).longValue(),
              snapshot.getStatusMetric(NodeStatusDescriptor.CONTENT_REPOSITORY_FREE_SPACE.getDescriptor()).longValue());
      Assert.assertEquals(
              nodeStatus.getContentRepositories().stream().map(r -> r.getUsedSpace()).reduce(0L, (a, b) -> a + b).longValue(),
              snapshot.getStatusMetric(NodeStatusDescriptor.CONTENT_REPOSITORY_USED_SPACE.getDescriptor()).longValue());
      Assert.assertEquals(
              nodeStatus.getProvenanceRepositories().stream().map(r -> r.getFreeSpace()).reduce(0L, (a, b) -> a + b).longValue(),
              snapshot.getStatusMetric(NodeStatusDescriptor.PROVENANCE_REPOSITORY_FREE_SPACE.getDescriptor()).longValue());
      Assert.assertEquals(
              nodeStatus.getProvenanceRepositories().stream().map(r -> r.getUsedSpace()).reduce(0L, (a, b) -> a + b).longValue(),
              snapshot.getStatusMetric(NodeStatusDescriptor.PROVENANCE_REPOSITORY_USED_SPACE.getDescriptor()).longValue());

      // metrics based on repositories
      Assert.assertEquals(12 + i, getMetricAtOrdinal(snapshot, 16)); // c1 free
      Assert.assertEquals(13 + i, getMetricAtOrdinal(snapshot, 17)); // c1 used
      Assert.assertEquals(14 + i, getMetricAtOrdinal(snapshot, 18)); // c2 free
      Assert.assertEquals(15 + i, getMetricAtOrdinal(snapshot, 19)); // c2 used

      Assert.assertEquals(16 + i, getMetricAtOrdinal(snapshot, 20)); // p1 free
      Assert.assertEquals(17 + i, getMetricAtOrdinal(snapshot, 21)); // p1 used
      Assert.assertEquals(18 + i, getMetricAtOrdinal(snapshot, 22)); // p2 free
      Assert.assertEquals(19 + i, getMetricAtOrdinal(snapshot, 23)); // p2 used
    }

    // metrics based on GarbageCollectionStatus (The ordinal numbers are true for setup, in production it might differ)
    final int g0TimeOrdinal = 24;
    final int g0CountOrdinal = 25;
    final int g0TimeDiffOrdinal = 26;
    final int g0CountDiffOrdinal = 27;
    final int g1TimeOrdinal = 28;
    final int g1CountOrdinal = 29;
    final int g1TimeDiffOrdinal = 30;
    final int g1CountDiffOrdinal = 31;

    final StatusSnapshot snapshot1 = result.getStatusSnapshots().get(0);
    final StatusSnapshot snapshot2 = result.getStatusSnapshots().get(1);

    Assert.assertEquals(100L, getMetricAtOrdinal(snapshot1, g0TimeOrdinal));
    Assert.assertEquals(0L, getMetricAtOrdinal(snapshot1, g0TimeDiffOrdinal));
    Assert.assertEquals(1L, getMetricAtOrdinal(snapshot1, g0CountOrdinal));
    Assert.assertEquals(0L, getMetricAtOrdinal(snapshot1, g0CountDiffOrdinal));
    Assert.assertEquals(300L, getMetricAtOrdinal(snapshot1, g1TimeOrdinal));
    Assert.assertEquals(0L, getMetricAtOrdinal(snapshot1, g1TimeDiffOrdinal));
    Assert.assertEquals(2L, getMetricAtOrdinal(snapshot1, g1CountOrdinal));
    Assert.assertEquals(0L, getMetricAtOrdinal(snapshot1, g1CountDiffOrdinal));

    Assert.assertEquals(100L, getMetricAtOrdinal(snapshot2, g0TimeOrdinal));
    Assert.assertEquals(0L, getMetricAtOrdinal(snapshot2, g0TimeDiffOrdinal));
    Assert.assertEquals(1L, getMetricAtOrdinal(snapshot2, g0CountOrdinal));
    Assert.assertEquals(0L, getMetricAtOrdinal(snapshot2, g0CountDiffOrdinal));
    Assert.assertEquals(700L, getMetricAtOrdinal(snapshot2, g1TimeOrdinal));
    Assert.assertEquals(400L, getMetricAtOrdinal(snapshot2, g1TimeDiffOrdinal));
    Assert.assertEquals(5L, getMetricAtOrdinal(snapshot2, g1CountOrdinal));
    Assert.assertEquals(3L, getMetricAtOrdinal(snapshot2, g1CountDiffOrdinal));
  }

  private long getMetricAtOrdinal(final StatusSnapshot snapshot, final long ordinal) {
    final Set<MetricDescriptor<?>> metricDescriptors = snapshot.getMetricDescriptors();

    for (final MetricDescriptor<?> metricDescriptor : metricDescriptors) {
      if (metricDescriptor.getMetricIdentifier() == ordinal) {
        return snapshot.getStatusMetric(metricDescriptor);
      }
    }

    Assert.fail();
    return Long.MIN_VALUE;
  }

  private NodeStatus givenNodeStatus(final int number) {
    final NodeStatus result = new NodeStatus();
    result.setCreatedAtInMs(System.currentTimeMillis());
    result.setFreeHeap(1 + number);
    result.setUsedHeap(2 + number);
    result.setHeapUtilization(3 + number);
    result.setFreeNonHeap(4 + number);
    result.setUsedNonHeap(5 + number);
    result.setOpenFileHandlers(6 + number);
    result.setProcessorLoadAverage(7.1d + number);
    result.setTotalThreads(9 + number);
    result.setEventDrivenThreads(20 + number);
    result.setTimerDrivenThreads(21 + number);
    result.setFlowFileRepositoryFreeSpace(10 + number);
    result.setFlowFileRepositoryUsedSpace(11 + number);
    result.setContentRepositories(Arrays.asList(
            givenStorageStatus("c1", 12 + number, 13 + number),
            givenStorageStatus("c2", 14 + number, 15 + number)
    ));
    result.setProvenanceRepositories(Arrays.asList(
            givenStorageStatus("p1", 16 + number, 17 + number),
            givenStorageStatus("p2", 18 + number, 19 + number)
    ));

    return result;
  }

  private StorageStatus givenStorageStatus(final String name, final long freeSpace, final long usedSpace) {
    final StorageStatus result = new StorageStatus();
    result.setName(name);
    result.setFreeSpace(freeSpace);
    result.setUsedSpace(usedSpace);
    return result;
  }

  private ProcessGroupStatus givenProcessGroupStatus() {
    final ProcessGroupStatus result = Mockito.mock(ProcessGroupStatus.class);
    Mockito.when(result.getId()).thenReturn("rootId");
    Mockito.when(result.getName()).thenReturn("rootName");
    Mockito.when(result.getProcessorStatus()).thenReturn(Collections.emptyList());
    Mockito.when(result.getConnectionStatus()).thenReturn(Collections.emptyList());
    Mockito.when(result.getRemoteProcessGroupStatus()).thenReturn(Collections.emptyList());
    Mockito.when(result.getProcessGroupStatus()).thenReturn(Collections.emptyList());
    return result;
  }

  private List<GarbageCollectionStatus> givenGarbageCollectionStatus(long gc1Count, long gc1Millis, long gc2Count, long gc2Millis) {
    final List<GarbageCollectionStatus> result = new ArrayList<>(2);
    result.add(new StandardGarbageCollectionStatus("gc0", new Date(), gc1Count, gc1Millis));
    result.add(new StandardGarbageCollectionStatus("gc1", new Date(), gc2Count, gc2Millis));
    return result;
  }
}
