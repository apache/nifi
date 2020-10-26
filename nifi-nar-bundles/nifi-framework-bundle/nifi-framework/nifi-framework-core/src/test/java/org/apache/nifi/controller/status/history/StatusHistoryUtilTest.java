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

import org.apache.nifi.web.api.dto.status.StatusDescriptorDTO;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class StatusHistoryUtilTest {

    @Test
    public void testCreateFieldDescriptorDtos() {
        // given
        final Collection<MetricDescriptor<?>> metricDescriptors = Arrays.asList(
                new StandardMetricDescriptor<>(() -> 1, "field2",  "Field2", "Field 2", MetricDescriptor.Formatter.COUNT, __ -> 2L),
                new StandardMetricDescriptor<>(() -> 0, "field1", "Field1", "Field 1", MetricDescriptor.Formatter.COUNT, __ -> 1L)
        );

        final List<StatusDescriptorDTO> expected = Arrays.asList(
                new StatusDescriptorDTO("field1", "Field1", "Field 1", MetricDescriptor.Formatter.COUNT.name()),
                new StatusDescriptorDTO("field2", "Field2", "Field 2", MetricDescriptor.Formatter.COUNT.name())
        );

        // when
        final List<StatusDescriptorDTO> result = StatusHistoryUtil.createFieldDescriptorDtos(metricDescriptors);

        // then
        Assert.assertEquals(expected, result);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testCreateFieldDescriptorDtosWhenNotConsecutive() {
        // given
        final Collection<MetricDescriptor<?>> metricDescriptors = Arrays.asList(
                new StandardMetricDescriptor<>(() -> 3, "field2",  "Field2", "Field 2", MetricDescriptor.Formatter.COUNT, __ -> 2L),
                new StandardMetricDescriptor<>(() -> 0, "field1", "Field1", "Field 1", MetricDescriptor.Formatter.COUNT, __ -> 1L)
        );

        // when
        StatusHistoryUtil.createFieldDescriptorDtos(metricDescriptors);
    }

    @Test
    public void testCreateFieldDescriptorDtosWhenEmpty() {
        // given
        final Collection<MetricDescriptor<?>> metricDescriptors = new ArrayList<>();
        final List<StatusDescriptorDTO> expected = new ArrayList<>();

        // when
        final List<StatusDescriptorDTO> result = StatusHistoryUtil.createFieldDescriptorDtos(metricDescriptors);

        // then
        Assert.assertEquals(expected, result);
    }

    @Test
    public void testCreateFieldDescriptorDtosWhenCounterTypeAppears() {
        // given
        final Collection<MetricDescriptor<?>> metricDescriptors = Arrays.asList(
                new CounterMetricDescriptor<>("fieldCounter1", "FieldCounter1", "Field Counter 1", MetricDescriptor.Formatter.COUNT, __ -> 3L),
                new StandardMetricDescriptor<>(() -> 1, "field2",  "Field2", "Field 2", MetricDescriptor.Formatter.COUNT, __ -> 2L),
                new CounterMetricDescriptor<>("fieldCounter2", "FieldCounter2", "Field Counter 2", MetricDescriptor.Formatter.COUNT, __ -> 4L),
                new StandardMetricDescriptor<>(() -> 0, "field1", "Field1", "Field 1", MetricDescriptor.Formatter.COUNT, __ -> 1L),
                new CounterMetricDescriptor<>("fieldCounter3", "FieldCounter3", "Field Counter 3", MetricDescriptor.Formatter.COUNT, __ -> 5L)
        );

        final List<StatusDescriptorDTO> expected = Arrays.asList(
                new StatusDescriptorDTO("field1", "Field1", "Field 1", MetricDescriptor.Formatter.COUNT.name()),
                new StatusDescriptorDTO("field2", "Field2", "Field 2", MetricDescriptor.Formatter.COUNT.name()),
                new StatusDescriptorDTO("fieldCounter1", "FieldCounter1", "Field Counter 1", MetricDescriptor.Formatter.COUNT.name()),
                new StatusDescriptorDTO("fieldCounter2", "FieldCounter2", "Field Counter 2", MetricDescriptor.Formatter.COUNT.name()),
                new StatusDescriptorDTO("fieldCounter3", "FieldCounter3", "Field Counter 3", MetricDescriptor.Formatter.COUNT.name())
        );

        // when
        final List<StatusDescriptorDTO> result = StatusHistoryUtil.createFieldDescriptorDtos(metricDescriptors);

        // then
        Assert.assertEquals(expected, result);
    }
}