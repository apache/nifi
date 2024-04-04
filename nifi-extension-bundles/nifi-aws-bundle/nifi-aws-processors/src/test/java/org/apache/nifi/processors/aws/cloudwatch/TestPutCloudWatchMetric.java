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
package org.apache.nifi.processors.aws.cloudwatch;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processors.aws.testutil.AuthUtils;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestPutCloudWatchMetric {

    private TestRunner runner;
    private MockPutCloudWatchMetric mockPutCloudWatchMetric;

    @BeforeEach
    public void setup() {
        mockPutCloudWatchMetric = new MockPutCloudWatchMetric();
        runner = TestRunners.newTestRunner(mockPutCloudWatchMetric);

        runner.setProperty(PutCloudWatchMetric.NAMESPACE, "TestNamespace");
        runner.setProperty(PutCloudWatchMetric.METRIC_NAME, "TestMetric");
        AuthUtils.enableAccessKey(runner, "accessKeyId", "secretKey");
    }

    @Test
    public void testPutSimpleMetric() {
        runner.setProperty(PutCloudWatchMetric.VALUE, "1.0");
        runner.setProperty(PutCloudWatchMetric.UNIT, "Count");
        runner.setProperty(PutCloudWatchMetric.TIMESTAMP, "1476296132575");
        runner.assertValid();

        runner.enqueue(new byte[] {});
        runner.run();

        runner.assertAllFlowFilesTransferred(PutCloudWatchMetric.REL_SUCCESS, 1);
        assertEquals(1, mockPutCloudWatchMetric.putMetricDataCallCount);
        assertEquals("TestNamespace", mockPutCloudWatchMetric.actualNamespace);
        MetricDatum datum = mockPutCloudWatchMetric.actualMetricData.get(0);
        assertEquals("TestMetric", datum.metricName());
        assertEquals(1d, datum.value(), 0.0001d);
    }

    @Test
    public void testValueLiteralDoubleInvalid() {
        runner.setProperty(PutCloudWatchMetric.VALUE, "nan");
        runner.assertNotValid();
    }

    @Test
    public void testMissingBothValueAndStatisticSetInvalid() {
        runner.assertNotValid();
    }

    @Test
    public void testContainsBothValueAndStatisticSetInvalid() {
        runner.setProperty(PutCloudWatchMetric.VALUE, "1.0");
        runner.setProperty(PutCloudWatchMetric.UNIT, "Count");
        runner.setProperty(PutCloudWatchMetric.TIMESTAMP, "1476296132575");
        runner.setProperty(PutCloudWatchMetric.MINIMUM, "1.0");
        runner.setProperty(PutCloudWatchMetric.MAXIMUM, "2.0");
        runner.setProperty(PutCloudWatchMetric.SUM, "3.0");
        runner.setProperty(PutCloudWatchMetric.SAMPLECOUNT, "2");
        runner.assertNotValid();
    }

    @Test
    public void testContainsIncompleteStatisticSetInvalid() {
        runner.setProperty(PutCloudWatchMetric.UNIT, "Count");
        runner.setProperty(PutCloudWatchMetric.TIMESTAMP, "1476296132575");
        runner.setProperty(PutCloudWatchMetric.MINIMUM, "1.0");
        runner.setProperty(PutCloudWatchMetric.MAXIMUM, "2.0");
        runner.setProperty(PutCloudWatchMetric.SUM, "3.0");
        // missing sample count
        runner.assertNotValid();
    }

    @Test
    public void testContainsBothValueAndIncompleteStatisticSetInvalid() {
        runner.setProperty(PutCloudWatchMetric.VALUE, "1.0");
        runner.setProperty(PutCloudWatchMetric.UNIT, "Count");
        runner.setProperty(PutCloudWatchMetric.TIMESTAMP, "1476296132575");
        runner.setProperty(PutCloudWatchMetric.MINIMUM, "1.0");
        runner.assertNotValid();
    }

    @Test
    public void testMetricExpressionValid() {
        runner.setProperty(PutCloudWatchMetric.VALUE, "${metric.value}");
        runner.assertValid();

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("metric.value", "1.23");
        runner.enqueue(new byte[] {}, attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred(PutCloudWatchMetric.REL_SUCCESS, 1);
        assertEquals(1, mockPutCloudWatchMetric.putMetricDataCallCount);
        assertEquals("TestNamespace", mockPutCloudWatchMetric.actualNamespace);
        MetricDatum datum = mockPutCloudWatchMetric.actualMetricData.get(0);
        assertEquals("TestMetric", datum.metricName());
        assertEquals(1.23d, datum.value(), 0.0001d);
    }

    @Test
    public void testStatisticSet() {
        runner.setProperty(PutCloudWatchMetric.MINIMUM, "${metric.min}");
        runner.setProperty(PutCloudWatchMetric.MAXIMUM, "${metric.max}");
        runner.setProperty(PutCloudWatchMetric.SUM, "${metric.sum}");
        runner.setProperty(PutCloudWatchMetric.SAMPLECOUNT, "${metric.count}");
        runner.assertValid();

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("metric.min", "1");
        attributes.put("metric.max", "2");
        attributes.put("metric.sum", "3");
        attributes.put("metric.count", "2");
        runner.enqueue(new byte[] {}, attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred(PutCloudWatchMetric.REL_SUCCESS, 1);
        assertEquals(1, mockPutCloudWatchMetric.putMetricDataCallCount);
        assertEquals("TestNamespace", mockPutCloudWatchMetric.actualNamespace);
        MetricDatum datum = mockPutCloudWatchMetric.actualMetricData.get(0);
        assertEquals("TestMetric", datum.metricName());
        assertEquals(1.0d, datum.statisticValues().minimum(), 0.0001d);
        assertEquals(2.0d, datum.statisticValues().maximum(), 0.0001d);
        assertEquals(3.0d, datum.statisticValues().sum(), 0.0001d);
        assertEquals(2.0d, datum.statisticValues().sampleCount(), 0.0001d);
    }

    @Test
    public void testDimensions() {
        runner.setProperty(PutCloudWatchMetric.VALUE, "1.0");
        runner.setProperty(PutCloudWatchMetric.UNIT, "Count");
        runner.setProperty(PutCloudWatchMetric.TIMESTAMP, "1476296132575");
        runner.setProperty(new PropertyDescriptor.Builder().dynamic(true).name("dim1").build(), "${metric.dim1}");
        runner.setProperty(new PropertyDescriptor.Builder().dynamic(true).name("dim2").build(), "val2");
        runner.assertValid();

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("metric.dim1", "1");
        runner.enqueue(new byte[] {}, attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred(PutCloudWatchMetric.REL_SUCCESS, 1);
        assertEquals(1, mockPutCloudWatchMetric.putMetricDataCallCount);
        assertEquals("TestNamespace", mockPutCloudWatchMetric.actualNamespace);
        MetricDatum datum = mockPutCloudWatchMetric.actualMetricData.get(0);
        assertEquals("TestMetric", datum.metricName());
        assertEquals(1d, datum.value(), 0.0001d);

        List<Dimension> dimensions = new ArrayList<>(datum.dimensions());
        dimensions.sort(Comparator.comparing(Dimension::name));
        assertEquals(2, dimensions.size());
        assertEquals("dim1", dimensions.get(0).name());
        assertEquals("1", dimensions.get(0).value());
        assertEquals("dim2", dimensions.get(1).name());
        assertEquals("val2", dimensions.get(1).value());
    }

    @Test
    public void testMaximumDimensions() {
        runner.setProperty(PutCloudWatchMetric.VALUE, "1.0");
        runner.setProperty(PutCloudWatchMetric.UNIT, "Count");
        runner.setProperty(PutCloudWatchMetric.TIMESTAMP, "1476296132575");
        for (int i=0; i < 10; i++) {
            runner.setProperty(new PropertyDescriptor.Builder().dynamic(true).name("dim" + i).build(), "0");
        }
        runner.assertValid();
    }

    @Test
    public void testTooManyDimensions() {
        runner.setProperty(PutCloudWatchMetric.VALUE, "1.0");
        runner.setProperty(PutCloudWatchMetric.UNIT, "Count");
        runner.setProperty(PutCloudWatchMetric.TIMESTAMP, "1476296132575");
        for (int i=0; i < 11; i++) {
            runner.setProperty(new PropertyDescriptor.Builder().dynamic(true).name("dim" + i).build(), "0");
        }
        runner.assertNotValid();
    }

    @Test
    public void testMetricExpressionInvalidRoutesToFailure() {
        runner.setProperty(PutCloudWatchMetric.VALUE, "${metric.value}");
        runner.assertValid();

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("metric.value", "nan");
        runner.enqueue(new byte[] {}, attributes);
        runner.run();

        assertEquals(0, mockPutCloudWatchMetric.putMetricDataCallCount);
        runner.assertAllFlowFilesTransferred(PutCloudWatchMetric.REL_FAILURE, 1);
    }

    @ParameterizedTest
    @CsvSource({"nan","percent","count"})
    public void testInvalidUnit(String unit) {
        runner.setProperty(PutCloudWatchMetric.UNIT, unit);
        runner.setProperty(PutCloudWatchMetric.VALUE, "1.0");
        runner.assertNotValid();
    }

    private static Stream<Arguments> data() {
        return PutCloudWatchMetric.units.stream().map(Arguments::of);
    }

    @ParameterizedTest
    @MethodSource("data")
    public void testValidUnit(String unit) {
        runner.setProperty(PutCloudWatchMetric.UNIT, unit);
        runner.setProperty(PutCloudWatchMetric.VALUE, "1");
        runner.assertValid();
    }

    @Test
    public void testTimestampExpressionInvalidRoutesToFailure() {
        runner.setProperty(PutCloudWatchMetric.UNIT, "Count");
        runner.setProperty(PutCloudWatchMetric.VALUE, "1");
        runner.setProperty(PutCloudWatchMetric.TIMESTAMP, "${timestamp.value}");
        runner.assertValid();

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("timestamp.value", "1476296132575broken");
        runner.enqueue(new byte[] {}, attributes);
        runner.run();

        assertEquals(0, mockPutCloudWatchMetric.putMetricDataCallCount);
        runner.assertAllFlowFilesTransferred(PutCloudWatchMetric.REL_FAILURE, 1);
    }


    @ParameterizedTest
    @CsvSource({"null","us-west-100","us-east-a"})
    public void testInvalidRegion(String region) {
        runner.setProperty(PutCloudWatchMetric.VALUE, "6");
        runner.setProperty(PutCloudWatchMetric.REGION, region);
        runner.assertNotValid();
    }

    @ParameterizedTest
    @CsvSource({"us-east-1","us-west-1","us-east-2"})
    public void testValidRegionRoutesToSuccess(String region) {
        runner.setProperty(PutCloudWatchMetric.VALUE, "6");
        runner.setProperty(PutCloudWatchMetric.REGION, region);
        runner.assertValid();

        runner.enqueue(new byte[] {});
        runner.run();

        assertEquals(1, mockPutCloudWatchMetric.putMetricDataCallCount);
        runner.assertAllFlowFilesTransferred(PutCloudWatchMetric.REL_SUCCESS, 1);
    }
}