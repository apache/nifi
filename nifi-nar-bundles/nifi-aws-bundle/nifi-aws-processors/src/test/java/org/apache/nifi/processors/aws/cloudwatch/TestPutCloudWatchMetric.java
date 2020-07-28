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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.InvalidParameterValueException;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import org.junit.Test;
import org.junit.Assert;

/**
 * Unit tests for {@link PutCloudWatchMetric}.
 */
public class TestPutCloudWatchMetric {

    @Test
    public void testPutSimpleMetric() throws Exception {
        MockPutCloudWatchMetric mockPutCloudWatchMetric = new MockPutCloudWatchMetric();
        final TestRunner runner = TestRunners.newTestRunner(mockPutCloudWatchMetric);

        runner.setProperty(PutCloudWatchMetric.NAMESPACE, "TestNamespace");
        runner.setProperty(PutCloudWatchMetric.METRIC_NAME, "TestMetric");
        runner.setProperty(PutCloudWatchMetric.VALUE, "1.0");
        runner.setProperty(PutCloudWatchMetric.UNIT, "Count");
        runner.setProperty(PutCloudWatchMetric.TIMESTAMP, "1476296132575");
        runner.assertValid();

        runner.enqueue(new byte[] {});
        runner.run();

        runner.assertAllFlowFilesTransferred(PutCloudWatchMetric.REL_SUCCESS, 1);
        Assert.assertEquals(1, mockPutCloudWatchMetric.putMetricDataCallCount);
        Assert.assertEquals("TestNamespace", mockPutCloudWatchMetric.actualNamespace);
        MetricDatum datum = mockPutCloudWatchMetric.actualMetricData.get(0);
        Assert.assertEquals("TestMetric", datum.getMetricName());
        Assert.assertEquals(1d, datum.getValue(), 0.0001d);
    }

    @Test
    public void testValueLiteralDoubleInvalid() throws Exception {
        MockPutCloudWatchMetric mockPutCloudWatchMetric = new MockPutCloudWatchMetric();
        final TestRunner runner = TestRunners.newTestRunner(mockPutCloudWatchMetric);

        runner.setProperty(PutCloudWatchMetric.NAMESPACE, "TestNamespace");
        runner.setProperty(PutCloudWatchMetric.METRIC_NAME, "TestMetric");
        runner.setProperty(PutCloudWatchMetric.VALUE, "nan");
        runner.assertNotValid();
    }

    @Test
    public void testMissingBothValueAndStatisticSetInvalid() throws Exception {
        MockPutCloudWatchMetric mockPutCloudWatchMetric = new MockPutCloudWatchMetric();
        final TestRunner runner = TestRunners.newTestRunner(mockPutCloudWatchMetric);

        runner.setProperty(PutCloudWatchMetric.NAMESPACE, "TestNamespace");
        runner.setProperty(PutCloudWatchMetric.METRIC_NAME, "TestMetric");
        runner.assertNotValid();
    }

    @Test
    public void testContainsBothValueAndStatisticSetInvalid() throws Exception {
        MockPutCloudWatchMetric mockPutCloudWatchMetric = new MockPutCloudWatchMetric();
        final TestRunner runner = TestRunners.newTestRunner(mockPutCloudWatchMetric);

        runner.setProperty(PutCloudWatchMetric.NAMESPACE, "TestNamespace");
        runner.setProperty(PutCloudWatchMetric.METRIC_NAME, "TestMetric");
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
    public void testContainsIncompleteStatisticSetInvalid() throws Exception {
        MockPutCloudWatchMetric mockPutCloudWatchMetric = new MockPutCloudWatchMetric();
        final TestRunner runner = TestRunners.newTestRunner(mockPutCloudWatchMetric);

        runner.setProperty(PutCloudWatchMetric.NAMESPACE, "TestNamespace");
        runner.setProperty(PutCloudWatchMetric.METRIC_NAME, "TestMetric");
        runner.setProperty(PutCloudWatchMetric.UNIT, "Count");
        runner.setProperty(PutCloudWatchMetric.TIMESTAMP, "1476296132575");
        runner.setProperty(PutCloudWatchMetric.MINIMUM, "1.0");
        runner.setProperty(PutCloudWatchMetric.MAXIMUM, "2.0");
        runner.setProperty(PutCloudWatchMetric.SUM, "3.0");
        // missing sample count
        runner.assertNotValid();
    }

    @Test
    public void testContainsBothValueAndIncompleteStatisticSetInvalid() throws Exception {
        MockPutCloudWatchMetric mockPutCloudWatchMetric = new MockPutCloudWatchMetric();
        final TestRunner runner = TestRunners.newTestRunner(mockPutCloudWatchMetric);

        runner.setProperty(PutCloudWatchMetric.NAMESPACE, "TestNamespace");
        runner.setProperty(PutCloudWatchMetric.METRIC_NAME, "TestMetric");
        runner.setProperty(PutCloudWatchMetric.VALUE, "1.0");
        runner.setProperty(PutCloudWatchMetric.UNIT, "Count");
        runner.setProperty(PutCloudWatchMetric.TIMESTAMP, "1476296132575");
        runner.setProperty(PutCloudWatchMetric.MINIMUM, "1.0");
        runner.assertNotValid();
    }

    @Test
    public void testMetricExpressionValid() throws Exception {
        MockPutCloudWatchMetric mockPutCloudWatchMetric = new MockPutCloudWatchMetric();
        final TestRunner runner = TestRunners.newTestRunner(mockPutCloudWatchMetric);

        runner.setProperty(PutCloudWatchMetric.NAMESPACE, "TestNamespace");
        runner.setProperty(PutCloudWatchMetric.METRIC_NAME, "TestMetric");
        runner.setProperty(PutCloudWatchMetric.VALUE, "${metric.value}");
        runner.assertValid();

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("metric.value", "1.23");
        runner.enqueue(new byte[] {}, attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred(PutCloudWatchMetric.REL_SUCCESS, 1);
        Assert.assertEquals(1, mockPutCloudWatchMetric.putMetricDataCallCount);
        Assert.assertEquals("TestNamespace", mockPutCloudWatchMetric.actualNamespace);
        MetricDatum datum = mockPutCloudWatchMetric.actualMetricData.get(0);
        Assert.assertEquals("TestMetric", datum.getMetricName());
        Assert.assertEquals(1.23d, datum.getValue(), 0.0001d);
    }

    @Test
    public void testStatisticSet() throws Exception {
        MockPutCloudWatchMetric mockPutCloudWatchMetric = new MockPutCloudWatchMetric();
        final TestRunner runner = TestRunners.newTestRunner(mockPutCloudWatchMetric);

        runner.setProperty(PutCloudWatchMetric.NAMESPACE, "TestNamespace");
        runner.setProperty(PutCloudWatchMetric.METRIC_NAME, "TestMetric");
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
        Assert.assertEquals(1, mockPutCloudWatchMetric.putMetricDataCallCount);
        Assert.assertEquals("TestNamespace", mockPutCloudWatchMetric.actualNamespace);
        MetricDatum datum = mockPutCloudWatchMetric.actualMetricData.get(0);
        Assert.assertEquals("TestMetric", datum.getMetricName());
        Assert.assertEquals(1.0d, datum.getStatisticValues().getMinimum(), 0.0001d);
        Assert.assertEquals(2.0d, datum.getStatisticValues().getMaximum(), 0.0001d);
        Assert.assertEquals(3.0d, datum.getStatisticValues().getSum(), 0.0001d);
        Assert.assertEquals(2.0d, datum.getStatisticValues().getSampleCount(), 0.0001d);
    }

    @Test
    public void testDimensions() throws Exception {
        MockPutCloudWatchMetric mockPutCloudWatchMetric = new MockPutCloudWatchMetric();
        final TestRunner runner = TestRunners.newTestRunner(mockPutCloudWatchMetric);

        runner.setProperty(PutCloudWatchMetric.NAMESPACE, "TestNamespace");
        runner.setProperty(PutCloudWatchMetric.METRIC_NAME, "TestMetric");
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
        Assert.assertEquals(1, mockPutCloudWatchMetric.putMetricDataCallCount);
        Assert.assertEquals("TestNamespace", mockPutCloudWatchMetric.actualNamespace);
        MetricDatum datum = mockPutCloudWatchMetric.actualMetricData.get(0);
        Assert.assertEquals("TestMetric", datum.getMetricName());
        Assert.assertEquals(1d, datum.getValue(), 0.0001d);

        List<Dimension> dimensions = datum.getDimensions();
        Collections.sort(dimensions, (d1, d2) -> d1.getName().compareTo(d2.getName()));
        Assert.assertEquals(2, dimensions.size());
        Assert.assertEquals("dim1", dimensions.get(0).getName());
        Assert.assertEquals("1", dimensions.get(0).getValue());
        Assert.assertEquals("dim2", dimensions.get(1).getName());
        Assert.assertEquals("val2", dimensions.get(1).getValue());
    }

    @Test
    public void testMaximumDimensions() throws Exception {
        MockPutCloudWatchMetric mockPutCloudWatchMetric = new MockPutCloudWatchMetric();
        final TestRunner runner = TestRunners.newTestRunner(mockPutCloudWatchMetric);

        runner.setProperty(PutCloudWatchMetric.NAMESPACE, "TestNamespace");
        runner.setProperty(PutCloudWatchMetric.METRIC_NAME, "TestMetric");
        runner.setProperty(PutCloudWatchMetric.VALUE, "1.0");
        runner.setProperty(PutCloudWatchMetric.UNIT, "Count");
        runner.setProperty(PutCloudWatchMetric.TIMESTAMP, "1476296132575");
        for (int i=0; i < 10; i++) {
            runner.setProperty(new PropertyDescriptor.Builder().dynamic(true).name("dim" + i).build(), "0");
        }
        runner.assertValid();
    }

    @Test
    public void testTooManyDimensions() throws Exception {
        MockPutCloudWatchMetric mockPutCloudWatchMetric = new MockPutCloudWatchMetric();
        final TestRunner runner = TestRunners.newTestRunner(mockPutCloudWatchMetric);

        runner.setProperty(PutCloudWatchMetric.NAMESPACE, "TestNamespace");
        runner.setProperty(PutCloudWatchMetric.METRIC_NAME, "TestMetric");
        runner.setProperty(PutCloudWatchMetric.VALUE, "1.0");
        runner.setProperty(PutCloudWatchMetric.UNIT, "Count");
        runner.setProperty(PutCloudWatchMetric.TIMESTAMP, "1476296132575");
        for (int i=0; i < 11; i++) {
            runner.setProperty(new PropertyDescriptor.Builder().dynamic(true).name("dim" + i).build(), "0");
        }
        runner.assertNotValid();
    }

    @Test
    public void testMetricExpressionInvalidRoutesToFailure() throws Exception {
        MockPutCloudWatchMetric mockPutCloudWatchMetric = new MockPutCloudWatchMetric();
        final TestRunner runner = TestRunners.newTestRunner(mockPutCloudWatchMetric);

        runner.setProperty(PutCloudWatchMetric.NAMESPACE, "TestNamespace");
        runner.setProperty(PutCloudWatchMetric.METRIC_NAME, "TestMetric");
        runner.setProperty(PutCloudWatchMetric.VALUE, "${metric.value}");
        runner.assertValid();

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("metric.value", "nan");
        runner.enqueue(new byte[] {}, attributes);
        runner.run();

        Assert.assertEquals(0, mockPutCloudWatchMetric.putMetricDataCallCount);
        runner.assertAllFlowFilesTransferred(PutCloudWatchMetric.REL_FAILURE, 1);
    }

    @Test
    public void testInvalidUnitRoutesToFailure() throws Exception {
        MockPutCloudWatchMetric mockPutCloudWatchMetric = new MockPutCloudWatchMetric();
        mockPutCloudWatchMetric.throwException = new InvalidParameterValueException("Unit error message");
        final TestRunner runner = TestRunners.newTestRunner(mockPutCloudWatchMetric);

        runner.setProperty(PutCloudWatchMetric.NAMESPACE, "TestNamespace");
        runner.setProperty(PutCloudWatchMetric.METRIC_NAME, "TestMetric");
        runner.setProperty(PutCloudWatchMetric.UNIT, "BogusUnit");
        runner.setProperty(PutCloudWatchMetric.VALUE, "1");
        runner.assertValid();

        runner.enqueue(new byte[] {});
        runner.run();

        Assert.assertEquals(1, mockPutCloudWatchMetric.putMetricDataCallCount);
        runner.assertAllFlowFilesTransferred(PutCloudWatchMetric.REL_FAILURE, 1);
    }

    @Test
    public void testTimestampExpressionInvalidRoutesToFailure() throws Exception {
        MockPutCloudWatchMetric mockPutCloudWatchMetric = new MockPutCloudWatchMetric();
        final TestRunner runner = TestRunners.newTestRunner(mockPutCloudWatchMetric);

        runner.setProperty(PutCloudWatchMetric.NAMESPACE, "TestNamespace");
        runner.setProperty(PutCloudWatchMetric.METRIC_NAME, "TestMetric");
        runner.setProperty(PutCloudWatchMetric.UNIT, "Count");
        runner.setProperty(PutCloudWatchMetric.VALUE, "1");
        runner.setProperty(PutCloudWatchMetric.TIMESTAMP, "${timestamp.value}");
        runner.assertValid();

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("timestamp.value", "1476296132575broken");
        runner.enqueue(new byte[] {}, attributes);
        runner.run();

        Assert.assertEquals(0, mockPutCloudWatchMetric.putMetricDataCallCount);
        runner.assertAllFlowFilesTransferred(PutCloudWatchMetric.REL_FAILURE, 1);
    }

}