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

import java.util.Map;
import java.util.HashMap;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

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