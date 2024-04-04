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

import com.amazonaws.AmazonClientException;
import org.apache.nifi.processor.ProcessContext;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataResponse;

import java.util.List;


/**
 * Simple mock {@link PutCloudWatchMetric} processor for testing.
 */
public class MockPutCloudWatchMetric extends PutCloudWatchMetric {

    protected String actualNamespace;
    protected List<MetricDatum> actualMetricData;
    protected AmazonClientException throwException;
    protected PutMetricDataResponse result = PutMetricDataResponse.builder().build();
    protected int putMetricDataCallCount = 0;


    protected PutMetricDataResponse putMetricData(final ProcessContext context, final PutMetricDataRequest metricDataRequest) throws AmazonClientException {
        putMetricDataCallCount++;
        actualNamespace = metricDataRequest.namespace();
        actualMetricData = metricDataRequest.metricData();

        if (throwException != null) {
            throw throwException;
        }

        return result;
    }
}
