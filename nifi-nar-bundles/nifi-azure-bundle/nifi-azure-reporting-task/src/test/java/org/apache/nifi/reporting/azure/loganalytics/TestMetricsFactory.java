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

package org.apache.nifi.reporting.azure.loganalytics;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;
import org.apache.nifi.metrics.jvm.JmxJvmMetrics;
import org.apache.nifi.metrics.jvm.JvmMetrics;

import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.reporting.azure.loganalytics.api.AzureLogAnalyticsMetricsFactory;
import org.junit.Before;
import org.junit.Test;

public class TestMetricsFactory {

    private ProcessGroupStatus status;
    private final Gson gson = new Gson();

    @Before
    public void init() {
        status = new ProcessGroupStatus();
        status.setId("1234");
        status.setFlowFilesReceived(5);
        status.setBytesReceived(10000);
        status.setFlowFilesSent(10);
        status.setBytesSent(20000);
        status.setQueuedCount(100);
        status.setQueuedContentSize(1024L);
        status.setBytesRead(60000L);
        status.setBytesWritten(80000L);
        status.setActiveThreadCount(5);
    }

    @Test
    public void testGetDataFlowMetrics() {
        ProcessorStatus procStatus = new ProcessorStatus();
        List<ProcessorStatus> processorStatuses = new ArrayList<>();
        processorStatuses.add(procStatus);
        status.setProcessorStatus(processorStatuses);

        List<Metric> metrics = AzureLogAnalyticsMetricsFactory.getDataFlowMetrics(status, "testcase");
        TestVerification.assertDatatFlowMetrics(metrics);
    }

    @Test
    public void testGetVirtualMachineMetrics() {
        JvmMetrics virtualMachineMetrics = JmxJvmMetrics.getInstance();
        List<Metric> metrics = AzureLogAnalyticsMetricsFactory.getJvmMetrics(virtualMachineMetrics, "testcase", "tests");
        String metricsInString = gson.toJson(metrics);
        System.out.println(metricsInString);
        TestVerification.assertJVMMetrics(metrics);
    }

    @Test
    public void  testToJsonWithLongValue() {
        Metric metric = new Metric("instanceId", "groupId", "groupName");
        metric.setCount(0x7ff8000000000000L);
        gson.toJson(metric);
    }
}