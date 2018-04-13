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
package org.apache.nifi.reporting.ambari.metrics;

import com.yammer.metrics.core.VirtualMachineMetrics;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.reporting.util.metrics.MetricNames;
import org.apache.nifi.reporting.util.metrics.MetricsService;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

public class TestMetricsService {

    @Test
    public void testGetProcessGroupStatusMetrics() {
        ProcessGroupStatus status = new ProcessGroupStatus();
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

        // create a processor status with processing time
        ProcessorStatus procStatus = new ProcessorStatus();
        procStatus.setProcessingNanos(123456789);

        Collection<ProcessorStatus> processorStatuses = new ArrayList<>();
        processorStatuses.add(procStatus);
        status.setProcessorStatus(processorStatuses);

        // create a group status with processing time
        ProcessGroupStatus groupStatus = new ProcessGroupStatus();
        groupStatus.setProcessorStatus(processorStatuses);

        Collection<ProcessGroupStatus> groupStatuses = new ArrayList<>();
        groupStatuses.add(groupStatus);
        status.setProcessGroupStatus(groupStatuses);

        final MetricsService service = new MetricsService();

        final Map<String,String> metrics = service.getMetrics(status, false);

        Assert.assertTrue(metrics.containsKey(MetricNames.FLOW_FILES_RECEIVED));
        Assert.assertTrue(metrics.containsKey(MetricNames.BYTES_RECEIVED));
        Assert.assertTrue(metrics.containsKey(MetricNames.FLOW_FILES_SENT));
        Assert.assertTrue(metrics.containsKey(MetricNames.BYTES_SENT));
        Assert.assertTrue(metrics.containsKey(MetricNames.FLOW_FILES_QUEUED));
        Assert.assertTrue(metrics.containsKey(MetricNames.BYTES_QUEUED));
        Assert.assertTrue(metrics.containsKey(MetricNames.BYTES_READ));
        Assert.assertTrue(metrics.containsKey(MetricNames.BYTES_WRITTEN));
        Assert.assertTrue(metrics.containsKey(MetricNames.ACTIVE_THREADS));
        Assert.assertTrue(metrics.containsKey(MetricNames.TOTAL_TASK_DURATION_SECONDS));
        Assert.assertTrue(metrics.containsKey(MetricNames.TOTAL_TASK_DURATION_NANOS));
    }

    @Test
    public void testGetProcessGroupStatusMetricsWithID() {
        ProcessGroupStatus status = new ProcessGroupStatus();
        String id = "1234";
        status.setId(id);
        status.setFlowFilesReceived(5);
        status.setBytesReceived(10000);
        status.setFlowFilesSent(10);
        status.setBytesSent(20000);
        status.setQueuedCount(100);
        status.setQueuedContentSize(1024L);
        status.setBytesRead(60000L);
        status.setBytesWritten(80000L);
        status.setActiveThreadCount(5);

        // create a processor status with processing time
        ProcessorStatus procStatus = new ProcessorStatus();
        procStatus.setProcessingNanos(123456789);

        Collection<ProcessorStatus> processorStatuses = new ArrayList<>();
        processorStatuses.add(procStatus);
        status.setProcessorStatus(processorStatuses);

        // create a group status with processing time
        ProcessGroupStatus groupStatus = new ProcessGroupStatus();
        groupStatus.setProcessorStatus(processorStatuses);

        Collection<ProcessGroupStatus> groupStatuses = new ArrayList<>();
        groupStatuses.add(groupStatus);
        status.setProcessGroupStatus(groupStatuses);

        final MetricsService service = new MetricsService();

        final Map<String,String> metrics = service.getMetrics(status, true);

        Assert.assertTrue(metrics.containsKey(MetricNames.FLOW_FILES_RECEIVED + MetricNames.METRIC_NAME_SEPARATOR + id));
    }

    @Test
    public void testGetVirtualMachineMetrics() {
        final VirtualMachineMetrics virtualMachineMetrics = VirtualMachineMetrics.getInstance();
        final MetricsService service = new MetricsService();

        final Map<String,String> metrics = service.getMetrics(virtualMachineMetrics);
        Assert.assertTrue(metrics.containsKey(MetricNames.JVM_UPTIME));
        Assert.assertTrue(metrics.containsKey(MetricNames.JVM_HEAP_USED));
        Assert.assertTrue(metrics.containsKey(MetricNames.JVM_HEAP_USAGE));
        Assert.assertTrue(metrics.containsKey(MetricNames.JVM_NON_HEAP_USAGE));
        Assert.assertTrue(metrics.containsKey(MetricNames.JVM_THREAD_STATES_RUNNABLE));
        Assert.assertTrue(metrics.containsKey(MetricNames.JVM_THREAD_STATES_BLOCKED));
        Assert.assertTrue(metrics.containsKey(MetricNames.JVM_THREAD_STATES_TIMED_WAITING));
        Assert.assertTrue(metrics.containsKey(MetricNames.JVM_THREAD_STATES_TERMINATED));
        Assert.assertTrue(metrics.containsKey(MetricNames.JVM_THREAD_COUNT));
        Assert.assertTrue(metrics.containsKey(MetricNames.JVM_DAEMON_THREAD_COUNT));
        Assert.assertTrue(metrics.containsKey(MetricNames.JVM_FILE_DESCRIPTOR_USAGE));
    }

}
