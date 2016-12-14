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
package org.apache.nifi.processors.minififlowstatus.reporting.azure;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.applicationinsights.TelemetryClient;
import org.apache.nifi.processors.minififlowstatus.minifi.MiNiFiFlowStatus;
import org.apache.nifi.processors.minififlowstatus.minifi.connection.ConnectionStats;
import org.apache.nifi.processors.minififlowstatus.minifi.connection.ConnectionStatusBean;
import org.apache.nifi.processors.minififlowstatus.minifi.instance.InstanceStats;
import org.apache.nifi.processors.minififlowstatus.minifi.instance.InstanceStatus;
import org.apache.nifi.processors.minififlowstatus.minifi.processor.ProcessorStats;
import org.apache.nifi.processors.minififlowstatus.minifi.processor.ProcessorStatusBean;
import org.apache.nifi.processors.minififlowstatus.minifi.rpg.RemoteProcessGroupStats;
import org.apache.nifi.processors.minififlowstatus.minifi.rpg.RemoteProcessGroupStatusBean;
import org.apache.nifi.processors.minififlowstatus.minifi.system.ContentRepositoryUsage;
import org.apache.nifi.processors.minififlowstatus.minifi.system.FlowfileRepositoryUsage;
import org.apache.nifi.processors.minififlowstatus.minifi.system.HeapStatus;
import org.apache.nifi.processors.minififlowstatus.minifi.system.SystemDiagnosticsStatus;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;

import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;


public class AzureApplicationInsightsProcessorTest {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final String TEST_KEY = "agp_test_key";
    private static final String MINIFI_HOSTNAME = "minifi.hostname";

    private TelemetryClient telemetryClient = new TelemetryClient();
    private TelemetryClient mockTelemetryClient = spy(telemetryClient);


    @Before
    public void init() {

        doNothing().when(mockTelemetryClient).trackMetric(anyString(), anyDouble());
    }

    @Test
    public void testConnectionStatus() throws JsonProcessingException {


        final TestRunner testRunner = TestRunners.newTestRunner(new MockAzureApplicaitonInsights() {
        });

        MiNiFiFlowStatus miNiFiFlowStatus = new MiNiFiFlowStatus();
        ConnectionStatusBean connectionStatusBean = new ConnectionStatusBean();
        connectionStatusBean.name = "testConnectionStatus";
        connectionStatusBean.connectionStats = new ConnectionStats();
        connectionStatusBean.connectionStats.inputBytes = 100L;
        connectionStatusBean.connectionStats.inputCount = 10;
        connectionStatusBean.connectionStats.outputBytes = 200L;
        connectionStatusBean.connectionStats.outputCount = 20;
        miNiFiFlowStatus.connectionStatusList = new ArrayList<>();
        miNiFiFlowStatus.connectionStatusList.add(connectionStatusBean);

        testRunner.enqueue(objectMapper.writeValueAsBytes(miNiFiFlowStatus));
        testRunner.setProperty(AzureApplicationInsights.SEND_CONNECTION_STATUS_PROP, AzureApplicationInsights.TRUE);
        testRunner.setProperty(AzureApplicationInsights.BASE_METRIC_NAME, MINIFI_HOSTNAME);
        testRunner.setProperty(AzureApplicationInsights.AZURE_INSTRUMENTATION_KEY, TEST_KEY);
        testRunner.run();

        final String baseName = MINIFI_HOSTNAME + ".connection." + connectionStatusBean.name + ".";

        verify(mockTelemetryClient).trackMetric(baseName + "inputCount", connectionStatusBean.connectionStats.inputCount);
        verify(mockTelemetryClient).trackMetric(baseName + "inputBytes", connectionStatusBean.connectionStats.inputBytes);
        verify(mockTelemetryClient).trackMetric(baseName + "outputCount", connectionStatusBean.connectionStats.outputCount);
        verify(mockTelemetryClient).trackMetric(baseName + "outputBytes", connectionStatusBean.connectionStats.outputBytes);
        verify(mockTelemetryClient, times(2)).getContext();
        verify(mockTelemetryClient).flush();
    }

    @Test
    public void testInstanceStatus() throws JsonProcessingException {

        final TestRunner testRunner = TestRunners.newTestRunner(new MockAzureApplicaitonInsights() {
        });

        MiNiFiFlowStatus miNiFiFlowStatus = new MiNiFiFlowStatus();
        InstanceStats instanceStats = new InstanceStats();
        instanceStats.bytesRead = 100L;
        instanceStats.bytesWritten = 100L;
        instanceStats.bytesSent = 100L;
        instanceStats.flowfilesSent = 1;
        instanceStats.bytesTransferred = 100L;
        instanceStats.flowfilesTransferred = 1;
        instanceStats.bytesReceived = 100L;
        instanceStats.flowfilesReceived = 1;

        miNiFiFlowStatus.instanceStatus = new InstanceStatus();
        miNiFiFlowStatus.instanceStatus.instanceStats = instanceStats;

        testRunner.enqueue(objectMapper.writeValueAsBytes(miNiFiFlowStatus));
        testRunner.setProperty(AzureApplicationInsights.SEND_INSTANCE_STATUS_PROP, AzureApplicationInsights.TRUE);
        testRunner.setProperty(AzureApplicationInsights.BASE_METRIC_NAME, MINIFI_HOSTNAME);
        testRunner.setProperty(AzureApplicationInsights.AZURE_INSTRUMENTATION_KEY, TEST_KEY);
        testRunner.run();

        final String baseName = MINIFI_HOSTNAME + ".instance.";
        verify(mockTelemetryClient).trackMetric(baseName + "bytesRead", instanceStats.bytesRead);
        verify(mockTelemetryClient).trackMetric(baseName + "bytesWritten", instanceStats.bytesWritten);
        verify(mockTelemetryClient).trackMetric(baseName + "bytesSent", instanceStats.bytesSent);
        verify(mockTelemetryClient).trackMetric(baseName + "flowfilesSent", instanceStats.flowfilesSent);
        verify(mockTelemetryClient).trackMetric(baseName + "bytesTransferred", instanceStats.bytesTransferred);
        verify(mockTelemetryClient).trackMetric(baseName + "flowfilesTransferred", instanceStats.flowfilesTransferred);
        verify(mockTelemetryClient).trackMetric(baseName + "bytesReceived", instanceStats.bytesReceived);
        verify(mockTelemetryClient).trackMetric(baseName + "flowfilesReceived", instanceStats.flowfilesReceived);
        verify(mockTelemetryClient, times(2)).getContext();
        verify(mockTelemetryClient).flush();
    }

    @Test
    public void testProcessorStatus() throws JsonProcessingException {

        final TestRunner testRunner = TestRunners.newTestRunner(new MockAzureApplicaitonInsights() {
        });

        MiNiFiFlowStatus miNiFiFlowStatus = new MiNiFiFlowStatus();
        ProcessorStatusBean processorStatusBean = new ProcessorStatusBean();
        ProcessorStats processorStats = new ProcessorStats();
        processorStats.bytesRead = 100L;
        processorStats.bytesWritten = 100L;
        processorStats.activeThreads = 1;
        processorStats.flowfilesSent = 1;
        processorStats.invocations = 1;
        processorStats.processingNanos = 100L;
        processorStats.flowfilesReceived = 1;
        processorStatusBean.name = "my_processor";
        processorStatusBean.processorStats = processorStats;

        miNiFiFlowStatus.processorStatusList = Collections.singletonList(processorStatusBean);


        testRunner.enqueue(objectMapper.writeValueAsBytes(miNiFiFlowStatus));

        testRunner.setProperty(AzureApplicationInsights.SEND_INSTANCE_STATUS_PROP, AzureApplicationInsights.TRUE);
        testRunner.setProperty(AzureApplicationInsights.BASE_METRIC_NAME, MINIFI_HOSTNAME);
        testRunner.setProperty(AzureApplicationInsights.AZURE_INSTRUMENTATION_KEY, TEST_KEY);

        testRunner.run();

        final String baseName = MINIFI_HOSTNAME + ".processor." + processorStatusBean.name.replace('/', '-') + ".";
        verify(mockTelemetryClient).trackMetric(baseName + "activeThreads", processorStats.activeThreads);
        verify(mockTelemetryClient).trackMetric(baseName + "flowfilesReceived", processorStats.flowfilesReceived);
        verify(mockTelemetryClient).trackMetric(baseName + "flowfilesSent", processorStats.flowfilesSent);
        verify(mockTelemetryClient).trackMetric(baseName + "bytesRead", processorStats.bytesRead);
        verify(mockTelemetryClient).trackMetric(baseName + "bytesWritten", processorStats.bytesWritten);
        verify(mockTelemetryClient).trackMetric(baseName + "invocations", processorStats.invocations);
        verify(mockTelemetryClient).trackMetric(baseName + "processingNanos", processorStats.processingNanos);

        verify(mockTelemetryClient, times(2)).getContext();
        verify(mockTelemetryClient).flush();
    }

    @Test
    public void testRPGStatus() throws JsonProcessingException {

        final TestRunner testRunner = TestRunners.newTestRunner(new MockAzureApplicaitonInsights() {
        });

        MiNiFiFlowStatus miNiFiFlowStatus = new MiNiFiFlowStatus();
        RemoteProcessGroupStatusBean remoteProcessGroupStatusBean = new RemoteProcessGroupStatusBean();
        RemoteProcessGroupStats remoteProcessGroupStats = new RemoteProcessGroupStats();
        remoteProcessGroupStats.sentCount = 100;
        remoteProcessGroupStats.sentContentSize = 100L;
        remoteProcessGroupStats.activeThreads = 1;

        remoteProcessGroupStatusBean.name = "http://localhost/nifi";
        remoteProcessGroupStatusBean.remoteProcessGroupStats = remoteProcessGroupStats;

        miNiFiFlowStatus.remoteProcessGroupStatusList = Collections.singletonList(remoteProcessGroupStatusBean);


        testRunner.enqueue(objectMapper.writeValueAsBytes(miNiFiFlowStatus));

        testRunner.setProperty(AzureApplicationInsights.SEND_INSTANCE_STATUS_PROP, AzureApplicationInsights.TRUE);
        testRunner.setProperty(AzureApplicationInsights.BASE_METRIC_NAME, MINIFI_HOSTNAME);
        testRunner.setProperty(AzureApplicationInsights.AZURE_INSTRUMENTATION_KEY, TEST_KEY);

        testRunner.run();


        final String baseName = MINIFI_HOSTNAME + ".rpg." + URI.create(remoteProcessGroupStatusBean.name).getHost() + ".";
        verify(mockTelemetryClient).trackMetric(baseName + "activeThreads", remoteProcessGroupStatusBean.remoteProcessGroupStats.activeThreads);
        verify(mockTelemetryClient).trackMetric(baseName + "sentCount", remoteProcessGroupStatusBean.remoteProcessGroupStats.sentCount);
        verify(mockTelemetryClient).trackMetric(baseName + "sentContentSize", remoteProcessGroupStatusBean.remoteProcessGroupStats.sentContentSize);
        verify(mockTelemetryClient, times(2)).getContext();
        verify(mockTelemetryClient).flush();
    }

    @Test
    public void testSystemStatus() throws JsonProcessingException {

        final TestRunner testRunner = TestRunners.newTestRunner(new MockAzureApplicaitonInsights() {
        });

        MiNiFiFlowStatus miNiFiFlowStatus = new MiNiFiFlowStatus();
        SystemDiagnosticsStatus systemDiagnosticsStatus = new SystemDiagnosticsStatus();
        HeapStatus heapStatus = new HeapStatus();
        heapStatus.totalHeap = 100;
        heapStatus.maxHeap = 100;
        heapStatus.freeHeap = 100;
        heapStatus.usedHeap = 100;
        heapStatus.heapUtilization = 100;
        heapStatus.totalNonHeap = 100;
        heapStatus.maxNonHeap = 100;
        heapStatus.freeNonHeap = 100;
        heapStatus.usedNonHeap = 100;
        heapStatus.nonHeapUtilization = 100;

        ContentRepositoryUsage contentRepositoryUsage = new ContentRepositoryUsage();
        contentRepositoryUsage.freeSpace = 100L;
        contentRepositoryUsage.totalSpace = 100L;
        contentRepositoryUsage.usedSpace = 100L;
        contentRepositoryUsage.diskUtilization = 100;
        contentRepositoryUsage.name = "contentRepo";

        FlowfileRepositoryUsage flowfileRepositoryUsage = new FlowfileRepositoryUsage();
        flowfileRepositoryUsage.freeSpace = 100L;
        flowfileRepositoryUsage.totalSpace = 100L;
        flowfileRepositoryUsage.usedSpace = 100L;
        flowfileRepositoryUsage.diskUtilization = 100;
        systemDiagnosticsStatus.heapStatus = heapStatus;
        systemDiagnosticsStatus.contentRepositoryUsageList = Collections.singletonList(contentRepositoryUsage);
        systemDiagnosticsStatus.flowfileRepositoryUsage = flowfileRepositoryUsage;

        miNiFiFlowStatus.systemDiagnosticsStatus = systemDiagnosticsStatus;

        testRunner.enqueue(objectMapper.writeValueAsBytes(miNiFiFlowStatus));
        testRunner.setProperty(AzureApplicationInsights.SEND_INSTANCE_STATUS_PROP, AzureApplicationInsights.TRUE);
        testRunner.setProperty(AzureApplicationInsights.BASE_METRIC_NAME, MINIFI_HOSTNAME);
        testRunner.setProperty(AzureApplicationInsights.AZURE_INSTRUMENTATION_KEY, TEST_KEY);
        testRunner.run();

        final String contentRepobaseName = MINIFI_HOSTNAME + ".system.content.repository." + contentRepositoryUsage.name + ".";

        verify(mockTelemetryClient).trackMetric(contentRepobaseName + "freeSpace", contentRepositoryUsage.freeSpace);
        verify(mockTelemetryClient).trackMetric(contentRepobaseName + "totalSpace", contentRepositoryUsage.totalSpace);
        verify(mockTelemetryClient).trackMetric(contentRepobaseName + "usedSpace", contentRepositoryUsage.usedSpace);
        verify(mockTelemetryClient).trackMetric(contentRepobaseName + "diskUtilization", contentRepositoryUsage.diskUtilization);

        final String flowRepoBaseName = MINIFI_HOSTNAME + ".system.flowfile.repository.";

        verify(mockTelemetryClient).trackMetric(flowRepoBaseName + "freeSpace", flowfileRepositoryUsage.freeSpace);
        verify(mockTelemetryClient).trackMetric(flowRepoBaseName + "totalSpace", flowfileRepositoryUsage.totalSpace);
        verify(mockTelemetryClient).trackMetric(flowRepoBaseName + "usedSpace", flowfileRepositoryUsage.usedSpace);
        verify(mockTelemetryClient).trackMetric(flowRepoBaseName + "diskUtilization", flowfileRepositoryUsage.diskUtilization);

        final String heapBaseName = MINIFI_HOSTNAME + ".system.heap.";

        telemetryClient.trackMetric(heapBaseName + "totalHeap", heapStatus.totalHeap);
        telemetryClient.trackMetric(heapBaseName + "maxHeap", heapStatus.maxHeap);
        telemetryClient.trackMetric(heapBaseName + "freeHeap", heapStatus.freeHeap);
        telemetryClient.trackMetric(heapBaseName + "usedHeap", heapStatus.usedHeap);
        telemetryClient.trackMetric(heapBaseName + "heapUtilization", heapStatus.heapUtilization);
        telemetryClient.trackMetric(heapBaseName + "totalNonHeap", heapStatus.totalNonHeap);
        telemetryClient.trackMetric(heapBaseName + "maxNonHeap", heapStatus.maxNonHeap);
        telemetryClient.trackMetric(heapBaseName + "freeNonHeap", heapStatus.freeNonHeap);
        telemetryClient.trackMetric(heapBaseName + "usedNonHeap", heapStatus.usedNonHeap);
        telemetryClient.trackMetric(heapBaseName + "nonHeapUtilization", heapStatus.nonHeapUtilization);


        verify(mockTelemetryClient, times(2)).getContext();
        verify(mockTelemetryClient).flush();
    }

    class MockAzureApplicaitonInsights extends AzureApplicationInsights {

        @Override
        protected TelemetryClient createTelemetryClient() {
            return mockTelemetryClient;
        }
    }

}
