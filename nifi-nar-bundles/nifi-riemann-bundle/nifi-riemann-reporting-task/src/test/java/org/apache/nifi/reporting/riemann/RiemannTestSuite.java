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
package org.apache.nifi.reporting.riemann;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyListOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.controller.status.RunStatus;
import org.apache.nifi.reporting.riemann.metrics.MetricsService;
import org.junit.Before;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.aphyr.riemann.Proto;
import com.aphyr.riemann.client.IPromise;
import com.aphyr.riemann.client.RiemannClient;

public class RiemannTestSuite {
    protected static String HOST = "host-name";
    protected static String SERVICE_PREFIX = "service-prefix";
    protected static String NAME = "test-nifi-riemann";
    protected static String FLOW_ID = "1337";
    protected static String PROCESSOR_ID = "ba4fc3d4-7a96-46c7-b567-b8fa0039fffa";
    protected static String PROCESSOR_GROUP_ID = "5b491a05-83f4-43a4-918e-df7ac71c1f8c";
    protected static String PROCESSOR_NAME = "Webscale Write";
    protected static String PROCESSOR_TYPE = "PutHDFS";
    protected static int FLOW_FILES_RECEIVED = 31;
    protected static int FLOW_FILES_REMOVED = 3;
    protected static long BYTES_TRANSFERRED = 129338;
    protected static long BYTES_RECEIVED = 85753;
    protected static int FLOW_FILES_SENT = 13;
    protected static long BYTES_SENT = 9381;
    protected static int QUEUED_COUNT = 33;
    protected static long QUEUED_CONTENT_SIZE = 4096;
    protected static long INPUT_CONTENT_SIZE = 4096;
    protected static long OUTPUT_CONTENT_SIZE = 46;
    protected static long INPUT_BYTES = 38858;
    protected static long OUTPUT_BYTES = 938;
    protected static long BYTES_READ = 383832;
    protected static long BYTES_WRITTEN = 9398844;
    protected static int ACTIVE_THREADS = 60;
    protected static long AVERAGE_LINEAGE_DURATION = 1345;
    protected static long PROCESSING_NANOS = 123456789;
    protected static RunStatus RUN_STATUS = RunStatus.Stopped;
    protected final MetricsService service = new MetricsService(SERVICE_PREFIX, HOST, new String[] { "nifi" });
    protected ProcessGroupStatus processGroupStatus;
    protected ProcessorStatus processorStatus;
    protected RiemannClient riemannMockClient;
    protected boolean failOnWrite = false;
    // Holds incoming events to Riemann
    protected Queue<Proto.Event> eventStream = new LinkedList<Proto.Event>();

    @Before
    public void setup() {
        processGroupStatus = new ProcessGroupStatus();
        processGroupStatus.setName(NAME);
        processGroupStatus.setId(FLOW_ID);
        processGroupStatus.setFlowFilesReceived(FLOW_FILES_RECEIVED);
        processGroupStatus.setBytesTransferred(BYTES_TRANSFERRED);
        processGroupStatus.setInputContentSize(INPUT_CONTENT_SIZE);
        processGroupStatus.setOutputContentSize(OUTPUT_CONTENT_SIZE);
        processGroupStatus.setBytesReceived(BYTES_RECEIVED);
        processGroupStatus.setFlowFilesSent(FLOW_FILES_SENT);
        processGroupStatus.setBytesSent(BYTES_SENT);
        processGroupStatus.setQueuedCount(QUEUED_COUNT);
        processGroupStatus.setQueuedContentSize(QUEUED_CONTENT_SIZE);
        processGroupStatus.setBytesRead(BYTES_READ);
        processGroupStatus.setBytesWritten(BYTES_WRITTEN);
        processGroupStatus.setActiveThreadCount(ACTIVE_THREADS);

        ProcessorStatus procStatus = new ProcessorStatus();
        procStatus.setProcessingNanos(PROCESSING_NANOS);

        Collection<ProcessorStatus> processorStatuses = new ArrayList<>();
        processorStatuses.add(procStatus);
        processGroupStatus.setProcessorStatus(processorStatuses);

        ProcessGroupStatus groupStatus = new ProcessGroupStatus();
        groupStatus.setProcessorStatus(processorStatuses);

        Collection<ProcessGroupStatus> groupStatuses = new ArrayList<>();
        groupStatuses.add(groupStatus);
        processGroupStatus.setProcessGroupStatus(groupStatuses);

        processorStatus = new ProcessorStatus();
        processorStatus.setId(PROCESSOR_ID);
        processorStatus.setGroupId(PROCESSOR_GROUP_ID);
        processorStatus.setType(PROCESSOR_TYPE);
        processorStatus.setInputCount(FLOW_FILES_RECEIVED);
        processorStatus.setInputBytes(INPUT_BYTES);
        processorStatus.setOutputBytes(OUTPUT_BYTES);
        processorStatus.setBytesRead(BYTES_READ);
        processorStatus.setBytesWritten(BYTES_WRITTEN);
        processorStatus.setBytesReceived(BYTES_RECEIVED);
        processorStatus.setBytesSent(BYTES_SENT);
        processorStatus.setOutputCount(FLOW_FILES_SENT);
        processorStatus.setActiveThreadCount(ACTIVE_THREADS);
        processorStatus.setInputBytes(INPUT_CONTENT_SIZE);
        processorStatus.setOutputBytes(OUTPUT_CONTENT_SIZE);
        processorStatus.setName(PROCESSOR_NAME);
        processorStatus.setRunStatus(RUN_STATUS);
        processorStatus.setFlowFilesRemoved(FLOW_FILES_REMOVED);
        processorStatus.setAverageLineageDuration(AVERAGE_LINEAGE_DURATION, TimeUnit.MILLISECONDS);

        riemannMockClient = mock(RiemannClient.class);
        when(riemannMockClient.sendEvents(anyListOf(Proto.Event.class))).thenAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                List<Proto.Event> events = (List<Proto.Event>) invocationOnMock.getArguments()[0];
                for (Proto.Event event : events) {
                    eventStream.add(event);
                }
                IPromise iPromise = mock(IPromise.class);
                if (!failOnWrite) {
                    when(iPromise.deref(anyInt(), any(TimeUnit.class))).thenReturn(Proto.Msg.getDefaultInstance());
                } else {
                    when(iPromise.deref(anyInt(), any(TimeUnit.class))).thenReturn(null);
                }
                return iPromise;
            }
        });
        when(riemannMockClient.isConnected()).thenReturn(true);
    }
}
