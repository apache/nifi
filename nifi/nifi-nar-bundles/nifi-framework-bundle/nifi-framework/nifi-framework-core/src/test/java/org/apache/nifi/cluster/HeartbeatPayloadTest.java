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
package org.apache.nifi.cluster;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.nifi.controller.Counter;
import org.apache.nifi.controller.StandardCounter;
import org.apache.nifi.diagnostics.SystemDiagnostics;
import org.apache.nifi.util.NiFiProperties;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author unattributed
 */
public class HeartbeatPayloadTest {

    private HeartbeatPayload payload;

    private List<Counter> counters;

    private Counter counter;

    private int activeThreadCount;

    private int totalFlowFileCount;

    private ByteArrayOutputStream marshalledBytes;

    @BeforeClass
    public static void setupSuite() {
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, HeartbeatPayloadTest.class.getResource("/conf/nifi.properties").getFile());
    }

    @Before
    public void setup() {

        payload = new HeartbeatPayload();

        activeThreadCount = 15;
        totalFlowFileCount = 25;

        counters = new ArrayList<>();
        String identifier = "identifier";
        String context = "context";
        String name = "name";
        counter = new StandardCounter(identifier, context, name);
        counters.add(counter);

        marshalledBytes = new ByteArrayOutputStream();
    }

    @Test
    public void testMarshallingWithNoInfo() {
        HeartbeatPayload.marshal(payload, marshalledBytes);
        HeartbeatPayload newPayload = HeartbeatPayload.unmarshal(new ByteArrayInputStream(marshalledBytes.toByteArray()));
        assertNull(newPayload.getCounters());
        assertEquals(0, newPayload.getActiveThreadCount());
        assertEquals(0, newPayload.getTotalFlowFileCount());
    }

    @Test
    public void testMarshalling() {

        payload.setActiveThreadCount(activeThreadCount);
        payload.setTotalFlowFileCount(totalFlowFileCount);
        payload.setCounters(counters);
        payload.setSystemDiagnostics(new SystemDiagnostics());

        HeartbeatPayload.marshal(payload, marshalledBytes);
        HeartbeatPayload newPayload = HeartbeatPayload.unmarshal(new ByteArrayInputStream(marshalledBytes.toByteArray()));

        List<Counter> newCounters = newPayload.getCounters();
        assertEquals(1, newCounters.size());

        Counter newCounter = newCounters.get(0);
        assertCounterEquals(counter, newCounter);

        assertEquals(activeThreadCount, newPayload.getActiveThreadCount());
        assertEquals(totalFlowFileCount, newPayload.getTotalFlowFileCount());
    }

    private void assertCounterEquals(Counter expected, Counter actual) {
        assertEquals(expected.getContext(), actual.getContext());
        assertEquals(expected.getIdentifier(), actual.getIdentifier());
        assertEquals(expected.getName(), actual.getName());
        assertEquals(expected.getValue(), actual.getValue());
    }

//    private void assertRepositoryStatusReportEntryEquals(RepositoryStatusReportEntry expected, RepositoryStatusReportEntry actual) {
//        assertEquals(expected.getConsumerId(), actual.getConsumerId());
//        assertEquals(expected.getBytesRead(), actual.getBytesRead());
//        assertEquals(expected.getBytesWritten(), actual.getBytesWritten());
//        assertEquals(expected.getContentSizeIn(), actual.getContentSizeIn());
//        assertEquals(expected.getContentSizeOut(), actual.getContentSizeOut());
//        assertEquals(expected.getFlowFilesIn(), actual.getFlowFilesIn());
//        assertEquals(expected.getFlowFilesOut(), actual.getFlowFilesOut());
//        assertEquals(expected.getInvocations(), actual.getInvocations());
//        assertEquals(expected.getProcessingNanos(), actual.getProcessingNanos());
//    }
}
