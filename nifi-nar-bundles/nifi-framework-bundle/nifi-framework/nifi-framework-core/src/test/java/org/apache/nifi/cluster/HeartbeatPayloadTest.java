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

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import org.apache.nifi.cluster.protocol.HeartbeatPayload;
import org.apache.nifi.util.NiFiProperties;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 */
public class HeartbeatPayloadTest {

    private HeartbeatPayload payload;
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
        marshalledBytes = new ByteArrayOutputStream();
    }

    @Test
    public void testMarshallingWithNoInfo() {
        HeartbeatPayload.marshal(payload, marshalledBytes);
        HeartbeatPayload newPayload = HeartbeatPayload.unmarshal(new ByteArrayInputStream(marshalledBytes.toByteArray()));
        assertEquals(0, newPayload.getActiveThreadCount());
        assertEquals(0, newPayload.getTotalFlowFileCount());
    }

    @Test
    public void testMarshalling() {
        payload.setActiveThreadCount(activeThreadCount);
        payload.setTotalFlowFileCount(totalFlowFileCount);

        HeartbeatPayload.marshal(payload, marshalledBytes);
        HeartbeatPayload newPayload = HeartbeatPayload.unmarshal(new ByteArrayInputStream(marshalledBytes.toByteArray()));

        assertEquals(activeThreadCount, newPayload.getActiveThreadCount());
        assertEquals(totalFlowFileCount, newPayload.getTotalFlowFileCount());
    }
}
