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

package org.apache.nifi.minifi.toolkit.configuration.dto;

import org.apache.nifi.minifi.commons.schema.RemotePortSchema;
import org.apache.nifi.minifi.commons.schema.RemoteProcessGroupSchema;
import org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys;
import org.apache.nifi.web.api.dto.RemoteProcessGroupContentsDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupDTO;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class RemoteProcessGroupSchemaTest extends BaseSchemaTester<RemoteProcessGroupSchema, RemoteProcessGroupDTO> {
    private final RemoteInputPortSchemaTest remoteInputPortSchemaTest;
    private String testId = UUID.randomUUID().toString();
    private String testName = "testName";
    private String testUrl = "testUrl";
    private String testComment = "testComment";
    private String testTimeout = "11 s";
    private String testYieldPeriod = "22 s";
    private String transportProtocol = "HTTP";
    private String localNetworkInterface = "eth0";

    public RemoteProcessGroupSchemaTest() {
        super(new RemoteProcessGroupSchemaFunction(new RemotePortSchemaFunction()), RemoteProcessGroupSchema::new);
        remoteInputPortSchemaTest = new RemoteInputPortSchemaTest();
    }

    @Before
    public void setup() {
        remoteInputPortSchemaTest.setup();

        dto = new RemoteProcessGroupDTO();
        dto.setId(testId);
        dto.setName(testName);
        dto.setTargetUri(testUrl);

        RemoteProcessGroupContentsDTO contents = new RemoteProcessGroupContentsDTO();
        contents.setInputPorts(Arrays.asList(remoteInputPortSchemaTest.dto).stream().collect(Collectors.toSet()));
        dto.setContents(contents);

        dto.setComments(testComment);
        dto.setCommunicationsTimeout(testTimeout);
        dto.setYieldDuration(testYieldPeriod);
        dto.setTransportProtocol(transportProtocol);

        map = new HashMap<>();

        map.put(CommonPropertyKeys.ID_KEY, testId);
        map.put(CommonPropertyKeys.NAME_KEY, testName);
        map.put(RemoteProcessGroupSchema.URL_KEY, testUrl);
        map.put(CommonPropertyKeys.INPUT_PORTS_KEY, new ArrayList<>(Arrays.asList(remoteInputPortSchemaTest.map)));
        map.put(CommonPropertyKeys.COMMENT_KEY, testComment);
        map.put(RemoteProcessGroupSchema.TIMEOUT_KEY, testTimeout);
        map.put(CommonPropertyKeys.YIELD_PERIOD_KEY, testYieldPeriod);
        map.put(RemoteProcessGroupSchema.TRANSPORT_PROTOCOL_KEY, transportProtocol);
    }

    @Test
    public void testNoId() {
        dto.setId(null);
        map.remove(CommonPropertyKeys.ID_KEY);
        assertDtoAndMapConstructorAreSame(1);
    }

    @Test
    public void testNoName() {
        dto.setName(null);
        map.remove(CommonPropertyKeys.NAME_KEY);
        assertDtoAndMapConstructorAreSame(0);
    }

    @Test
    public void testNoUrl() {
        dto.setTargetUri(null);
        map.remove(RemoteProcessGroupSchema.URL_KEY);
        assertDtoAndMapConstructorAreSame(1);
    }

    @Test
    public void testNoInputPorts() {
        dto.getContents().setInputPorts(null);
        map.remove(CommonPropertyKeys.INPUT_PORTS_KEY);
        assertDtoAndMapConstructorAreSame(1);
    }

    @Test
    public void testNoComment() {
        dto.setComments(null);
        map.remove(CommonPropertyKeys.COMMENT_KEY);
        assertDtoAndMapConstructorAreSame(0);
    }

    @Test
    public void testNoTimeout() {
        dto.setCommunicationsTimeout(null);
        map.remove(RemoteProcessGroupSchema.TIMEOUT_KEY);
        assertDtoAndMapConstructorAreSame(0);
    }

    @Test
    public void testNoYield() {
        dto.setYieldDuration(null);
        map.remove(CommonPropertyKeys.YIELD_PERIOD_KEY);
        assertDtoAndMapConstructorAreSame(0);
    }

    @Test
    public void testNoTransportProtocol() {
        dto.setTransportProtocol(null);
        map.remove(RemoteProcessGroupSchema.TRANSPORT_PROTOCOL_KEY);
        assertDtoAndMapConstructorAreSame(0);
    }

    @Test
    public void testNoLocalNetworkInterface() {
        dto.setLocalNetworkInterface(null);
        map.remove(RemoteProcessGroupSchema.LOCAL_NETWORK_INTERFACE_KEY);
        assertDtoAndMapConstructorAreSame(0);
    }

    @Test
    public void testLocalNetworkInterface() {
        dto.setLocalNetworkInterface(localNetworkInterface);
        map.put(RemoteProcessGroupSchema.LOCAL_NETWORK_INTERFACE_KEY, localNetworkInterface);
        assertDtoAndMapConstructorAreSame(0);
    }

    @Override
    public void assertSchemaEquals(RemoteProcessGroupSchema one, RemoteProcessGroupSchema two) {
        assertEquals(one.getName(), two.getName());
        assertEquals(one.getUrls(), two.getUrls());

        List<RemotePortSchema> oneInputPorts = one.getInputPorts();
        List<RemotePortSchema> twoInputPorts = two.getInputPorts();
        if (oneInputPorts == null) {
            assertNull(twoInputPorts);
        } else {
            assertNotNull(twoInputPorts);
            assertEquals(oneInputPorts.size(), twoInputPorts.size());
            for (int i = 0; i < oneInputPorts.size(); i++) {
                remoteInputPortSchemaTest.assertSchemaEquals(oneInputPorts.get(i), twoInputPorts.get(i));
            }
        }

        assertEquals(one.getComment(), two.getComment());
        assertEquals(one.getTimeout(), two.getTimeout());
        assertEquals(one.getYieldPeriod(), two.getYieldPeriod());
        assertEquals(one.getLocalNetworkInterface(), two.getLocalNetworkInterface());
    }
}
