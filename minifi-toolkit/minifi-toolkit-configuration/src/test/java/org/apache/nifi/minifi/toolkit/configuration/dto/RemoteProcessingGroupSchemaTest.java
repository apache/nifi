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

import org.apache.nifi.minifi.commons.schema.RemoteInputPortSchema;
import org.apache.nifi.minifi.commons.schema.RemoteProcessingGroupSchema;
import org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys;
import org.apache.nifi.web.api.dto.RemoteProcessGroupContentsDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupDTO;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class RemoteProcessingGroupSchemaTest extends BaseSchemaTester<RemoteProcessingGroupSchema, RemoteProcessGroupDTO> {
    private final RemoteInputPortSchemaTest remoteInputPortSchemaTest;
    private String testName = "testName";
    private String testUrl = "testUrl";
    private String testComment = "testComment";
    private String testTimeout = "11 s";
    private String testYieldPeriod = "22 s";

    public RemoteProcessingGroupSchemaTest() {
        super(new RemoteProcessingGroupSchemaFunction(new RemoteInputPortSchemaFunction()), RemoteProcessingGroupSchema::new);
        remoteInputPortSchemaTest = new RemoteInputPortSchemaTest();
    }

    @Before
    public void setup() {
        remoteInputPortSchemaTest.setup();

        dto = new RemoteProcessGroupDTO();
        dto.setName(testName);
        dto.setTargetUri(testUrl);

        RemoteProcessGroupContentsDTO contents = new RemoteProcessGroupContentsDTO();
        contents.setInputPorts(Arrays.asList(remoteInputPortSchemaTest.dto).stream().collect(Collectors.toSet()));
        dto.setContents(contents);

        dto.setComments(testComment);
        dto.setCommunicationsTimeout(testTimeout);
        dto.setYieldDuration(testYieldPeriod);

        map = new HashMap<>();

        map.put(CommonPropertyKeys.NAME_KEY, testName);
        map.put(RemoteProcessingGroupSchema.URL_KEY, testUrl);
        map.put(CommonPropertyKeys.INPUT_PORTS_KEY, new ArrayList<>(Arrays.asList(remoteInputPortSchemaTest.map)));
        map.put(CommonPropertyKeys.COMMENT_KEY, testComment);
        map.put(RemoteProcessingGroupSchema.TIMEOUT_KEY, testTimeout);
        map.put(CommonPropertyKeys.YIELD_PERIOD_KEY, testYieldPeriod);
    }

    @Test
    public void testNoName() {
        dto.setName(null);
        map.remove(CommonPropertyKeys.NAME_KEY);
        assertDtoAndMapConstructorAreSame(1);
    }

    @Test
    public void testNoUrl() {
        dto.setTargetUri(null);
        map.remove(RemoteProcessingGroupSchema.URL_KEY);
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
        map.remove(RemoteProcessingGroupSchema.TIMEOUT_KEY);
        assertDtoAndMapConstructorAreSame(0);
    }

    @Test
    public void testNoYield() {
        dto.setYieldDuration(null);
        map.remove(CommonPropertyKeys.YIELD_PERIOD_KEY);
        assertDtoAndMapConstructorAreSame(0);
    }

    @Override
    public void assertSchemaEquals(RemoteProcessingGroupSchema one, RemoteProcessingGroupSchema two) {
        assertEquals(one.getName(), two.getName());
        assertEquals(one.getUrl(), two.getUrl());

        List<RemoteInputPortSchema> oneInputPorts = one.getInputPorts();
        List<RemoteInputPortSchema> twoInputPorts = two.getInputPorts();
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
    }
}
