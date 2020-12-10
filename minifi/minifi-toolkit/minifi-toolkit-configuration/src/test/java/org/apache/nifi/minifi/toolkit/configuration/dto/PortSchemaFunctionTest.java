/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.nifi.minifi.toolkit.configuration.dto;

import org.apache.nifi.minifi.commons.schema.PortSchema;
import org.apache.nifi.web.api.dto.PortDTO;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PortSchemaFunctionTest {
    private String testId;
    private String testName;
    private String testWrapperName;
    private PortDTO portDTO;
    private PortSchemaFunction portSchemaFunction;

    @Before
    public void setup() {
        testId = UUID.nameUUIDFromBytes("testId".getBytes(StandardCharsets.UTF_8)).toString();
        testName = "testName";
        testWrapperName = "testWrapperName";
        portDTO = new PortDTO();
        portDTO.setId(testId);
        portDTO.setName(testName);
        portSchemaFunction = new PortSchemaFunction(testWrapperName);
    }

    @Test
    public void testFullMap() {
        PortSchema portSchema = portSchemaFunction.apply(portDTO);
        assertEquals(testId, portSchema.getId());
        assertEquals(testName, portSchema.getName());
        assertTrue(portSchema.isValid());
    }

    @Test
    public void testNoId() {
        portDTO.setId(null);
        PortSchema portSchema = portSchemaFunction.apply(portDTO);
        assertEquals("", portSchema.getId());
        assertEquals(testName, portSchema.getName());
        assertFalse(portSchema.isValid());
    }

    @Test
    public void testNoName() {
        portDTO.setName(null);
        PortSchema portSchema = portSchemaFunction.apply(portDTO);
        assertEquals(testId, portSchema.getId());
        assertEquals("", portSchema.getName());
        assertTrue(portSchema.isValid());
    }
}
