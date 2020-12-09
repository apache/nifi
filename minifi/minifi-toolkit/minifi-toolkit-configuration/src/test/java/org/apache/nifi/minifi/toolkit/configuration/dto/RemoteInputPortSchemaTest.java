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
import org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys;
import org.apache.nifi.web.api.dto.RemoteProcessGroupPortDTO;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class RemoteInputPortSchemaTest extends BaseSchemaTester<RemotePortSchema, RemoteProcessGroupPortDTO> {

    private String testId = UUID.nameUUIDFromBytes("testId".getBytes(StandardCharsets.UTF_8)).toString();
    private String testName = "testName";
    private String testComment = "testComment";
    private int testMaxConcurrentTasks = 111;
    private boolean testUseCompression = false;

    public RemoteInputPortSchemaTest() {
        super(new RemotePortSchemaFunction(), RemotePortSchema::new);
    }

    @Before
    public void setup() {
        dto = new RemoteProcessGroupPortDTO();
        dto.setId(testId);
        dto.setName(testName);
        dto.setComments(testComment);
        dto.setConcurrentlySchedulableTaskCount(testMaxConcurrentTasks);
        dto.setUseCompression(testUseCompression);

        map = new HashMap<>();
        map.put(CommonPropertyKeys.ID_KEY, testId);
        map.put(CommonPropertyKeys.NAME_KEY, testName);
        map.put(CommonPropertyKeys.COMMENT_KEY, testComment);
        map.put(CommonPropertyKeys.MAX_CONCURRENT_TASKS_KEY, testMaxConcurrentTasks);
        map.put(CommonPropertyKeys.USE_COMPRESSION_KEY, testUseCompression);
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
    public void testNoComment() {
        dto.setComments(null);
        map.remove(CommonPropertyKeys.COMMENT_KEY);
        assertDtoAndMapConstructorAreSame(0);
    }

    @Test
    public void testNoMaxConcurrentTasks() {
        dto.setConcurrentlySchedulableTaskCount(null);
        map.remove(CommonPropertyKeys.MAX_CONCURRENT_TASKS_KEY);
        assertDtoAndMapConstructorAreSame(0);
    }

    @Test
    public void testNoUseCompression() {
        dto.setUseCompression(null);
        map.remove(CommonPropertyKeys.USE_COMPRESSION_KEY);
        assertDtoAndMapConstructorAreSame(0);
    }

    @Override
    public void assertSchemaEquals(RemotePortSchema one, RemotePortSchema two) {
        assertEquals(one.getId(), two.getId());
        assertEquals(one.getName(), two.getName());
        assertEquals(one.getComment(), two.getComment());
        assertEquals(one.getMax_concurrent_tasks(), two.getMax_concurrent_tasks());
        assertEquals(one.getUseCompression(), two.getUseCompression());
    }
}
