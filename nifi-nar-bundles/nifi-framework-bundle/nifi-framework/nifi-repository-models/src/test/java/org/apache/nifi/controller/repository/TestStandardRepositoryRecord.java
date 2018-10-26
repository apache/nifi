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
package org.apache.nifi.controller.repository;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class TestStandardRepositoryRecord {

    @Test
    public void testUpdatedAttributesMaintainedWhenFlowFileRemoved() {
        final StandardRepositoryRecord record = new StandardRepositoryRecord(null);

        final Map<String, String> updatedAttributes = new HashMap<>();
        updatedAttributes.put("abc", "xyz");
        updatedAttributes.put("hello", "123");

        final String uuid = UUID.randomUUID().toString();
        final FlowFileRecord flowFileRecord = new StandardFlowFileRecord.Builder()
            .addAttribute("uuid", uuid)
            .addAttributes(updatedAttributes)
            .build();

        record.setWorking(flowFileRecord, updatedAttributes);

        final Map<String, String> updatedWithId = new HashMap<>(updatedAttributes);
        updatedWithId.put("uuid", uuid);

        assertEquals(updatedWithId, record.getUpdatedAttributes());

        record.markForDelete();

        assertEquals(updatedWithId, record.getUpdatedAttributes());
    }
}
