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
package org.apache.nifi.web.api.dto;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestProcessGroupDTO {

    @Test
    public void testGetInputPortCount() {
        final ProcessGroupDTO dto = new ProcessGroupDTO();
        assertEquals(null, dto.getInputPortCount());

        dto.setLocalInputPortCount(3);
        dto.setPublicInputPortCount(4);

        assertEquals(Integer.valueOf(7), dto.getInputPortCount());
        assertEquals(Integer.valueOf(3), dto.getLocalInputPortCount());
        assertEquals(Integer.valueOf(4), dto.getPublicInputPortCount());
    }

    @Test
    public void testGetOutputPortCount() {
        final ProcessGroupDTO dto = new ProcessGroupDTO();
        assertEquals(null, dto.getOutputPortCount());

        dto.setLocalOutputPortCount(2);
        dto.setPublicOutputPortCount(3);

        assertEquals(Integer.valueOf(5), dto.getOutputPortCount());
        assertEquals(Integer.valueOf(2), dto.getLocalOutputPortCount());
        assertEquals(Integer.valueOf(3), dto.getPublicOutputPortCount());
    }
}
