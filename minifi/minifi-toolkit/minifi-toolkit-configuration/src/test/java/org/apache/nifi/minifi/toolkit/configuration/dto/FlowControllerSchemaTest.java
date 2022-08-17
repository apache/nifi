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

import org.apache.nifi.minifi.commons.schema.FlowControllerSchema;
import org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys;
import org.apache.nifi.web.api.dto.TemplateDTO;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FlowControllerSchemaTest extends BaseSchemaTester<FlowControllerSchema, TemplateDTO> {
    private static final String TEST_NAME = "testName";
    private static final String TEST_COMMENT = "testComment";

    public FlowControllerSchemaTest() {
        super(new FlowControllerSchemaFunction(), FlowControllerSchema::new);
    }

    @BeforeEach
    public void setup() {
        dto = new TemplateDTO();

        dto.setName(TEST_NAME);
        dto.setDescription(TEST_COMMENT);

        map = new HashMap<>();

        map.put(CommonPropertyKeys.NAME_KEY, TEST_NAME);
        map.put(CommonPropertyKeys.COMMENT_KEY, TEST_COMMENT);
    }

    @Test
    public void testNoNameSame() {
        dto.setName(null);
        map.remove(CommonPropertyKeys.NAME_KEY);
        assertDtoAndMapConstructorAreSame(1);
    }

    @Test
    public void testNoCommentSame() {
        dto.setDescription(null);
        map.remove(CommonPropertyKeys.COMMENT_KEY);
        assertDtoAndMapConstructorAreSame(0);
    }

    @Override
    public void assertSchemaEquals(FlowControllerSchema one, FlowControllerSchema two) {
        assertEquals(one.getName(), two.getName());
        assertEquals(one.getComment(), two.getComment());
    }
}
