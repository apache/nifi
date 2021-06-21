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
package org.apache.nifi.websocket.util;

import org.junit.Test;
import util.HeaderMapExtractor;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HeaderMapExtractorTest {

    private static final Map<String, String> FLOW_FILE_ATTRIBUTES;
    private static final Map<String, List<String>> EXPECTED_HEADER_MAP;

    static {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("header.AUTHORIZATION", "AUTH_VALUE");
        attributes.put("header.MULTI_HEADER_KEY", "FIRST, SECOND   ,THIRD");
        attributes.put("headers.dots.dots.dots.DOTS_KEY", "DOTS_VALUE");
        attributes.put("something.else.SOMETHING_ELSE_KEY", "SOMETHING_ELSE_VALUE");
        attributes.put("header.EMPTY_VALUE_KEY", "");
        FLOW_FILE_ATTRIBUTES = Collections.unmodifiableMap(attributes);
    }

    static {
        final Map<String, List<String>> headers = new HashMap<>();
        headers.put("AUTHORIZATION", Collections.singletonList("AUTH_VALUE"));
        headers.put("MULTI_HEADER_KEY", Arrays.asList("FIRST", "SECOND", "THIRD"));
        headers.put("dots.dots.dots.DOTS_KEY", Collections.singletonList("DOTS_VALUE"));
        EXPECTED_HEADER_MAP = Collections.unmodifiableMap(headers);
    }

    @Test
    public void testMapExtractor() {
        final Map<String, List<String>> headerMap = HeaderMapExtractor.getHeaderMap(FLOW_FILE_ATTRIBUTES);

        assertEquals(EXPECTED_HEADER_MAP.size(), headerMap.size());
        for (Map.Entry<String, List<String>> entry : headerMap.entrySet()) {
            assertTrue(EXPECTED_HEADER_MAP.containsKey(entry.getKey()));
            final List<String> actualHeaderValues = entry.getValue();
            final List<String> expectedHeaderValues = EXPECTED_HEADER_MAP.get(entry.getKey());
            for (int i = 0; i < actualHeaderValues.size(); i++) {
                assertEquals(expectedHeaderValues.get(i), actualHeaderValues.get(i));
            }
        }
    }

}
