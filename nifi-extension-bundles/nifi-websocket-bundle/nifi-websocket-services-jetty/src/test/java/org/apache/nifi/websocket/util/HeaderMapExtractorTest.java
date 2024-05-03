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

import org.junit.jupiter.api.Test;
import org.apache.nifi.websocket.jetty.util.HeaderMapExtractor;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class HeaderMapExtractorTest {

    @Test
    public void testMapExtractor() {

        // GIVEN
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("header.AUTHORIZATION", "AUTH_VALUE");
        attributes.put("header.MULTI_HEADER_KEY", "FIRST, SECOND   ,THIRD");
        attributes.put("header.dots.dots.dots.DOTS_KEY", "DOTS_VALUE");
        attributes.put("something.else.SOMETHING_ELSE_KEY", "SOMETHING_ELSE_VALUE");
        attributes.put("header.EMPTY_VALUE_KEY", "");
        attributes.put("headerButNotReally.UNRECOGNIZED", "NOT_A_HEADER_VALUE");

        final Map<String, List<String>> expected = new HashMap<>();
        expected.put("AUTHORIZATION", Collections.singletonList("AUTH_VALUE"));
        expected.put("MULTI_HEADER_KEY", Arrays.asList("FIRST", "SECOND", "THIRD"));
        expected.put("dots.dots.dots.DOTS_KEY", Collections.singletonList("DOTS_VALUE"));

        // WHEN
        final Map<String, List<String>> actual = HeaderMapExtractor.getHeaderMap(attributes);

        // THEN
        assertEquals(expected, actual);

        assertEquals(expected.size(), actual.size());
        for (Map.Entry<String, List<String>> entry : actual.entrySet()) {
            assertTrue(expected.containsKey(entry.getKey()));
            final List<String> actualHeaderValues = entry.getValue();
            final List<String> expectedHeaderValues = expected.get(entry.getKey());
            for (int i = 0; i < actualHeaderValues.size(); i++) {
                assertEquals(expectedHeaderValues.get(i), actualHeaderValues.get(i));
            }
        }
    }

}
