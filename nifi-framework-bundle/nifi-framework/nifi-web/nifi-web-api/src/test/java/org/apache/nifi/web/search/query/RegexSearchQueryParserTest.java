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
package org.apache.nifi.web.search.query;

import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.util.StringUtils;
import org.junit.jupiter.params.ParameterizedTest;

import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RegexSearchQueryParserTest {

    private final NiFiUser user = Mockito.mock(NiFiUser.class);
    private final ProcessGroup processGroup = Mockito.mock(ProcessGroup.class);

    @ParameterizedTest
    @MethodSource("data")
    public void testParsing(String input, String expectedTerm, String[] expectedFilterNames, String[] expectedFilterValues) {
        // given
        final RegexSearchQueryParser testSubject = new RegexSearchQueryParser();

        // when
        final SearchQuery result = testSubject.parse(input, user, processGroup, processGroup);

        // then
        assertEquals(expectedTerm, result.getTerm());

        for (int i = 0; i < expectedFilterNames.length; i++) {
            assertTrue(result.hasFilter(expectedFilterNames[i]));
            assertEquals(expectedFilterValues[i], result.getFilter(expectedFilterNames[i]));
        }
    }

    private static Stream<Arguments> data() {
        return Stream.of(
                Arguments.of("", "", new String[]{}, new String[]{}),
                Arguments.of("lorem ipsum", "lorem ipsum", new String[]{}, new String[]{}),
                Arguments.of("lorem ipsum  ", "lorem ipsum  ", new String[]{}, new String[]{}),
                Arguments.of("a:b c:d lorem ipsum", "lorem ipsum", new String[]{"a", "c"}, new String[]{"b", "d"}),
                Arguments.of("a:b\tc:d\tlorem ipsum", "lorem ipsum", new String[]{"a", "c"}, new String[]{"b", "d"}),
                Arguments.of("a:b   c:d     lorem ipsum", "lorem ipsum", new String[]{"a", "c"}, new String[]{"b", "d"}),
                Arguments.of("1a:1b c2:d2 lorem ipsum", "lorem ipsum", new String[]{"1a", "c2"}, new String[]{"1b", "d2"}),
                Arguments.of("1:2 3:4 lorem ipsum", "lorem ipsum", new String[]{"1", "3"}, new String[]{"2", "4"}),
                Arguments.of("a:b lorem c:d ipsum", "lorem c:d ipsum", new String[]{"a"}, new String[]{"b"}),
                Arguments.of("a:b lorem ipsum c:d", "lorem ipsum c:d", new String[]{"a"}, new String[]{"b"}),
                Arguments.of("a:b lorem ipsum c:d ", "lorem ipsum c:d ", new String[]{"a"}, new String[]{"b"}),
                Arguments.of("lorem ipsum a:b", "lorem ipsum a:b", new String[]{}, new String[]{}),
                Arguments.of("a:b c:d", StringUtils.EMPTY, new String[]{"a", "c"}, new String[]{"b", "d"}),
                Arguments.of("a:b c:d     ", StringUtils.EMPTY, new String[]{"a", "c"}, new String[]{"b", "d"}),
                Arguments.of("a: lorem ipsum", "a: lorem ipsum", new String[]{}, new String[]{}),
                Arguments.of(":b lorem ipsum", ":b lorem ipsum", new String[]{}, new String[]{}),
                Arguments.of(":b lorem ipsum", ":b lorem ipsum", new String[]{}, new String[]{}),
                Arguments.of("a:b a:b lorem ipsum", "lorem ipsum", new String[]{"a"}, new String[]{"b"}),
                Arguments.of("a:b a:c lorem ipsum", "lorem ipsum", new String[]{"a"}, new String[]{"b"}),
                Arguments.of("a:b-c", StringUtils.EMPTY, new String[]{"a"}, new String[]{"b-c"}),
                Arguments.of("a:b-c lorem ipsum", "lorem ipsum", new String[]{"a"}, new String[]{"b-c"}),
                Arguments.of("a:b-c d:e lorem ipsum", "lorem ipsum", new String[]{"a", "d"}, new String[]{"b-c", "e"})
        );
    }
}