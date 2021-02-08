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
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class RegexSearchQueryParserTest {

    private final NiFiUser user = Mockito.mock(NiFiUser.class);
    private final ProcessGroup processGroup = Mockito.mock(ProcessGroup.class);

    @Parameterized.Parameter(0)
    public String input;

    @Parameterized.Parameter(1)
    public String expectedTerm;

    @Parameterized.Parameter(2)
    public String[] expectedFilterNames;

    @Parameterized.Parameter(3)
    public String[] expectedFilterValues;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {"", "", new String[]{}, new String[]{}},
                {"lorem ipsum", "lorem ipsum", new String[]{}, new String[]{}},
                {"lorem ipsum  ", "lorem ipsum  ", new String[]{}, new String[]{}},
                {"a:b c:d lorem ipsum", "lorem ipsum", new String[]{"a", "c"}, new String[]{"b", "d"}},
                {"a:b\tc:d\tlorem ipsum", "lorem ipsum", new String[]{"a", "c"}, new String[]{"b", "d"}},
                {"a:b   c:d     lorem ipsum", "lorem ipsum", new String[]{"a", "c"}, new String[]{"b", "d"}},
                {"1a:1b c2:d2 lorem ipsum", "lorem ipsum", new String[]{"1a", "c2"}, new String[]{"1b", "d2"}},
                {"1:2 3:4 lorem ipsum", "lorem ipsum", new String[]{"1", "3"}, new String[]{"2", "4"}},
                {"a:b lorem c:d ipsum", "lorem c:d ipsum", new String[]{"a"}, new String[]{"b"}},
                {"a:b lorem ipsum c:d", "lorem ipsum c:d", new String[]{"a"}, new String[]{"b"}},
                {"a:b lorem ipsum c:d ", "lorem ipsum c:d ", new String[]{"a"}, new String[]{"b"}},
                {"lorem ipsum a:b", "lorem ipsum a:b", new String[]{}, new String[]{}},
                {"a:b c:d", StringUtils.EMPTY, new String[]{"a", "c"}, new String[]{"b", "d"}},
                {"a:b c:d     ", StringUtils.EMPTY, new String[]{"a", "c"}, new String[]{"b", "d"}},
                {"a: lorem ipsum", "a: lorem ipsum", new String[]{}, new String[]{}},
                {":b lorem ipsum", ":b lorem ipsum", new String[]{}, new String[]{}},
                {":b lorem ipsum", ":b lorem ipsum", new String[]{}, new String[]{}},
                {"a:b a:b lorem ipsum", "lorem ipsum", new String[]{"a"}, new String[]{"b"}},
                {"a:b a:c lorem ipsum", "lorem ipsum", new String[]{"a"}, new String[]{"b"}},
                {"a:b-c", StringUtils.EMPTY, new String[]{"a"}, new String[]{"b-c"}},
                {"a:b-c lorem ipsum", "lorem ipsum", new String[]{"a"}, new String[]{"b-c"}},
                {"a:b-c d:e lorem ipsum", "lorem ipsum", new String[]{"a", "d"}, new String[]{"b-c", "e"}}

        });
    }

    @Test
    public void testParsing() {
        // given
        final RegexSearchQueryParser testSubject = new RegexSearchQueryParser();

        // when
        final SearchQuery result = testSubject.parse(input, user, processGroup, processGroup);

        // then
        Assert.assertEquals(expectedTerm, result.getTerm());

        for (int i = 0; i < expectedFilterNames.length; i++) {
            Assert.assertTrue(result.hasFilter(expectedFilterNames[i]));
            Assert.assertEquals(expectedFilterValues[i], result.getFilter(expectedFilterNames[i]));
        }
    }
}