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
package org.apache.nifi.util.text;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestRegexDateTimeMatcher {

    @ParameterizedTest
    @MethodSource("exampleToPattern")
    public void testCommonFormatsExpectedToPass(String example, String pattern) {
        final RegexDateTimeMatcher matcher = new RegexDateTimeMatcher.Compiler().compile(pattern);
        assertTrue(matcher.matches(example), String.format("Pattern <%s> did not match <%s>", pattern, example));
    }

    private static Stream<Arguments> exampleToPattern() {
        return Stream.of(
                // Following examples are intended to test specific functions in the regex generation.
                Arguments.of("2018-12-12", "yyyy-MM-dd"),
                Arguments.of("2018/12/12", "yyyy/MM/dd"),
                Arguments.of("12/12/2018", "MM/dd/yyyy"),
                Arguments.of("12/12/18", "MM/dd/yy"),
                Arguments.of("1/1/18", "M/d/yy"),
                Arguments.of("1/10/18", "M/d/yy"),
                Arguments.of("1:40:55", "HH:mm:ss"),
                Arguments.of("01:0:5", "HH:mm:ss"),
                Arguments.of("12/12/2018 13:04:08 GMT-05:00", "MM/dd/yyyy HH:mm:ss z"),
                Arguments.of("12/12/2018 13:04:08 -0500", "MM/dd/yyyy HH:mm:ss Z"),
                Arguments.of("12/12/2018 13:04:08 EST", "MM/dd/yyyy HH:mm:ss zzzz"),
                Arguments.of("12/12/2018 13:04:08 -05", "MM/dd/yyyy HH:mm:ss X"),
                Arguments.of("0:08 PM", "K:mm a"),
                Arguments.of("Dec 12, 2018", "MMM dd, yyyy"),
                Arguments.of("12 Dec 2018", "dd MMM yyyy"),
                Arguments.of("12 December 2018", "dd MMM yyyy"),
                Arguments.of("2001.07.04 AD at 12:08:56 PDT", "yyyy.MM.dd G 'at' HH:mm:ss z"),
                Arguments.of("Wed, Jul 4, '01", "EEE, MMM d, ''yy"),
                Arguments.of("12:08 PM", "h:mm a"),
                Arguments.of("12 o'clock PM, Pacific Daylight Time", "hh 'o''clock' a, zzzz"),
                Arguments.of("0:08 PM, PDT", "K:mm a, z"),
                Arguments.of("02001.July.04 AD 12:08 PM", "yyyyy.MMMMM.dd GGG hh:mm aaa"),
                Arguments.of("Wed, 4 Jul 2001 12:08:56 -0700", "EEE, d MMM yyyy HH:mm:ss Z"),
                Arguments.of("010704120856-0700", "yyMMddHHmmssZ"),
                Arguments.of("2001-07-04T12:08:56.235-0700", "yyyy-MM-dd'T'HH:mm:ss.SSSZ"),
                Arguments.of("2001-07-04T12:08:56.235-07:00", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
                Arguments.of("2001-W27-3", "YYYY-'W'ww-u")
        );
    }
}
