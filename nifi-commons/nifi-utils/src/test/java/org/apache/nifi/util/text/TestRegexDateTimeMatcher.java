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

import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class TestRegexDateTimeMatcher {

    @Test
    public void testCommonFormatsExpectedToPass() {
        final Map<String, String> exampleToPattern = new LinkedHashMap<>();

        // Following examples are intended to test specific functions in the regex generation.
        exampleToPattern.put("2018-12-12", "yyyy-MM-dd");
        exampleToPattern.put("2018/12/12", "yyyy/MM/dd");
        exampleToPattern.put("12/12/2018", "MM/dd/yyyy");
        exampleToPattern.put("12/12/18", "MM/dd/yy");
        exampleToPattern.put("1:40:55", "HH:mm:ss");
        exampleToPattern.put("01:0:5", "HH:mm:ss");
        exampleToPattern.put("12/12/2018 13:04:08 GMT-05:00", "MM/dd/yyyy HH:mm:ss z");
        exampleToPattern.put("12/12/2018 13:04:08 -0500", "MM/dd/yyyy HH:mm:ss Z");
        exampleToPattern.put("12/12/2018 13:04:08 EST", "MM/dd/yyyy HH:mm:ss zzzz");
        exampleToPattern.put("12/12/2018 13:04:08 -05", "MM/dd/yyyy HH:mm:ss X");
        exampleToPattern.put("0:08 PM", "K:mm a");
        exampleToPattern.put("Dec 12, 2018", "MMM dd, yyyy");
        exampleToPattern.put("12 Dec 2018", "dd MMM yyyy");
        exampleToPattern.put("12 December 2018", "dd MMM yyyy");

        // TODO: The following examples are taken from the SimpleDateFormat's JavaDoc. Ensure that this is not a licensing concern,
        // since it is not being distributed.
        exampleToPattern.put("2001.07.04 AD at 12:08:56 PDT", "yyyy.MM.dd G 'at' HH:mm:ss z");
        exampleToPattern.put("Wed, Jul 4, '01", "EEE, MMM d, ''yy");
        exampleToPattern.put("12:08 PM", "h:mm a");
        exampleToPattern.put("12 o'clock PM, Pacific Daylight Time", "hh 'o''clock' a, zzzz");
        exampleToPattern.put("0:08 PM, PDT", "K:mm a, z");
        exampleToPattern.put("02001.July.04 AD 12:08 PM", "yyyyy.MMMMM.dd GGG hh:mm aaa");
        exampleToPattern.put("Wed, 4 Jul 2001 12:08:56 -0700", "EEE, d MMM yyyy HH:mm:ss Z");
        exampleToPattern.put("010704120856-0700", "yyMMddHHmmssZ");
        exampleToPattern.put("2001-07-04T12:08:56.235-0700", "yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        exampleToPattern.put("2001-07-04T12:08:56.235-07:00", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
        exampleToPattern.put("2001-W27-3", "YYYY-'W'ww-u");

        for (final Map.Entry<String, String> entry : exampleToPattern.entrySet()) {
            final RegexDateTimeMatcher matcher = new RegexDateTimeMatcher.Compiler().compile(entry.getValue());
            final boolean matches = matcher.matches(entry.getKey());

            assertTrue("Pattern <" + entry.getValue() + "> did not match <" + entry.getKey() + ">", matches);
        }
    }
}
