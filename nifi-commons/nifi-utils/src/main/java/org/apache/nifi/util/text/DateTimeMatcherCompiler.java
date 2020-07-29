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

import java.util.ArrayList;
import java.util.List;

class DateTimeMatcherCompiler {

    DateTimeMatcher compile(final String format) {
        final RegexDateTimeMatcher regexMatcher = new RegexDateTimeMatcher.Compiler().compile(format);

        final List<DateTimeMatcher> matchers = new ArrayList<>(4);

        // Add a matcher that will filter out any input if it's too short or too long to match the regex.
        // This allows us to save on the expense of evaluating the Regular Expression in some cases.
        final int minLength = regexMatcher.getMinInputLength();
        final int maxLength = regexMatcher.getMaxInputLength();
        matchers.add(input -> input.length() >= minLength && input.length() <= maxLength);

        // Look for common patterns in date/time formats that allow us to quickly determine if some input text
        // will match the pattern. For example, a common date pattern is yyyy/MM/dd or MM/dd/yyyy. In the first
        // case, we know that it is not going to match unless the first 4 characters of the input are digits.
        // In the later case, we know that it will not match if the first 2 characters are not digits.
        if (format.startsWith("yyyy")) {
            matchers.add(new StartsWithDigitsDateTimeMatcher(4));
        } else if (format.startsWith("yy") || format.startsWith("mm")) {
            matchers.add(new StartsWithDigitsDateTimeMatcher(2));
        } else if (format.startsWith("H") || format.startsWith("h")) {
            matchers.add(new StartsWithDigitsDateTimeMatcher(1));
        } else if (format.startsWith("M") && !format.startsWith("MMM")) {
            // If format begins with M, it could be a number of a month name. So we have to check if it starts with at least 3 M's to determine if the month is a number of a name.
            matchers.add(new StartsWithDigitsDateTimeMatcher(1));
        }

        matchers.add(regexMatcher);

        // Use the SimpleDateFormatMatcher only if our regex matches. This allows us to parse the date only to guarantee that we are correct if we say that the input text matches.
        matchers.add(new SimpleDateFormatMatcher(format));
        return new ListDateTimeMatcher(matchers);
    }
}
