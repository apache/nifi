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

/**
 * An implementation of the DateTimeMatcher that accepts in its constructor a List of delegate DateTimeMatchers.
 * This matcher will return <code>true</code> if and only if ALL matchers in the constructor return <code>true</code> for
 * the same input.
 */
class ListDateTimeMatcher implements DateTimeMatcher {
    private final List<DateTimeMatcher> matchers;

    public ListDateTimeMatcher(final List<DateTimeMatcher> matchers) {
        this.matchers = new ArrayList<>(matchers);
    }

    @Override
    public boolean matches(final String text) {
        for (final DateTimeMatcher matcher : matchers) {
            if (!matcher.matches(text)) {
                return false;
            }
        }

        return true;
    }
}
