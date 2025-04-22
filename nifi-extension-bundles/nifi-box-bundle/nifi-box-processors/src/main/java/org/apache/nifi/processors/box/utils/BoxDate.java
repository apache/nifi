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
package org.apache.nifi.processors.box.utils;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * A wrapper class for formatting {@link LocalDate} to a string that is accepted by Box Metadata API.
 */
public final class BoxDate {

    // The time part is always set to 0. https://developer.box.com/guides/metadata/fields/date/
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T00:00:00.000Z'");

    private final LocalDate date;

    private BoxDate(final LocalDate date) {
        this.date = date;
    }

    public static BoxDate of(final LocalDate date) {
        return new BoxDate(date);
    }

    public String format() {
        return DATE_FORMATTER.format(date);
    }
}
