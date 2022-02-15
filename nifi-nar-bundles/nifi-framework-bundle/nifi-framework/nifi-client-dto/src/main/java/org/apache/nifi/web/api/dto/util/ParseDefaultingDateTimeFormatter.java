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

package org.apache.nifi.web.api.dto.util;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * Some adapters need to create a Date object from a String that contains only a portion of it, such as the time.
 * This is handled by using a DateTimeFormatter with defaulted values for some fields. The defaults are derived from
 * the current date/time. Because of this, we can't just create a DateTimeFormatter once and never change it, as doing
 * so would result in the wrong date/time after midnight, when the date changes.
 * But we don't want to create a new instance of DateTimeFormatter every time, either, because it uses lazy initialization,
 * so the first call to parse() is far more expensive than subsequent calls.
 * This class allows us to easily create a DateTimeFormatter and cache it, continually reusing it, until the date changes.
 */
public class ParseDefaultingDateTimeFormatter {
    private final AtomicReference<Wrapper> wrapperReference = new AtomicReference<>();

    private final Function<LocalDateTime, String> applicabilityTransform;
    private final Function<LocalDateTime, DateTimeFormatter> formatFactory;

    /**
     * Default constructor
     * @param applicabilityTransform a transform that creates a String that can be used identify whether or not previously created DateTimeFormatter exists. This may be created, for instance,
     * by concatenating specific fields from the given LocalDateTime
     * @param formatFactory a transform that will give us a DateTimeFormatter that is applicable for the given LocalDateTime
     */
    public ParseDefaultingDateTimeFormatter(final Function<LocalDateTime, String> applicabilityTransform, final Function<LocalDateTime, DateTimeFormatter> formatFactory) {
        this.applicabilityTransform = applicabilityTransform;
        this.formatFactory = formatFactory;
    }

    public DateTimeFormatter get() {
        final LocalDateTime now = LocalDateTime.now();
        final String applicabilityValue = applicabilityTransform.apply(now);

        final Wrapper wrapper = wrapperReference.get();
        if (wrapper != null && wrapper.getApplicabilityValue().equals(applicabilityValue)) {
            return wrapper.getFormatter();
        }

        final DateTimeFormatter formatter = formatFactory.apply(now);
        final Wrapper updatedWrapper = new Wrapper(formatter, applicabilityValue);
        wrapperReference.compareAndSet(wrapper, updatedWrapper);
        return formatter;
    }

    private static class Wrapper {
        private final DateTimeFormatter formatter;
        private final String applicabilityValue;

        public Wrapper(final DateTimeFormatter formatter, final String applicabilityValue) {
            this.formatter = formatter;
            this.applicabilityValue = applicabilityValue;
        }

        public DateTimeFormatter getFormatter() {
            return formatter;
        }

        public String getApplicabilityValue() {
            return applicabilityValue;
        }
    }
}
