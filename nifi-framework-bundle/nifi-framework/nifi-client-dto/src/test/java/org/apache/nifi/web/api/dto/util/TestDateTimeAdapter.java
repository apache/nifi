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

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestDateTimeAdapter {

    private static final DateTimeAdapter DEFAULT_TIME_ZONE_ADAPTER = new DateTimeAdapter();
    private static final String DATE_TIME_ADAPTER_CLASS_NAME = "org.apache.nifi.web.api.dto.util.DateTimeAdapter";
    // Month, day, hour, minute, and second values are distinct for verification.
    private static final long TEST_DATE_TIME_MILLISECONDS = 1767323096000L; // Represents 2026-01-02T03:04:56Z
    private static final Date TEST_DATE = new Date(TEST_DATE_TIME_MILLISECONDS);
    private static final DateTimeFormatter TEST_DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss z")
            .withZone(ZoneId.systemDefault());
    private static final String TEST_DATE_TIME = TEST_DATE_TIME_FORMATTER.format(TEST_DATE.toInstant());

    @Test
    public void testMarshalThenUnmarshal() throws Exception {
        assertEquals(TEST_DATE, DEFAULT_TIME_ZONE_ADAPTER.unmarshal(DEFAULT_TIME_ZONE_ADAPTER.marshal(TEST_DATE)));
    }

    @Test
    public void testUnmarshalThenMarshal() throws Exception {
        assertEquals(TEST_DATE_TIME, DEFAULT_TIME_ZONE_ADAPTER.marshal(DEFAULT_TIME_ZONE_ADAPTER.unmarshal(TEST_DATE_TIME)));
    }

    @Test
    public void testMarshalAcrossTimeZones() throws Exception {
        assertAll(
                () -> assertMarshalInTimeZone("UTC", "01/02/2026 03:04:56 UTC"),
                () -> assertMarshalInTimeZone("Asia/Shanghai", "01/02/2026 11:04:56 CST"),
                () -> assertMarshalInTimeZone("America/Chicago", "01/01/2026 21:04:56 CST"),
                () -> assertMarshalInTimeZone("Asia/Kolkata", "01/02/2026 08:34:56 IST"),
                () -> assertMarshalInTimeZone("America/St_Johns", "01/01/2026 23:34:56 NST"),
                () -> assertMarshalInTimeZone("America/New_York", "01/01/2026 22:04:56 EST"),
                () -> assertMarshalInTimeZone("Asia/Tokyo", "01/02/2026 12:04:56 JST"),
                () -> assertMarshalInTimeZone("Australia/Adelaide", "01/02/2026 13:34:56 ACDT"),
                () -> assertMarshalInTimeZone("Pacific/Auckland", "01/02/2026 16:04:56 NZDT"),
                () -> assertMarshalInTimeZone("Europe/Berlin", "01/02/2026 04:04:56 CET")
        );
    }

    @Test
    public void testUnmarshalAcrossTimeZones() throws Exception {
        assertAll(
                () -> assertUnmarshalInTimeZone("UTC", "01/02/2026 03:04:56 UTC"),
                () -> assertUnmarshalInTimeZone("Asia/Shanghai", "01/02/2026 11:04:56 CST"),
                () -> assertUnmarshalInTimeZone("America/Chicago", "01/01/2026 21:04:56 CST"),
                () -> assertUnmarshalInTimeZone("Asia/Kolkata", "01/02/2026 08:34:56 IST"),
                () -> assertUnmarshalInTimeZone("America/St_Johns", "01/01/2026 23:34:56 NST"),
                () -> assertUnmarshalInTimeZone("America/New_York", "01/01/2026 22:04:56 EST"),
                () -> assertUnmarshalInTimeZone("Asia/Tokyo", "01/02/2026 12:04:56 JST"),
                () -> assertUnmarshalInTimeZone("Australia/Adelaide", "01/02/2026 13:34:56 ACDT"),
                () -> assertUnmarshalInTimeZone("Pacific/Auckland", "01/02/2026 16:04:56 NZDT"),
                () -> assertUnmarshalInTimeZone("Europe/Berlin", "01/02/2026 04:04:56 CET")
        );
    }

    private static void assertMarshalInTimeZone(final String timeZoneId, final String expectedDateTime) throws Exception {
        assertEquals(expectedDateTime, invokeInTimeZone(timeZoneId, "marshal", Date.class, TEST_DATE));
    }

    private static void assertUnmarshalInTimeZone(final String timeZoneId, final String dateTime) throws Exception {
        assertEquals(TEST_DATE, invokeInTimeZone(timeZoneId, "unmarshal", String.class, dateTime));
    }

    @SuppressWarnings("unchecked")
    private static <T> T invokeInTimeZone(final String timeZoneId, final String methodName, final Class<?> parameterType, final Object argument) throws Exception {
        final TimeZone originalTimeZone = TimeZone.getDefault();

        try {
            TimeZone.setDefault(TimeZone.getTimeZone(timeZoneId));

            // DateTimeAdapter initializes its static formatter with ZoneId.systemDefault() during class loading.
            // Creating a new adapter after changing the default time zone is not sufficient because the formatter
            // remains bound to the zone captured during the first initialization. Loading the class through a
            // dedicated ClassLoader forces static initialization to run again for each configured time zone.
            final Class<?> adapterClass = Class.forName(DATE_TIME_ADAPTER_CLASS_NAME, true, new DateTimeAdapterClassLoader());
            final Object adapter = adapterClass.getDeclaredConstructor().newInstance();
            // The reloaded class is isolated from the test class loader, so reflective invocation is required.
            return (T) adapterClass.getMethod(methodName, parameterType).invoke(adapter, argument);
        } finally {
            TimeZone.setDefault(originalTimeZone);
        }
    }

    private static final class DateTimeAdapterClassLoader extends ClassLoader {
        private DateTimeAdapterClassLoader() {
            super(TestDateTimeAdapter.class.getClassLoader());
        }

        @Override
        protected Class<?> loadClass(final String name, final boolean resolve) throws ClassNotFoundException {
            synchronized (getClassLoadingLock(name)) {
                if (DATE_TIME_ADAPTER_CLASS_NAME.equals(name)) {
                    Class<?> loadedClass = findLoadedClass(name);
                    if (loadedClass == null) {
                        loadedClass = findClass(name);
                    }
                    if (resolve) {
                        resolveClass(loadedClass);
                    }
                    return loadedClass;
                }
            }

            return super.loadClass(name, resolve);
        }

        @Override
        protected Class<?> findClass(final String name) throws ClassNotFoundException {
            if (!DATE_TIME_ADAPTER_CLASS_NAME.equals(name)) {
                throw new ClassNotFoundException(name);
            }

            final String resourceName = name.replace('.', '/') + ".class";
            try (InputStream inputStream = getParent().getResourceAsStream(resourceName)) {
                if (inputStream == null) {
                    throw new ClassNotFoundException(name);
                }

                final byte[] classBytes = inputStream.readAllBytes();
                return defineClass(name, classBytes, 0, classBytes.length);
            } catch (IOException e) {
                throw new ClassNotFoundException(name, e);
            }
        }
    }
}
