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
package org.apache.nifi.events;

import org.apache.nifi.reporting.Bulletin;
import org.apache.nifi.reporting.ComponentType;
import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.StringWriter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BulletinFactoryStackTraceTest {

    private static String toStackTrace(final Throwable t) {
        final StringWriter sw = new StringWriter();
        try (PrintWriter pw = new PrintWriter(sw)) {
            t.printStackTrace(pw);
        }
        return sw.toString();
    }

    @Test
    void testCreateBulletinWithThrowableIncludesPrintableStackTrace() {
        final Exception cause = new IllegalStateException("inner");
        final RuntimeException ex = new RuntimeException("outer", cause);

        final Bulletin bulletin = BulletinFactory.createBulletin(
                "pg1", "Process Group 1", "proc1", ComponentType.PROCESSOR, "MyProcessor",
                "Log Message", "ERROR", "Something failed", "/root / Process Group 1", null, ex);

        assertNotNull(bulletin);
        final String stackTrace = bulletin.getStackTrace();
        assertNotNull(stackTrace, "Stack trace should be set on bulletin");

        final String expected = toStackTrace(ex);
        assertEquals(expected, stackTrace, "Stack trace should match Throwable.printStackTrace output exactly");
        assertTrue(stackTrace.contains(cause.getClass().getSimpleName()));
        assertTrue(stackTrace.contains(ex.getClass().getSimpleName()));
        assertTrue(stackTrace.contains("\n"), "Stack trace should contain newlines for multiline formatting");
        assertTrue(stackTrace.contains("\tat ") || stackTrace.contains("\n\tat "), "Stack trace should include frame lines");
    }
}

