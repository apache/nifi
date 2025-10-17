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
package org.apache.nifi.web.api.dto;

import org.apache.nifi.events.BulletinFactory;
import org.apache.nifi.reporting.Bulletin;
import org.apache.nifi.reporting.ComponentType;
import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.StringWriter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class DtoFactoryBulletinStackTraceTest {

    private static String toStackTrace(final Throwable t) {
        final StringWriter sw = new StringWriter();
        try (PrintWriter pw = new PrintWriter(sw)) {
            t.printStackTrace(pw);
        }
        return sw.toString();
    }

    @Test
    void testBulletinDtoDoesNotIncludeStackTraceByDefault() {
        final Throwable ex = new IllegalArgumentException("invalid");
        final Bulletin bulletin = BulletinFactory.createBulletin(
                "pg", "PG", "svc1", ComponentType.CONTROLLER_SERVICE, "CS",
                "Log Message", "ERROR", "boom", "/PG", null, ex);

        final DtoFactory dtoFactory = new DtoFactory();
        final BulletinDTO dto = dtoFactory.createBulletinDto(bulletin, false);

        assertNotNull(dto);
        assertEquals(null, dto.getStackTrace(), "DTO must not include stackTrace by default");
    }

    @Test
    void testBulletinDtoIncludesStackTraceWhenRequested() {
        final Throwable ex = new IllegalArgumentException("invalid");
        final Bulletin bulletin = BulletinFactory.createBulletin(
                "pg", "PG", "svc1", ComponentType.CONTROLLER_SERVICE, "CS",
                "Log Message", "ERROR", "boom", "/PG", null, ex);

        final DtoFactory dtoFactory = new DtoFactory();
        final BulletinDTO dto = dtoFactory.createBulletinDto(bulletin, true);

        assertNotNull(dto);
        assertEquals(toStackTrace(ex), dto.getStackTrace(), "DTO must carry stackTrace when requested");
    }
}
