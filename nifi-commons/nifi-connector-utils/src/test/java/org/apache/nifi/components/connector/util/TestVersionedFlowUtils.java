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

package org.apache.nifi.components.connector.util;

import org.apache.nifi.flow.Bundle;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class TestVersionedFlowUtils {

    @Nested
    class GetLatest {
        @Test
        void testMinorVersion() {
            final List<Bundle> bundles = List.of(
                new Bundle("group", "artifact", "1.0.0"),
                new Bundle("group", "artifact", "1.2.0")
            );

            final Bundle latestBundle = VersionedFlowUtils.getLatest(bundles);
            assertNotNull(latestBundle);
            assertEquals("1.2.0", latestBundle.getVersion());
        }

        @Test
        void testMajorVersion() {
            final List<Bundle> bundles = List.of(
                new Bundle("group", "artifact", "1.0.0"),
                new Bundle("group", "artifact", "2.0.0")
            );

            final Bundle latestBundle = VersionedFlowUtils.getLatest(bundles);
            assertNotNull(latestBundle);
            assertEquals("2.0.0", latestBundle.getVersion());
        }

        @Test
        void testMoreDigitsLater() {
            final List<Bundle> bundles = List.of(
                new Bundle("group", "artifact", "1.0"),
                new Bundle("group", "artifact", "1.0.1")
            );

            final Bundle latestBundle = VersionedFlowUtils.getLatest(bundles);
            assertNotNull(latestBundle);
            assertEquals("1.0.1", latestBundle.getVersion());
        }

        @Test
        void testMoreDigitsFirst() {
            final List<Bundle> bundles = List.of(
                new Bundle("group", "artifact", "1.0.1"),
                new Bundle("group", "artifact", "1.0")
            );

            final Bundle latestBundle = VersionedFlowUtils.getLatest(bundles);
            assertNotNull(latestBundle);
            assertEquals("1.0.1", latestBundle.getVersion());
        }

        @Test
        void testWithSnapshotAndSameVersion() {
            final List<Bundle> bundles = List.of(
                new Bundle("group", "artifact", "1.0.0-SNAPSHOT"),
                new Bundle("group", "artifact", "1.0.0")
            );

            final Bundle latestBundle = VersionedFlowUtils.getLatest(bundles);
            assertNotNull(latestBundle);
            assertEquals("1.0.0", latestBundle.getVersion());
        }

        @Test
        void testWithSnapshotAndDifferentVersion() {
            final List<Bundle> bundles = List.of(
                new Bundle("group", "artifact", "1.0.1-SNAPSHOT"),
                new Bundle("group", "artifact", "1.0.0")
            );

            final Bundle latestBundle = VersionedFlowUtils.getLatest(bundles);
            assertNotNull(latestBundle);
            assertEquals("1.0.1-SNAPSHOT", latestBundle.getVersion());
        }

        @Test
        void testEmptyList() {
            final List<Bundle> bundles = List.of();
            assertNull(VersionedFlowUtils.getLatest(bundles));
        }
    }

}
