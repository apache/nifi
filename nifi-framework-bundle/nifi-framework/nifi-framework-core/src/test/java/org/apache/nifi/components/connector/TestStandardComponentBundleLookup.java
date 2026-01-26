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

package org.apache.nifi.components.connector;

import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.bundle.BundleDetails;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.nar.ExtensionManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestStandardComponentBundleLookup {
    private static final String COMPONENT_TYPE = "org.apache.nifi.processors.TestProcessor";

    private ExtensionManager extensionManager;
    private StandardComponentBundleLookup lookup;

    @BeforeEach
    void setup() {
        extensionManager = mock(ExtensionManager.class);
        lookup = new StandardComponentBundleLookup(extensionManager);
    }

    @Nested
    class GetLatestBundle {
        @Test
        void testMinorVersion() {
            setupBundles(
                createBundle("group", "artifact", "1.0.0"),
                createBundle("group", "artifact", "1.2.0")
            );

            final Optional<Bundle> latestBundle = lookup.getLatestBundle(COMPONENT_TYPE);
            assertTrue(latestBundle.isPresent());
            assertEquals("1.2.0", latestBundle.get().getVersion());
        }

        @Test
        void testMajorVersion() {
            setupBundles(
                createBundle("group", "artifact", "1.0.0"),
                createBundle("group", "artifact", "2.0.0")
            );

            final Optional<Bundle> latestBundle = lookup.getLatestBundle(COMPONENT_TYPE);
            assertTrue(latestBundle.isPresent());
            assertEquals("2.0.0", latestBundle.get().getVersion());
        }

        @Test
        void testMoreDigitsLater() {
            setupBundles(
                createBundle("group", "artifact", "1.0"),
                createBundle("group", "artifact", "1.0.1")
            );

            final Optional<Bundle> latestBundle = lookup.getLatestBundle(COMPONENT_TYPE);
            assertTrue(latestBundle.isPresent());
            assertEquals("1.0.1", latestBundle.get().getVersion());
        }

        @Test
        void testMoreDigitsFirst() {
            setupBundles(
                createBundle("group", "artifact", "1.0.1"),
                createBundle("group", "artifact", "1.0")
            );

            final Optional<Bundle> latestBundle = lookup.getLatestBundle(COMPONENT_TYPE);
            assertTrue(latestBundle.isPresent());
            assertEquals("1.0.1", latestBundle.get().getVersion());
        }

        @Test
        void testWithSnapshotAndSameVersion() {
            setupBundles(
                createBundle("group", "artifact", "1.0.0-SNAPSHOT"),
                createBundle("group", "artifact", "1.0.0")
            );

            final Optional<Bundle> latestBundle = lookup.getLatestBundle(COMPONENT_TYPE);
            assertTrue(latestBundle.isPresent());
            assertEquals("1.0.0", latestBundle.get().getVersion());
        }

        @Test
        void testWithSnapshotAndDifferentVersion() {
            setupBundles(
                createBundle("group", "artifact", "1.0.1-SNAPSHOT"),
                createBundle("group", "artifact", "1.0.0")
            );

            final Optional<Bundle> latestBundle = lookup.getLatestBundle(COMPONENT_TYPE);
            assertTrue(latestBundle.isPresent());
            assertEquals("1.0.1-SNAPSHOT", latestBundle.get().getVersion());
        }

        @Test
        void testEmptyList() {
            setupBundles();
            assertTrue(lookup.getLatestBundle(COMPONENT_TYPE).isEmpty());
        }

        @Test
        void testNonNumericVersionPart() {
            setupBundles(
                createBundle("group", "artifact", "4.0.0"),
                createBundle("group", "artifact", "4.0.next")
            );

            final Optional<Bundle> latestBundle = lookup.getLatestBundle(COMPONENT_TYPE);
            assertTrue(latestBundle.isPresent());
            assertEquals("4.0.0", latestBundle.get().getVersion());
        }

        @Test
        void testFullyNonNumericVersionVsNumeric() {
            setupBundles(
                createBundle("group", "artifact", "1.0.0"),
                createBundle("group", "artifact", "undefined")
            );

            final Optional<Bundle> latestBundle = lookup.getLatestBundle(COMPONENT_TYPE);
            assertTrue(latestBundle.isPresent());
            assertEquals("1.0.0", latestBundle.get().getVersion());
        }

        @Test
        void testTwoFullyNonNumericVersions() {
            setupBundles(
                createBundle("group", "artifact", "undefined"),
                createBundle("group", "artifact", "unknown")
            );

            final Optional<Bundle> latestBundle = lookup.getLatestBundle(COMPONENT_TYPE);
            assertTrue(latestBundle.isPresent());
            assertEquals("unknown", latestBundle.get().getVersion());
        }

        @Test
        void testQualifierOrdering() {
            setupBundles(
                createBundle("group", "artifact", "2.0.0-SNAPSHOT"),
                createBundle("group", "artifact", "2.0.0-M1"),
                createBundle("group", "artifact", "2.0.0-M4"),
                createBundle("group", "artifact", "2.0.0-RC1"),
                createBundle("group", "artifact", "2.0.0-RC2"),
                createBundle("group", "artifact", "2.0.0")
            );

            final Optional<Bundle> latestBundle = lookup.getLatestBundle(COMPONENT_TYPE);
            assertTrue(latestBundle.isPresent());
            assertEquals("2.0.0", latestBundle.get().getVersion());
        }

        @Test
        void testQualifierOrderingWithoutRelease() {
            setupBundles(
                createBundle("group", "artifact", "2.0.0-SNAPSHOT"),
                createBundle("group", "artifact", "2.0.0-M1"),
                createBundle("group", "artifact", "2.0.0-M4"),
                createBundle("group", "artifact", "2.0.0-RC1"),
                createBundle("group", "artifact", "2.0.0-RC2")
            );

            final Optional<Bundle> latestBundle = lookup.getLatestBundle(COMPONENT_TYPE);
            assertTrue(latestBundle.isPresent());
            assertEquals("2.0.0-RC2", latestBundle.get().getVersion());
        }

        @Test
        void testMilestoneVersionsOrdering() {
            setupBundles(
                createBundle("group", "artifact", "2.0.0-M1"),
                createBundle("group", "artifact", "2.0.0-M4"),
                createBundle("group", "artifact", "2.0.0-SNAPSHOT")
            );

            final Optional<Bundle> latestBundle = lookup.getLatestBundle(COMPONENT_TYPE);
            assertTrue(latestBundle.isPresent());
            assertEquals("2.0.0-M4", latestBundle.get().getVersion());
        }

        @Test
        void testCalendarDateFormat() {
            setupBundles(
                createBundle("group", "artifact", "2025.12.31"),
                createBundle("group", "artifact", "2026.01.01")
            );

            final Optional<Bundle> latestBundle = lookup.getLatestBundle(COMPONENT_TYPE);
            assertTrue(latestBundle.isPresent());
            assertEquals("2026.01.01", latestBundle.get().getVersion());
        }

        @Test
        void testCalendarDateFormatWithBuildNumber() {
            setupBundles(
                createBundle("group", "artifact", "2025.12.31.999"),
                createBundle("group", "artifact", "2026.01.01.451")
            );

            final Optional<Bundle> latestBundle = lookup.getLatestBundle(COMPONENT_TYPE);
            assertTrue(latestBundle.isPresent());
            assertEquals("2026.01.01.451", latestBundle.get().getVersion());
        }

        private void setupBundles(final org.apache.nifi.bundle.Bundle... bundles) {
            when(extensionManager.getBundles(COMPONENT_TYPE)).thenReturn(List.of(bundles));
        }
    }

    private org.apache.nifi.bundle.Bundle createBundle(final String group, final String artifact, final String version) {
        final BundleCoordinate coordinate = new BundleCoordinate(group, artifact, version);
        final BundleDetails details = new BundleDetails.Builder()
            .coordinate(coordinate)
            .workingDir(new File("."))
            .build();
        return new org.apache.nifi.bundle.Bundle(details, this.getClass().getClassLoader());
    }
}
