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
package org.apache.nifi.registry.provider.extension;

import org.apache.nifi.registry.extension.BundleVersionCoordinate;
import org.apache.nifi.registry.extension.BundleVersionType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TestStandardBundleVersionCoordinate {

    private static final String BUCKET_ID = "b0000000-0000-0000-0000-000000000000";

    @Test
    void testBuildAcceptsTypicalCoordinates() {
        assertAccepted("org.apache.nifi", "nifi-standard-nar", "2.0.0-SNAPSHOT");
        assertAccepted("org.apache.nifi", "nifi-standard-nar", "1.0.0+build.5");
    }

    @Test
    void testBuildRejectsInvalidComponents() {
        assertInvalid("..", "nifi-standard-nar", "1.0.0");
        assertInvalid("org.apache.nifi", "..", "1.0.0");
        assertInvalid("org.apache.nifi", "nifi-standard-nar", "..");
        assertInvalid(".", "nifi-standard-nar", "1.0.0");
        assertInvalid("org/apache", "nifi-standard-nar", "1.0.0");
    }

    private void assertAccepted(final String groupId, final String artifactId, final String version) {
        final BundleVersionCoordinate coordinate = new StandardBundleVersionCoordinate.Builder()
                .bucketId(BUCKET_ID)
                .groupId(groupId)
                .artifactId(artifactId)
                .version(version)
                .type(BundleVersionType.NIFI_NAR)
                .build();
        assertEquals(groupId, coordinate.getGroupId());
        assertEquals(artifactId, coordinate.getArtifactId());
        assertEquals(version, coordinate.getVersion());
    }

    private void assertInvalid(final String groupId, final String artifactId, final String version) {
        assertThrows(IllegalArgumentException.class, () -> new StandardBundleVersionCoordinate.Builder()
                .bucketId(BUCKET_ID)
                .groupId(groupId)
                .artifactId(artifactId)
                .version(version)
                .type(BundleVersionType.NIFI_NAR)
                .build());
    }
}
