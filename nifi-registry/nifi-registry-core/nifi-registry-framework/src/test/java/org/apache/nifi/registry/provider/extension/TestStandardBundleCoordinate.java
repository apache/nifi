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

import org.apache.nifi.registry.extension.BundleCoordinate;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TestStandardBundleCoordinate {

    private static final String BUCKET_ID = "b0000000-0000-0000-0000-000000000000";

    @Test
    void testBuildAcceptsTypicalCoordinates() {
        final BundleCoordinate coordinate = new StandardBundleCoordinate.Builder()
                .bucketId(BUCKET_ID)
                .groupId("org.apache.nifi")
                .artifactId("nifi-standard-nar")
                .build();
        assertEquals(BUCKET_ID, coordinate.getBucketId());
        assertEquals("org.apache.nifi", coordinate.getGroupId());
        assertEquals("nifi-standard-nar", coordinate.getArtifactId());
    }

    @Test
    void testBuildRejectsInvalidComponents() {
        assertInvalid("..", "nifi-standard-nar");
        assertInvalid("org.apache.nifi", "..");
        assertInvalid(".", "nifi-standard-nar");
        assertInvalid("org/apache", "nifi-standard-nar");
        assertInvalid("org.apache.nifi", "art\\ifact");
    }

    private void assertInvalid(final String groupId, final String artifactId) {
        assertThrows(IllegalArgumentException.class, () -> new StandardBundleCoordinate.Builder()
                .bucketId(BUCKET_ID)
                .groupId(groupId)
                .artifactId(artifactId)
                .build());
    }
}
