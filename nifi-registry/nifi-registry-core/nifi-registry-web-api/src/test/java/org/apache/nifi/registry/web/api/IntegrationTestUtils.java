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
package org.apache.nifi.registry.web.api;

import org.apache.nifi.registry.bucket.Bucket;
import org.apache.nifi.registry.flow.VersionedComponent;
import org.apache.nifi.registry.flow.VersionedFlow;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.apache.nifi.registry.flow.VersionedFlowSnapshotMetadata;
import org.apache.nifi.registry.flow.VersionedProcessGroup;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

class IntegrationTestUtils {

    public static void assertBucketsEqual(Bucket expected, Bucket actual, boolean checkServerSetFields) {
        assertNotNull(actual);
        assertEquals(expected.getName(), actual.getName());
        assertEquals(expected.getDescription(), actual.getDescription());
        if (checkServerSetFields) {
            assertEquals(expected.getIdentifier(), actual.getIdentifier());
            assertEquals(expected.getCreatedTimestamp(), actual.getCreatedTimestamp());
            assertEquals(expected.getPermissions(), actual.getPermissions());
            assertEquals(expected.getLink(), actual.getLink());
        }
    }

    public static void assertFlowsEqual(VersionedFlow expected, VersionedFlow actual, boolean checkServerSetFields) {
        assertNotNull(actual);
        assertEquals(expected.getName(), actual.getName());
        assertEquals(expected.getDescription(), actual.getDescription());
        assertEquals(expected.getBucketIdentifier(), actual.getBucketIdentifier());
        if (checkServerSetFields) {
            assertEquals(expected.getIdentifier(), actual.getIdentifier());
            assertEquals(expected.getVersionCount(), actual.getVersionCount());
            assertEquals(expected.getCreatedTimestamp(), actual.getCreatedTimestamp());
            assertEquals(expected.getModifiedTimestamp(), actual.getModifiedTimestamp());
            assertEquals(expected.getType(), actual.getType());
            assertEquals(expected.getLink(), actual.getLink());
        }
    }

    public static void assertFlowSnapshotsEqual(VersionedFlowSnapshot expected, VersionedFlowSnapshot actual, boolean checkServerSetFields) {

        assertNotNull(actual);

        if (expected.getSnapshotMetadata() != null) {
            assertFlowSnapshotMetadataEqual(expected.getSnapshotMetadata(), actual.getSnapshotMetadata(), checkServerSetFields);
        }

        if (expected.getFlowContents() != null) {
            assertVersionedProcessGroupsEqual(expected.getFlowContents(), actual.getFlowContents());
        }

        if (checkServerSetFields) {
            assertFlowsEqual(expected.getFlow(), actual.getFlow(), false); // false because if we are checking a newly created snapshot, the versionsCount won't match
            assertBucketsEqual(expected.getBucket(), actual.getBucket(), true);
        }

    }

    public static void assertFlowSnapshotMetadataEqual(
            VersionedFlowSnapshotMetadata expected, VersionedFlowSnapshotMetadata actual, boolean checkServerSetFields) {

        assertNotNull(actual);
        assertEquals(expected.getBucketIdentifier(), actual.getBucketIdentifier());
        assertEquals(expected.getFlowIdentifier(), actual.getFlowIdentifier());
        assertEquals(expected.getVersion(), actual.getVersion());
        assertEquals(expected.getComments(), actual.getComments());
        if (checkServerSetFields) {
            assertEquals(expected.getTimestamp(), actual.getTimestamp());
        }
    }

    private static void assertVersionedProcessGroupsEqual(VersionedProcessGroup expected, VersionedProcessGroup actual) {
        assertNotNull(actual);

        assertEquals(((VersionedComponent)expected), ((VersionedComponent)actual));

        // Poor man's set equality assertion as we are only checking the base type and not doing a recursive check
        // TODO, this would be a stronger assertion by replacing this with a true VersionedProcessGroup.equals() method that does a deep equality check
        assertSetsEqual(expected.getProcessGroups(), actual.getProcessGroups());
        assertSetsEqual(expected.getRemoteProcessGroups(), actual.getRemoteProcessGroups());
        assertSetsEqual(expected.getProcessors(), actual.getProcessors());
        assertSetsEqual(expected.getInputPorts(), actual.getInputPorts());
        assertSetsEqual(expected.getOutputPorts(), actual.getOutputPorts());
        assertSetsEqual(expected.getConnections(), actual.getConnections());
        assertSetsEqual(expected.getLabels(), actual.getLabels());
        assertSetsEqual(expected.getFunnels(), actual.getFunnels());
        assertSetsEqual(expected.getControllerServices(), actual.getControllerServices());
    }


    private static void assertSetsEqual(Set<? extends VersionedComponent> expected, Set<? extends VersionedComponent> actual) {
        if (expected != null) {
            assertNotNull(actual);
            assertEquals(expected.size(), actual.size());
            assertTrue(actual.containsAll(expected));
        }
    }

}
