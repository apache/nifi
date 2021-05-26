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
package org.apache.nifi.registry.event;

import org.apache.nifi.registry.bucket.Bucket;
import org.apache.nifi.registry.extension.bundle.Bundle;
import org.apache.nifi.registry.extension.bundle.BundleType;
import org.apache.nifi.registry.extension.bundle.BundleVersion;
import org.apache.nifi.registry.extension.bundle.BundleVersionMetadata;
import org.apache.nifi.registry.flow.VersionedFlow;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.apache.nifi.registry.flow.VersionedFlowSnapshotMetadata;
import org.apache.nifi.registry.flow.VersionedProcessGroup;
import org.apache.nifi.registry.hook.Event;
import org.apache.nifi.registry.hook.EventFieldName;
import org.apache.nifi.registry.hook.EventType;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class TestEventFactory {

    private Bucket bucket;
    private VersionedFlow versionedFlow;
    private VersionedFlowSnapshot versionedFlowSnapshot;
    private Bundle bundle;
    private BundleVersion bundleVersion;

    @Before
    public void setup() {
        bucket = new Bucket();
        bucket.setName("Bucket1");
        bucket.setIdentifier(UUID.randomUUID().toString());
        bucket.setCreatedTimestamp(System.currentTimeMillis());

        versionedFlow = new VersionedFlow();
        versionedFlow.setIdentifier(UUID.randomUUID().toString());
        versionedFlow.setName("Flow 1");
        versionedFlow.setBucketIdentifier(bucket.getIdentifier());
        versionedFlow.setBucketName(bucket.getName());

        VersionedFlowSnapshotMetadata metadata = new VersionedFlowSnapshotMetadata();
        metadata.setAuthor("user1");
        metadata.setComments("This is flow 1");
        metadata.setVersion(1);
        metadata.setBucketIdentifier(bucket.getIdentifier());
        metadata.setFlowIdentifier(versionedFlow.getIdentifier());

        versionedFlowSnapshot = new VersionedFlowSnapshot();
        versionedFlowSnapshot.setSnapshotMetadata(metadata);
        versionedFlowSnapshot.setFlowContents(new VersionedProcessGroup());

        bundle = new Bundle();
        bundle.setIdentifier(UUID.randomUUID().toString());
        bundle.setBucketIdentifier(bucket.getIdentifier());
        bundle.setBundleType(BundleType.NIFI_NAR);
        bundle.setGroupId("org.apache.nifi");
        bundle.setArtifactId("nifi-foo-nar");

        final BundleVersionMetadata bundleVersionMetadata = new BundleVersionMetadata();
        bundleVersionMetadata.setId(UUID.randomUUID().toString());
        bundleVersionMetadata.setVersion("1.0.0");
        bundleVersionMetadata.setBucketId(bucket.getIdentifier());
        bundleVersionMetadata.setBundleId(bundle.getIdentifier());

        bundleVersion = new BundleVersion();
        bundleVersion.setVersionMetadata(bundleVersionMetadata);
        bundleVersion.setBundle(bundle);
        bundleVersion.setBucket(bucket);
    }

    @Test
    public void testBucketCreatedEvent() {
        final Event event = EventFactory.bucketCreated(bucket);
        event.validate();

        assertEquals(EventType.CREATE_BUCKET, event.getEventType());
        assertEquals(2, event.getFields().size());

        assertEquals(bucket.getIdentifier(), event.getField(EventFieldName.BUCKET_ID).getValue());
        assertEquals("unknown", event.getField(EventFieldName.USER).getValue());
    }

    @Test
    public void testBucketUpdatedEvent() {
        final Event event = EventFactory.bucketUpdated(bucket);
        event.validate();

        assertEquals(EventType.UPDATE_BUCKET, event.getEventType());
        assertEquals(2, event.getFields().size());

        assertEquals(bucket.getIdentifier(), event.getField(EventFieldName.BUCKET_ID).getValue());
        assertEquals("unknown", event.getField(EventFieldName.USER).getValue());
    }

    @Test
    public void testBucketDeletedEvent() {
        final Event event = EventFactory.bucketDeleted(bucket);
        event.validate();

        assertEquals(EventType.DELETE_BUCKET, event.getEventType());
        assertEquals(2, event.getFields().size());

        assertEquals(bucket.getIdentifier(), event.getField(EventFieldName.BUCKET_ID).getValue());
        assertEquals("unknown", event.getField(EventFieldName.USER).getValue());
    }

    @Test
    public void testFlowCreated() {
        final Event event = EventFactory.flowCreated(versionedFlow);
        event.validate();

        assertEquals(EventType.CREATE_FLOW, event.getEventType());
        assertEquals(3, event.getFields().size());

        assertEquals(bucket.getIdentifier(), event.getField(EventFieldName.BUCKET_ID).getValue());
        assertEquals(versionedFlow.getIdentifier(), event.getField(EventFieldName.FLOW_ID).getValue());
        assertEquals("unknown", event.getField(EventFieldName.USER).getValue());
    }

    @Test
    public void testFlowUpdated() {
        final Event event = EventFactory.flowUpdated(versionedFlow);
        event.validate();

        assertEquals(EventType.UPDATE_FLOW, event.getEventType());
        assertEquals(3, event.getFields().size());

        assertEquals(bucket.getIdentifier(), event.getField(EventFieldName.BUCKET_ID).getValue());
        assertEquals(versionedFlow.getIdentifier(), event.getField(EventFieldName.FLOW_ID).getValue());
        assertEquals("unknown", event.getField(EventFieldName.USER).getValue());
    }

    @Test
    public void testFlowDeleted() {
        final Event event = EventFactory.flowDeleted(versionedFlow);
        event.validate();

        assertEquals(EventType.DELETE_FLOW, event.getEventType());
        assertEquals(3, event.getFields().size());

        assertEquals(bucket.getIdentifier(), event.getField(EventFieldName.BUCKET_ID).getValue());
        assertEquals(versionedFlow.getIdentifier(), event.getField(EventFieldName.FLOW_ID).getValue());
        assertEquals("unknown", event.getField(EventFieldName.USER).getValue());
    }

    @Test
    public void testFlowVersionedCreated() {
        final Event event = EventFactory.flowVersionCreated(versionedFlowSnapshot);
        event.validate();

        assertEquals(EventType.CREATE_FLOW_VERSION, event.getEventType());
        assertEquals(5, event.getFields().size());

        assertEquals(bucket.getIdentifier(), event.getField(EventFieldName.BUCKET_ID).getValue());
        assertEquals(versionedFlow.getIdentifier(), event.getField(EventFieldName.FLOW_ID).getValue());

        assertEquals(String.valueOf(versionedFlowSnapshot.getSnapshotMetadata().getVersion()),
                event.getField(EventFieldName.VERSION).getValue());

        assertEquals(versionedFlowSnapshot.getSnapshotMetadata().getAuthor(),
                event.getField(EventFieldName.USER).getValue());

        assertEquals(versionedFlowSnapshot.getSnapshotMetadata().getComments(),
                event.getField(EventFieldName.COMMENT).getValue());
    }

    @Test
    public void testFlowVersionedCreatedWhenCommentsMissing() {
        versionedFlowSnapshot.getSnapshotMetadata().setComments(null);
        final Event event = EventFactory.flowVersionCreated(versionedFlowSnapshot);
        event.validate();
        assertEquals("", event.getField(EventFieldName.COMMENT).getValue());
    }

    @Test
    public void testExtensionBundleCreated() {
        final Event event = EventFactory.extensionBundleCreated(bundle);
        event.validate();

        assertEquals(EventType.CREATE_EXTENSION_BUNDLE, event.getEventType());
        assertEquals(3, event.getFields().size());

        assertEquals(bucket.getIdentifier(), event.getField(EventFieldName.BUCKET_ID).getValue());
        assertEquals(bundle.getIdentifier(), event.getField(EventFieldName.EXTENSION_BUNDLE_ID).getValue());
        assertEquals("unknown", event.getField(EventFieldName.USER).getValue());
    }

    @Test
    public void testExtensionBundleDeleted() {
        final Event event = EventFactory.extensionBundleDeleted(bundle);
        event.validate();

        assertEquals(EventType.DELETE_EXTENSION_BUNDLE, event.getEventType());
        assertEquals(3, event.getFields().size());

        assertEquals(bucket.getIdentifier(), event.getField(EventFieldName.BUCKET_ID).getValue());
        assertEquals(bundle.getIdentifier(), event.getField(EventFieldName.EXTENSION_BUNDLE_ID).getValue());
        assertEquals("unknown", event.getField(EventFieldName.USER).getValue());
    }

    @Test
    public void testExtensionBundleVersionCreated() {
        final Event event = EventFactory.extensionBundleVersionCreated(bundleVersion);
        event.validate();

        assertEquals(EventType.CREATE_EXTENSION_BUNDLE_VERSION, event.getEventType());
        assertEquals(4, event.getFields().size());

        assertEquals(bucket.getIdentifier(), event.getField(EventFieldName.BUCKET_ID).getValue());
        assertEquals(bundle.getIdentifier(), event.getField(EventFieldName.EXTENSION_BUNDLE_ID).getValue());
        assertEquals(bundleVersion.getVersionMetadata().getVersion(), event.getField(EventFieldName.VERSION).getValue());
        assertEquals("unknown", event.getField(EventFieldName.USER).getValue());
    }

    @Test
    public void testExtensionBundleVersionDeleted() {
        final Event event = EventFactory.extensionBundleVersionDeleted(bundleVersion);
        event.validate();

        assertEquals(EventType.DELETE_EXTENSION_BUNDLE_VERSION, event.getEventType());
        assertEquals(4, event.getFields().size());

        assertEquals(bucket.getIdentifier(), event.getField(EventFieldName.BUCKET_ID).getValue());
        assertEquals(bundle.getIdentifier(), event.getField(EventFieldName.EXTENSION_BUNDLE_ID).getValue());
        assertEquals(bundleVersion.getVersionMetadata().getVersion(), event.getField(EventFieldName.VERSION).getValue());
        assertEquals("unknown", event.getField(EventFieldName.USER).getValue());
    }
}
