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
package org.apache.nifi.registry.service;

import org.apache.nifi.registry.bucket.Bucket;
import org.apache.nifi.registry.db.entity.BucketEntity;
import org.apache.nifi.registry.db.entity.FlowEntity;
import org.apache.nifi.registry.db.entity.FlowSnapshotEntity;
import org.apache.nifi.registry.diff.ComponentDifference;
import org.apache.nifi.registry.diff.ComponentDifferenceGroup;
import org.apache.nifi.registry.diff.VersionedFlowDifference;
import org.apache.nifi.registry.exception.ResourceNotFoundException;
import org.apache.nifi.registry.extension.BundlePersistenceProvider;
import org.apache.nifi.registry.flow.FlowPersistenceProvider;
import org.apache.nifi.registry.flow.VersionedFlow;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.apache.nifi.registry.flow.VersionedFlowSnapshotMetadata;
import org.apache.nifi.registry.flow.VersionedProcessGroup;
import org.apache.nifi.registry.flow.VersionedProcessor;
import org.apache.nifi.registry.serialization.FlowContent;
import org.apache.nifi.registry.serialization.FlowContentSerializer;
import org.apache.nifi.registry.service.alias.RegistryUrlAliasService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import javax.validation.ConstraintViolationException;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestRegistryService {

    private MetadataService metadataService;
    private FlowPersistenceProvider flowPersistenceProvider;
    private BundlePersistenceProvider bundlePersistenceProvider;
    private FlowContentSerializer flowContentSerializer;
    private Validator validator;
    private RegistryUrlAliasService registryUrlAliasService;

    private RegistryService registryService;

    @Before
    public void setup() {
        metadataService = mock(MetadataService.class);
        flowPersistenceProvider = mock(FlowPersistenceProvider.class);
        bundlePersistenceProvider = mock(BundlePersistenceProvider.class);
        flowContentSerializer = mock(FlowContentSerializer.class);
        registryUrlAliasService = mock(RegistryUrlAliasService.class);

        final ValidatorFactory validatorFactory = Validation.buildDefaultValidatorFactory();
        validator = validatorFactory.getValidator();

        registryService = new RegistryService(metadataService, flowPersistenceProvider, bundlePersistenceProvider,
                flowContentSerializer, validator, registryUrlAliasService);
    }

    // ---------------------- Test Bucket methods ---------------------------------------------

    @Test
    public void testCreateBucketValid() {
        final Bucket bucket = new Bucket();
        bucket.setIdentifier("1");
        bucket.setName("My Bucket");
        bucket.setDescription("This is my bucket.");

        when(metadataService.getBucketsByName(bucket.getName())).thenReturn(Collections.emptyList());

        doAnswer(createBucketAnswer()).when(metadataService).createBucket(any(BucketEntity.class));

        final Bucket createdBucket = registryService.createBucket(bucket);
        assertNotNull(createdBucket);
        assertNotNull(createdBucket.getIdentifier());
        assertNotNull(createdBucket.getCreatedTimestamp());

        assertEquals(bucket.getIdentifier(), createdBucket.getIdentifier());
        assertEquals(bucket.getName(), createdBucket.getName());
        assertEquals(bucket.getDescription(), createdBucket.getDescription());
    }

    @Test(expected = IllegalStateException.class)
    public void testCreateBucketWithSameName() {
        final Bucket bucket = new Bucket();
        bucket.setIdentifier("b2");
        bucket.setName("My Bucket");
        bucket.setDescription("This is my bucket.");

        final BucketEntity existingBucket = new BucketEntity();
        existingBucket.setId("b1");
        existingBucket.setName("My Bucket");
        existingBucket.setCreated(new Date());

        when(metadataService.getBucketsByName(bucket.getName())).thenReturn(Collections.singletonList(existingBucket));

        // should throw exception since a bucket with the same name exists
        registryService.createBucket(bucket);
    }

    @Test(expected = ConstraintViolationException.class)
    public void testCreateBucketWithMissingName() {
        final Bucket bucket = new Bucket();
        when(metadataService.getBucketsByName(bucket.getName())).thenReturn(Collections.emptyList());
        registryService.createBucket(bucket);
    }

    @Test
    public void testGetExistingBucket() {
        final BucketEntity existingBucket = new BucketEntity();
        existingBucket.setId("b1");
        existingBucket.setName("My Bucket");
        existingBucket.setDescription("This is my bucket");
        existingBucket.setCreated(new Date());

        when(metadataService.getBucketById(existingBucket.getId())).thenReturn(existingBucket);

        final Bucket bucket = registryService.getBucket(existingBucket.getId());
        assertNotNull(bucket);
        assertEquals(existingBucket.getId(), bucket.getIdentifier());
        assertEquals(existingBucket.getName(), bucket.getName());
        assertEquals(existingBucket.getDescription(), bucket.getDescription());
        assertEquals(existingBucket.getCreated().getTime(), bucket.getCreatedTimestamp());
    }

    @Test(expected = ResourceNotFoundException.class)
    public void testGetBucketDoesNotExist() {
        when(metadataService.getBucketById(any(String.class))).thenReturn(null);
        registryService.getBucket("does-not-exist");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUpdateBucketWithoutId() {
        final Bucket bucket = new Bucket();
        bucket.setName("My Bucket");
        bucket.setDescription("This is my bucket.");
        registryService.updateBucket(bucket);
    }

    @Test(expected = ResourceNotFoundException.class)
    public void testUpdateBucketDoesNotExist() {
        final Bucket bucket = new Bucket();
        bucket.setIdentifier("b1");
        bucket.setName("My Bucket");
        bucket.setDescription("This is my bucket.");
        registryService.updateBucket(bucket);

        when(metadataService.getBucketById(any(String.class))).thenReturn(null);
        registryService.updateBucket(bucket);
    }

    @Test(expected = IllegalStateException.class)
    public void testUpdateBucketWithSameNameAsExistingBucket() {
        final BucketEntity bucketToUpdate = new BucketEntity();
        bucketToUpdate.setId("b1");
        bucketToUpdate.setName("My Bucket");
        bucketToUpdate.setDescription("This is my bucket");
        bucketToUpdate.setCreated(new Date());

        when(metadataService.getBucketById(bucketToUpdate.getId())).thenReturn(bucketToUpdate);

        final BucketEntity otherBucket = new BucketEntity();
        otherBucket.setId("b2");
        otherBucket.setName("My Bucket #2");
        otherBucket.setDescription("This is my bucket");
        otherBucket.setCreated(new Date());

        when(metadataService.getBucketsByName(otherBucket.getName())).thenReturn(Collections.singletonList(otherBucket));

        // should fail because other bucket has the same name
        final Bucket updatedBucket = new Bucket();
        updatedBucket.setIdentifier(bucketToUpdate.getId());
        updatedBucket.setName("My Bucket #2");
        updatedBucket.setDescription(bucketToUpdate.getDescription());

        registryService.updateBucket(updatedBucket);
    }

    @Test
    public void testUpdateBucket() {
        final BucketEntity bucketToUpdate = new BucketEntity();
        bucketToUpdate.setId("b1");
        bucketToUpdate.setName("My Bucket");
        bucketToUpdate.setDescription("This is my bucket");
        bucketToUpdate.setCreated(new Date());

        when(metadataService.getBucketById(bucketToUpdate.getId())).thenReturn(bucketToUpdate);

        doAnswer(updateBucketAnswer()).when(metadataService).updateBucket(any(BucketEntity.class));

        final Bucket updatedBucket = new Bucket();
        updatedBucket.setIdentifier(bucketToUpdate.getId());
        updatedBucket.setName("Updated Name");
        updatedBucket.setDescription("Updated Description");

        final Bucket result = registryService.updateBucket(updatedBucket);
        assertNotNull(result);
        assertEquals(updatedBucket.getName(), result.getName());
        assertEquals(updatedBucket.getDescription(), result.getDescription());
    }

    @Test
    public void testUpdateBucketPartial() {
        final BucketEntity bucketToUpdate = new BucketEntity();
        bucketToUpdate.setId("b1");
        bucketToUpdate.setName("My Bucket");
        bucketToUpdate.setDescription("This is my bucket");
        bucketToUpdate.setCreated(new Date());

        when(metadataService.getBucketById(bucketToUpdate.getId())).thenReturn(bucketToUpdate);

        doAnswer(updateBucketAnswer()).when(metadataService).updateBucket(any(BucketEntity.class));

        final Bucket updatedBucket = new Bucket();
        updatedBucket.setIdentifier(bucketToUpdate.getId());
        updatedBucket.setName("Updated Name");
        updatedBucket.setDescription(null);

        // name should be updated but description should not be changed
        final Bucket result = registryService.updateBucket(updatedBucket);
        assertNotNull(result);
        assertEquals(updatedBucket.getName(), result.getName());
        assertEquals(bucketToUpdate.getDescription(), result.getDescription());
    }

    @Test(expected = ResourceNotFoundException.class)
    public void testDeleteBucketDoesNotExist() {
        final String bucketId = "b1";
        when(metadataService.getBucketById(bucketId)).thenReturn(null);
        registryService.deleteBucket(bucketId);
    }

    @Test
    public void testDeleteBucketWithFlows() {
        final BucketEntity bucketToDelete = new BucketEntity();
        bucketToDelete.setId("b1");
        bucketToDelete.setName("My Bucket");
        bucketToDelete.setDescription("This is my bucket");
        bucketToDelete.setCreated(new Date());

        final FlowEntity flowToDelete = new FlowEntity();
        flowToDelete.setId("flow1");
        flowToDelete.setName("Flow 1");
        flowToDelete.setDescription("This is flow 1");
        flowToDelete.setCreated(new Date());

        final List<FlowEntity> flows = new ArrayList<>();
        flows.add(flowToDelete);

        when(metadataService.getBucketById(bucketToDelete.getId())).thenReturn(bucketToDelete);

        when(metadataService.getFlowsByBucket(bucketToDelete.getId())).thenReturn(flows);

        final Bucket deletedBucket = registryService.deleteBucket(bucketToDelete.getId());
        assertNotNull(deletedBucket);
        assertEquals(bucketToDelete.getId(), deletedBucket.getIdentifier());

        verify(flowPersistenceProvider, times(1))
                .deleteAllFlowContent(eq(bucketToDelete.getId()), eq(flowToDelete.getId()));
    }

    // ---------------------- Test VersionedFlow methods ---------------------------------------------

    @Test(expected = ConstraintViolationException.class)
    public void testCreateFlowInvalid() {
        final VersionedFlow versionedFlow = new VersionedFlow();
        registryService.createFlow("b1", versionedFlow);
    }

    @Test(expected = ResourceNotFoundException.class)
    public void testCreateFlowBucketDoesNotExist() {

        when(metadataService.getBucketById(any(String.class))).thenReturn(null);

        final VersionedFlow versionedFlow = new VersionedFlow();
        versionedFlow.setIdentifier("f1");
        versionedFlow.setName("My Flow");
        versionedFlow.setBucketIdentifier("b1");

        registryService.createFlow(versionedFlow.getBucketIdentifier(), versionedFlow);
    }

    @Test(expected = IllegalStateException.class)
    public void testCreateFlowWithSameName() {
        final BucketEntity existingBucket = new BucketEntity();
        existingBucket.setId("b1");
        existingBucket.setName("My Bucket");
        existingBucket.setDescription("This is my bucket");
        existingBucket.setCreated(new Date());

        when(metadataService.getBucketById(existingBucket.getId())).thenReturn(existingBucket);

        // setup a flow with the same name that already exists

        final FlowEntity flowWithSameName = new FlowEntity();
        flowWithSameName.setId("flow1");
        flowWithSameName.setName("Flow 1");
        flowWithSameName.setDescription("This is flow 1");
        flowWithSameName.setCreated(new Date());
        flowWithSameName.setModified(new Date());

        when(metadataService.getFlowsByName(existingBucket.getId(), flowWithSameName.getName())).thenReturn(Collections.singletonList(flowWithSameName));

        final VersionedFlow versionedFlow = new VersionedFlow();
        versionedFlow.setIdentifier("flow2");
        versionedFlow.setName(flowWithSameName.getName());
        versionedFlow.setBucketIdentifier("b1");

        registryService.createFlow(versionedFlow.getBucketIdentifier(), versionedFlow);
    }

    @Test
    public void testCreateFlowValid() {
        final BucketEntity existingBucket = new BucketEntity();
        existingBucket.setId("b1");
        existingBucket.setName("My Bucket");
        existingBucket.setDescription("This is my bucket");
        existingBucket.setCreated(new Date());

        when(metadataService.getBucketById(existingBucket.getId())).thenReturn(existingBucket);

        final VersionedFlow versionedFlow = new VersionedFlow();
        versionedFlow.setIdentifier("f1");
        versionedFlow.setName("My Flow");
        versionedFlow.setBucketIdentifier("b1");

        doAnswer(createFlowAnswer()).when(metadataService).createFlow(any(FlowEntity.class));

        final VersionedFlow createdFlow = registryService.createFlow(versionedFlow.getBucketIdentifier(), versionedFlow);
        assertNotNull(createdFlow);
        assertNotNull(createdFlow.getIdentifier());
        assertTrue(createdFlow.getCreatedTimestamp() > 0);
        assertTrue(createdFlow.getModifiedTimestamp() > 0);
        assertEquals(versionedFlow.getIdentifier(), createdFlow.getIdentifier());
        assertEquals(versionedFlow.getName(), createdFlow.getName());
        assertEquals(versionedFlow.getBucketIdentifier(), createdFlow.getBucketIdentifier());
        assertEquals(versionedFlow.getDescription(), createdFlow.getDescription());
    }

    @Test(expected = ResourceNotFoundException.class)
    public void testGetFlowDoesNotExist() {
        when(metadataService.getFlowById(any(String.class))).thenReturn(null);
        registryService.getFlow("bucket1","flow1");
    }

    @Test(expected = ResourceNotFoundException.class)
    public void testGetFlowDirectDoesNotExist() {
        when(metadataService.getFlowById(any(String.class))).thenReturn(null);
        registryService.getFlow("flow1");
    }

    @Test
    public void testGetFlowExists() {
        final BucketEntity existingBucket = new BucketEntity();
        existingBucket.setId("b1");
        existingBucket.setName("My Bucket");
        existingBucket.setDescription("This is my bucket");
        existingBucket.setCreated(new Date());

        when(metadataService.getBucketById(existingBucket.getId())).thenReturn(existingBucket);

        final FlowEntity flowEntity = new FlowEntity();
        flowEntity.setId("flow1");
        flowEntity.setName("My Flow");
        flowEntity.setDescription("This is my flow.");
        flowEntity.setCreated(new Date());
        flowEntity.setModified(new Date());
        flowEntity.setBucketId(existingBucket.getId());

        when(metadataService.getFlowByIdWithSnapshotCounts(flowEntity.getId())).thenReturn(flowEntity);

        final VersionedFlow versionedFlow = registryService.getFlow(existingBucket.getId(), flowEntity.getId());
        assertNotNull(versionedFlow);
        assertEquals(flowEntity.getId(), versionedFlow.getIdentifier());
        assertEquals(flowEntity.getName(), versionedFlow.getName());
        assertEquals(flowEntity.getDescription(), versionedFlow.getDescription());
        assertEquals(flowEntity.getBucketId(), versionedFlow.getBucketIdentifier());
        assertEquals(existingBucket.getName(), versionedFlow.getBucketName());
        assertEquals(flowEntity.getCreated().getTime(), versionedFlow.getCreatedTimestamp());
        assertEquals(flowEntity.getModified().getTime(), versionedFlow.getModifiedTimestamp());
    }

    @Test
    public void testGetFlowDirectExists() {
        final BucketEntity existingBucket = new BucketEntity();
        existingBucket.setId("b1");
        existingBucket.setName("My Bucket");
        existingBucket.setDescription("This is my bucket");
        existingBucket.setCreated(new Date());

        final FlowEntity flowEntity = new FlowEntity();
        flowEntity.setId("flow1");
        flowEntity.setName("My Flow");
        flowEntity.setDescription("This is my flow.");
        flowEntity.setCreated(new Date());
        flowEntity.setModified(new Date());
        flowEntity.setBucketId(existingBucket.getId());

        when(metadataService.getFlowByIdWithSnapshotCounts(flowEntity.getId())).thenReturn(flowEntity);
        when(metadataService.getBucketById(existingBucket.getId())).thenReturn(existingBucket);

        final VersionedFlow versionedFlow = registryService.getFlow(flowEntity.getId());
        assertNotNull(versionedFlow);
        assertEquals(flowEntity.getId(), versionedFlow.getIdentifier());
        assertEquals(flowEntity.getName(), versionedFlow.getName());
        assertEquals(flowEntity.getDescription(), versionedFlow.getDescription());
        assertEquals(flowEntity.getBucketId(), versionedFlow.getBucketIdentifier());
        assertEquals(existingBucket.getName(), versionedFlow.getBucketName());
        assertEquals(flowEntity.getCreated().getTime(), versionedFlow.getCreatedTimestamp());
        assertEquals(flowEntity.getModified().getTime(), versionedFlow.getModifiedTimestamp());
    }

    @Test(expected = ResourceNotFoundException.class)
    public void testGetFlowsByBucketDoesNotExist() {
        when(metadataService.getBucketById(any(String.class))).thenReturn(null);
        registryService.getFlows("b1");
    }

    @Test
    public void testGetFlowsByBucketExists() {
        final BucketEntity existingBucket = new BucketEntity();
        existingBucket.setId("b1");
        existingBucket.setName("My Bucket");
        existingBucket.setDescription("This is my bucket");
        existingBucket.setCreated(new Date());

        when(metadataService.getBucketById(existingBucket.getId())).thenReturn(existingBucket);

        final FlowEntity flowEntity1 = new FlowEntity();
        flowEntity1.setId("flow1");
        flowEntity1.setName("My Flow");
        flowEntity1.setDescription("This is my flow.");
        flowEntity1.setCreated(new Date());
        flowEntity1.setModified(new Date());
        flowEntity1.setBucketId(existingBucket.getId());

        final FlowEntity flowEntity2 = new FlowEntity();
        flowEntity2.setId("flow2");
        flowEntity2.setName("My Flow 2");
        flowEntity2.setDescription("This is my flow 2.");
        flowEntity2.setCreated(new Date());
        flowEntity2.setModified(new Date());
        flowEntity2.setBucketId(existingBucket.getId());

        final List<FlowEntity> flows = new ArrayList<>();
        flows.add(flowEntity1);
        flows.add(flowEntity2);

        when(metadataService.getFlowsByBucket(eq(existingBucket.getId()))).thenReturn(flows);

        final List<VersionedFlow> allFlows = registryService.getFlows(existingBucket.getId());
        assertNotNull(allFlows);
        assertEquals(2, allFlows.size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUpdateFlowWithoutId() {
        final VersionedFlow versionedFlow = new VersionedFlow();
        registryService.updateFlow(versionedFlow);
    }

    @Test(expected = ResourceNotFoundException.class)
    public void testUpdateFlowDoesNotExist() {
        final VersionedFlow versionedFlow = new VersionedFlow();
        versionedFlow.setBucketIdentifier("b1");
        versionedFlow.setIdentifier("flow1");

        when(metadataService.getFlowById(versionedFlow.getIdentifier())).thenReturn(null);

        registryService.updateFlow(versionedFlow);
    }

    @Test(expected = IllegalStateException.class)
    public void testUpdateFlowWithSameNameAsExistingFlow() {
        final BucketEntity existingBucket = new BucketEntity();
        existingBucket.setId("b1");
        existingBucket.setName("My Bucket");
        existingBucket.setDescription("This is my bucket");
        existingBucket.setCreated(new Date());

        when(metadataService.getBucketById(existingBucket.getId())).thenReturn(existingBucket);

        final FlowEntity flowToUpdate = new FlowEntity();
        flowToUpdate.setId("flow1");
        flowToUpdate.setName("My Flow");
        flowToUpdate.setDescription("This is my flow.");
        flowToUpdate.setCreated(new Date());
        flowToUpdate.setModified(new Date());
        flowToUpdate.setBucketId(existingBucket.getId());

        when(metadataService.getFlowByIdWithSnapshotCounts(flowToUpdate.getId())).thenReturn(flowToUpdate);

        final FlowEntity otherFlow = new FlowEntity();
        otherFlow.setId("flow2");
        otherFlow.setName("My Flow 2");
        otherFlow.setDescription("This is my flow 2.");
        otherFlow.setCreated(new Date());
        otherFlow.setModified(new Date());
        otherFlow.setBucketId(existingBucket.getId());

        when(metadataService.getFlowsByName(existingBucket.getId(), otherFlow.getName())).thenReturn(Collections.singletonList(otherFlow));

        final VersionedFlow versionedFlow = new VersionedFlow();
        versionedFlow.setIdentifier(flowToUpdate.getId());
        versionedFlow.setBucketIdentifier(existingBucket.getId());
        versionedFlow.setName(otherFlow.getName());

        registryService.updateFlow(versionedFlow);
    }

    @Test
    public void testUpdateFlow() throws InterruptedException {
        final BucketEntity existingBucket = new BucketEntity();
        existingBucket.setId("b1");
        existingBucket.setName("My Bucket");
        existingBucket.setDescription("This is my bucket");
        existingBucket.setCreated(new Date());

        final FlowEntity flowToUpdate = new FlowEntity();
        flowToUpdate.setId("flow1");
        flowToUpdate.setName("My Flow");
        flowToUpdate.setDescription("This is my flow.");
        flowToUpdate.setCreated(new Date());
        flowToUpdate.setModified(new Date());
        flowToUpdate.setBucketId(existingBucket.getId());

        when(metadataService.getBucketById(existingBucket.getId())).thenReturn(existingBucket);
        when(metadataService.getFlowByIdWithSnapshotCounts(flowToUpdate.getId())).thenReturn(flowToUpdate);
        when(metadataService.getFlowsByName(flowToUpdate.getName())).thenReturn(Collections.singletonList(flowToUpdate));

        doAnswer(updateFlowAnswer()).when(metadataService).updateFlow(any(FlowEntity.class));

        final VersionedFlow versionedFlow = new VersionedFlow();
        versionedFlow.setBucketIdentifier(flowToUpdate.getBucketId());
        versionedFlow.setIdentifier(flowToUpdate.getId());
        versionedFlow.setName("New Flow Name");
        versionedFlow.setDescription("This is a new description");

        Thread.sleep(10);

        final VersionedFlow updatedFlow = registryService.updateFlow(versionedFlow);
        assertNotNull(updatedFlow);
        assertEquals(versionedFlow.getIdentifier(), updatedFlow.getIdentifier());

        // name and description should be updated
        assertEquals(versionedFlow.getName(), updatedFlow.getName());
        assertEquals(versionedFlow.getDescription(), updatedFlow.getDescription());

        // other fields should not be updated
        assertEquals(flowToUpdate.getBucketId(), updatedFlow.getBucketIdentifier());
        assertEquals(flowToUpdate.getCreated().getTime(), updatedFlow.getCreatedTimestamp());
    }

    @Test(expected = ResourceNotFoundException.class)
    public void testDeleteFlowDoesNotExist() {
        when(metadataService.getFlowById(any(String.class))).thenReturn(null);
        registryService.deleteFlow("b1", "flow1");
    }

    @Test
    public void testDeleteFlowWithSnapshots() {
        final BucketEntity existingBucket = new BucketEntity();
        existingBucket.setId("b1");
        existingBucket.setName("My Bucket");
        existingBucket.setDescription("This is my bucket");
        existingBucket.setCreated(new Date());

        final FlowEntity flowToDelete = new FlowEntity();
        flowToDelete.setId("flow1");
        flowToDelete.setName("My Flow");
        flowToDelete.setDescription("This is my flow.");
        flowToDelete.setCreated(new Date());
        flowToDelete.setModified(new Date());
        flowToDelete.setBucketId(existingBucket.getId());

        when(metadataService.getBucketById(existingBucket.getId())).thenReturn(existingBucket);
        when(metadataService.getFlowById(flowToDelete.getId())).thenReturn(flowToDelete);
        when(metadataService.getFlowsByName(flowToDelete.getName())).thenReturn(Collections.singletonList(flowToDelete));

        final VersionedFlow deletedFlow = registryService.deleteFlow(existingBucket.getId(), flowToDelete.getId());
        assertNotNull(deletedFlow);
        assertEquals(flowToDelete.getId(), deletedFlow.getIdentifier());

        verify(flowPersistenceProvider, times(1))
                .deleteAllFlowContent(flowToDelete.getBucketId(), flowToDelete.getId());

        verify(metadataService, times(1)).deleteFlow(flowToDelete);
    }

    // ---------------------- Test VersionedFlowSnapshot methods ---------------------------------------------

    private VersionedFlowSnapshot createSnapshot() {
        final VersionedFlowSnapshotMetadata snapshotMetadata = new VersionedFlowSnapshotMetadata();
        snapshotMetadata.setFlowIdentifier("flow1");
        snapshotMetadata.setVersion(1);
        snapshotMetadata.setComments("This is the first snapshot");
        snapshotMetadata.setBucketIdentifier("b1");
        snapshotMetadata.setAuthor("user1");

        final VersionedProcessGroup processGroup = new VersionedProcessGroup();
        processGroup.setIdentifier("pg1");
        processGroup.setName("My Process Group");

        final VersionedFlowSnapshot snapshot = new VersionedFlowSnapshot();
        snapshot.setSnapshotMetadata(snapshotMetadata);
        snapshot.setFlowContents(processGroup);

        return snapshot;
    }

    @Test(expected = ConstraintViolationException.class)
    public void testCreateSnapshotInvalidMetadata() {
        final VersionedFlowSnapshot snapshot = createSnapshot();
        snapshot.getSnapshotMetadata().setFlowIdentifier(null);
        registryService.createFlowSnapshot(snapshot);
    }

    @Test(expected = ConstraintViolationException.class)
    public void testCreateSnapshotInvalidFlowContents() {
        final VersionedFlowSnapshot snapshot = createSnapshot();
        snapshot.setFlowContents(null);
        registryService.createFlowSnapshot(snapshot);
    }

    @Test(expected = ConstraintViolationException.class)
    public void testCreateSnapshotNullMetadata() {
        final VersionedFlowSnapshot snapshot = createSnapshot();
        snapshot.setSnapshotMetadata(null);
        registryService.createFlowSnapshot(snapshot);
    }

    @Test(expected = ConstraintViolationException.class)
    public void testCreateSnapshotNullFlowContents() {
        final VersionedFlowSnapshot snapshot = createSnapshot();
        snapshot.setFlowContents(null);
        registryService.createFlowSnapshot(snapshot);
    }

    @Test(expected = ResourceNotFoundException.class)
    public void testCreateSnapshotBucketDoesNotExist() {
        when(metadataService.getBucketById(any(String.class))).thenReturn(null);

        final VersionedFlowSnapshot snapshot = createSnapshot();
        registryService.createFlowSnapshot(snapshot);
    }

    @Test(expected = ResourceNotFoundException.class)
    public void testCreateSnapshotFlowDoesNotExist() {
        final VersionedFlowSnapshot snapshot = createSnapshot();

        final BucketEntity existingBucket = new BucketEntity();
        existingBucket.setId("b1");
        existingBucket.setName("My Bucket");
        existingBucket.setDescription("This is my bucket");
        existingBucket.setCreated(new Date());

        when(metadataService.getBucketById(existingBucket.getId())).thenReturn(existingBucket);

        when(metadataService.getFlowById(snapshot.getSnapshotMetadata().getFlowIdentifier())).thenReturn(null);

        registryService.createFlowSnapshot(snapshot);
    }

    @Test(expected = IllegalStateException.class)
    public void testCreateSnapshotVersionAlreadyExists() {
        final VersionedFlowSnapshot snapshot = createSnapshot();

        final BucketEntity existingBucket = new BucketEntity();
        existingBucket.setId("b1");
        existingBucket.setName("My Bucket");
        existingBucket.setDescription("This is my bucket");
        existingBucket.setCreated(new Date());

        when(metadataService.getBucketById(existingBucket.getId())).thenReturn(existingBucket);

        // return a flow with the existing snapshot when getFlowById is called
        final FlowEntity existingFlow = new FlowEntity();
        existingFlow.setId("flow1");
        existingFlow.setName("My Flow");
        existingFlow.setDescription("This is my flow.");
        existingFlow.setCreated(new Date());
        existingFlow.setModified(new Date());
        existingFlow.setBucketId(existingBucket.getId());

        when(metadataService.getFlowById(existingFlow.getId())).thenReturn(existingFlow);

        // make a snapshot that has the same version as the one being created
        final FlowSnapshotEntity existingSnapshot = new FlowSnapshotEntity();
        existingSnapshot.setFlowId(snapshot.getSnapshotMetadata().getFlowIdentifier());
        existingSnapshot.setVersion(snapshot.getSnapshotMetadata().getVersion());
        existingSnapshot.setComments("This is an existing snapshot");
        existingSnapshot.setCreated(new Date());
        existingSnapshot.setCreatedBy("test-user");

        final List<FlowSnapshotEntity> existingSnapshots = Arrays.asList(existingSnapshot);
        when(metadataService.getSnapshots(existingFlow.getId())).thenReturn(existingSnapshots);

        registryService.createFlowSnapshot(snapshot);
    }

    @Test(expected = IllegalStateException.class)
    public void testCreateSnapshotVersionNotNextVersion() {
        final VersionedFlowSnapshot snapshot = createSnapshot();

        final BucketEntity existingBucket = new BucketEntity();
        existingBucket.setId("b1");
        existingBucket.setName("My Bucket");
        existingBucket.setDescription("This is my bucket");
        existingBucket.setCreated(new Date());

        when(metadataService.getBucketById(existingBucket.getId())).thenReturn(existingBucket);

        // return a flow with the existing snapshot when getFlowById is called
        final FlowEntity existingFlow = new FlowEntity();
        existingFlow.setId("flow1");
        existingFlow.setName("My Flow");
        existingFlow.setDescription("This is my flow.");
        existingFlow.setCreated(new Date());
        existingFlow.setModified(new Date());
        existingFlow.setBucketId(existingBucket.getId());

        // make a snapshot that has the same version as the one being created
        final FlowSnapshotEntity existingSnapshot = new FlowSnapshotEntity();
        existingSnapshot.setFlowId(snapshot.getSnapshotMetadata().getFlowIdentifier());
        existingSnapshot.setVersion(snapshot.getSnapshotMetadata().getVersion());
        existingSnapshot.setComments("This is an existing snapshot");
        existingSnapshot.setCreated(new Date());
        existingSnapshot.setCreatedBy("test-user");

        when(metadataService.getFlowById(existingFlow.getId())).thenReturn(existingFlow);

        // set the version to something that is not the next one-up version
        snapshot.getSnapshotMetadata().setVersion(100);
        registryService.createFlowSnapshot(snapshot);
    }

    @Test
    public void testCreateFirstSnapshot() {
        final VersionedFlowSnapshot snapshot = createSnapshot();

        final BucketEntity existingBucket = new BucketEntity();
        existingBucket.setId("b1");
        existingBucket.setName("My Bucket");
        existingBucket.setDescription("This is my bucket");
        existingBucket.setCreated(new Date());

        when(metadataService.getBucketById(existingBucket.getId())).thenReturn(existingBucket);

        // return a flow with the existing snapshot when getFlowById is called
        final FlowEntity existingFlow = new FlowEntity();
        existingFlow.setId("flow1");
        existingFlow.setName("My Flow");
        existingFlow.setDescription("This is my flow.");
        existingFlow.setCreated(new Date());
        existingFlow.setModified(new Date());
        existingFlow.setBucketId(existingBucket.getId());

        when(metadataService.getFlowById(existingFlow.getId())).thenReturn(existingFlow);
        when(metadataService.getFlowByIdWithSnapshotCounts(existingFlow.getId())).thenReturn(existingFlow);

        final VersionedFlowSnapshot createdSnapshot = registryService.createFlowSnapshot(snapshot);
        assertNotNull(createdSnapshot);
        assertNotNull(createdSnapshot.getSnapshotMetadata());
        assertNotNull(createdSnapshot.getFlow());
        assertNotNull(createdSnapshot.getBucket());

        verify(flowContentSerializer, times(1)).serializeFlowContent(any(FlowContent.class), any(OutputStream.class));
        verify(flowPersistenceProvider, times(1)).saveFlowContent(any(), any());
        verify(metadataService, times(1)).createFlowSnapshot(any(FlowSnapshotEntity.class));
    }

    @Test(expected = IllegalStateException.class)
    public void testCreateFirstSnapshotWithBadVersion() {
        final VersionedFlowSnapshot snapshot = createSnapshot();

        final BucketEntity existingBucket = new BucketEntity();
        existingBucket.setId("b1");
        existingBucket.setName("My Bucket");
        existingBucket.setDescription("This is my bucket");
        existingBucket.setCreated(new Date());

        when(metadataService.getBucketById(existingBucket.getId())).thenReturn(existingBucket);

        // return a flow with the existing snapshot when getFlowById is called
        final FlowEntity existingFlow = new FlowEntity();
        existingFlow.setId("flow1");
        existingFlow.setName("My Flow");
        existingFlow.setDescription("This is my flow.");
        existingFlow.setCreated(new Date());
        existingFlow.setModified(new Date());
        existingFlow.setBucketId(existingBucket.getId());

        when(metadataService.getFlowById(existingFlow.getId())).thenReturn(existingFlow);

        // set the first version to something other than 1
        snapshot.getSnapshotMetadata().setVersion(100);
        registryService.createFlowSnapshot(snapshot);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateFirstSnapshotWithZeroVersion() {
        final VersionedFlowSnapshot snapshot = createSnapshot();

        final BucketEntity existingBucket = new BucketEntity();
        existingBucket.setId("b1");
        existingBucket.setName("My Bucket");
        existingBucket.setDescription("This is my bucket");
        existingBucket.setCreated(new Date());

        when(metadataService.getBucketById(existingBucket.getId())).thenReturn(existingBucket);

        // return a flow with the existing snapshot when getFlowById is called
        final FlowEntity existingFlow = new FlowEntity();
        existingFlow.setId("flow1");
        existingFlow.setName("My Flow");
        existingFlow.setDescription("This is my flow.");
        existingFlow.setCreated(new Date());
        existingFlow.setModified(new Date());
        existingFlow.setBucketId(existingBucket.getId());

        when(metadataService.getFlowById(existingFlow.getId())).thenReturn(existingFlow);

        // set the first version to something other than 1
        snapshot.getSnapshotMetadata().setVersion(0);
        registryService.createFlowSnapshot(snapshot);
    }

    @Test
    public void testCreateFirstSnapshotWithLatestVersionWhenVersionExist() {
        final VersionedFlowSnapshot snapshot = createSnapshot();

        final BucketEntity existingBucket = new BucketEntity();
        existingBucket.setId("b1");
        existingBucket.setName("My Bucket");
        existingBucket.setDescription("This is my bucket");
        existingBucket.setCreated(new Date());

        when(metadataService.getBucketById(existingBucket.getId())).thenReturn(existingBucket);

        // return a flow with the existing snapshot when getFlowById is called
        final FlowEntity existingFlow = new FlowEntity();
        existingFlow.setId("flow1");
        existingFlow.setName("My Flow");
        existingFlow.setDescription("This is my flow.");
        existingFlow.setCreated(new Date());
        existingFlow.setModified(new Date());
        existingFlow.setBucketId(existingBucket.getId());

        when(metadataService.getFlowById(existingFlow.getId())).thenReturn(existingFlow);
        when(metadataService.getFlowByIdWithSnapshotCounts(existingFlow.getId())).thenReturn(existingFlow);

        // make a snapshot that has the same version as the one being created
        final FlowSnapshotEntity existingSnapshot = new FlowSnapshotEntity();
        existingSnapshot.setFlowId(snapshot.getSnapshotMetadata().getFlowIdentifier());
        existingSnapshot.setVersion(snapshot.getSnapshotMetadata().getVersion());
        existingSnapshot.setComments("This is an existing snapshot");
        existingSnapshot.setCreated(new Date());
        existingSnapshot.setCreatedBy("test-user");

        final List<FlowSnapshotEntity> existingSnapshots = Arrays.asList(existingSnapshot);
        when(metadataService.getSnapshots(existingFlow.getId())).thenReturn(existingSnapshots);

        // set the version to -1 to indicate that registry should make this the latest version
        snapshot.getSnapshotMetadata().setVersion(-1);
        registryService.createFlowSnapshot(snapshot);

        final VersionedFlowSnapshot createdSnapshot = registryService.createFlowSnapshot(snapshot);
        assertNotNull(createdSnapshot);
        assertNotNull(createdSnapshot.getSnapshotMetadata());
        assertEquals(2, createdSnapshot.getSnapshotMetadata().getVersion());
    }

    @Test
    public void testCreateFirstSnapshotWithLatestVersionWhenNoVersionsExist() {
        final VersionedFlowSnapshot snapshot = createSnapshot();

        final BucketEntity existingBucket = new BucketEntity();
        existingBucket.setId("b1");
        existingBucket.setName("My Bucket");
        existingBucket.setDescription("This is my bucket");
        existingBucket.setCreated(new Date());

        when(metadataService.getBucketById(existingBucket.getId())).thenReturn(existingBucket);

        // return a flow with the existing snapshot when getFlowById is called
        final FlowEntity existingFlow = new FlowEntity();
        existingFlow.setId("flow1");
        existingFlow.setName("My Flow");
        existingFlow.setDescription("This is my flow.");
        existingFlow.setCreated(new Date());
        existingFlow.setModified(new Date());
        existingFlow.setBucketId(existingBucket.getId());

        when(metadataService.getFlowById(existingFlow.getId())).thenReturn(existingFlow);
        when(metadataService.getFlowByIdWithSnapshotCounts(existingFlow.getId())).thenReturn(existingFlow);
        when(metadataService.getSnapshots(existingFlow.getId())).thenReturn(Collections.emptyList());

        // set the version to -1 to indicate that registry should make this the latest version
        snapshot.getSnapshotMetadata().setVersion(-1);
        registryService.createFlowSnapshot(snapshot);

        final VersionedFlowSnapshot createdSnapshot = registryService.createFlowSnapshot(snapshot);
        assertNotNull(createdSnapshot);
        assertNotNull(createdSnapshot.getSnapshotMetadata());
        assertEquals(1, createdSnapshot.getSnapshotMetadata().getVersion());
    }

    @Test
    public void testGetFlowSnapshots() {
        final BucketEntity existingBucket = new BucketEntity();
        existingBucket.setId("b1");
        existingBucket.setName("My Bucket");
        existingBucket.setDescription("This is my bucket");
        existingBucket.setCreated(new Date());

        when(metadataService.getBucketById(existingBucket.getId())).thenReturn(existingBucket);

        // return a flow with the existing snapshot when getFlowById is called
        final FlowEntity existingFlow = new FlowEntity();
        existingFlow.setId("flow1");
        existingFlow.setName("My Flow");
        existingFlow.setDescription("This is my flow.");
        existingFlow.setCreated(new Date());
        existingFlow.setModified(new Date());
        existingFlow.setBucketId(existingBucket.getId());

        when(metadataService.getFlowById(existingFlow.getId())).thenReturn(existingFlow);

        final FlowSnapshotEntity existingSnapshot1 = new FlowSnapshotEntity();
        existingSnapshot1.setVersion(1);
        existingSnapshot1.setFlowId(existingFlow.getId());
        existingSnapshot1.setCreatedBy("user1");
        existingSnapshot1.setCreated(new Date());
        existingSnapshot1.setComments("This is snapshot 1");

        final FlowSnapshotEntity existingSnapshot2 = new FlowSnapshotEntity();
        existingSnapshot2.setVersion(2);
        existingSnapshot2.setFlowId(existingFlow.getId());
        existingSnapshot2.setCreatedBy("user2");
        existingSnapshot2.setCreated(new Date());
        existingSnapshot2.setComments("This is snapshot 2");

        final List<FlowSnapshotEntity> snapshots = new ArrayList<>();
        snapshots.add(existingSnapshot1);
        snapshots.add(existingSnapshot2);

        when(metadataService.getSnapshots(existingFlow.getId())).thenReturn(snapshots);

        final SortedSet<VersionedFlowSnapshotMetadata> retrievedSnapshots = registryService.getFlowSnapshots(existingBucket.getId(), existingFlow.getId());
        assertNotNull(retrievedSnapshots);
        assertEquals(2, retrievedSnapshots.size());
        // check that sorted set order is reversed
        assertEquals(2, retrievedSnapshots.first().getVersion());
        assertEquals(1, retrievedSnapshots.last().getVersion());
    }

    @Test
    public void testGetFlowSnapshotsWhenNoSnapshots() {
        final BucketEntity existingBucket = new BucketEntity();
        existingBucket.setId("b1");
        existingBucket.setName("My Bucket");
        existingBucket.setDescription("This is my bucket");
        existingBucket.setCreated(new Date());

        when(metadataService.getBucketById(existingBucket.getId())).thenReturn(existingBucket);

        // return a flow with the existing snapshot when getFlowById is called
        final FlowEntity existingFlow = new FlowEntity();
        existingFlow.setId("flow1");
        existingFlow.setName("My Flow");
        existingFlow.setDescription("This is my flow.");
        existingFlow.setCreated(new Date());
        existingFlow.setModified(new Date());
        existingFlow.setBucketId(existingBucket.getId());

        final Set<FlowSnapshotEntity> snapshots = new HashSet<>();

        when(metadataService.getFlowById(existingFlow.getId())).thenReturn(existingFlow);

        final SortedSet<VersionedFlowSnapshotMetadata> retrievedSnapshots = registryService.getFlowSnapshots(existingBucket.getId(), existingFlow.getId());
        assertNotNull(retrievedSnapshots);
        assertEquals(0, retrievedSnapshots.size());
    }

    @Test
    public void testGetLatestSnapshotMetadataWhenVersionsExist() {
        final BucketEntity existingBucket = new BucketEntity();
        existingBucket.setId("b1");
        existingBucket.setName("My Bucket");
        existingBucket.setDescription("This is my bucket");
        existingBucket.setCreated(new Date());

        when(metadataService.getBucketById(existingBucket.getId())).thenReturn(existingBucket);

        // return a flow with the existing snapshot when getFlowById is called
        final FlowEntity existingFlow = new FlowEntity();
        existingFlow.setId("flow1");
        existingFlow.setName("My Flow");
        existingFlow.setDescription("This is my flow.");
        existingFlow.setCreated(new Date());
        existingFlow.setModified(new Date());
        existingFlow.setBucketId(existingBucket.getId());

        when(metadataService.getFlowById(existingFlow.getId())).thenReturn(existingFlow);

        final FlowSnapshotEntity existingSnapshot1 = new FlowSnapshotEntity();
        existingSnapshot1.setVersion(1);
        existingSnapshot1.setFlowId(existingFlow.getId());
        existingSnapshot1.setCreatedBy("user1");
        existingSnapshot1.setCreated(new Date());
        existingSnapshot1.setComments("This is snapshot 1");

        when(metadataService.getLatestSnapshot(existingFlow.getId())).thenReturn(existingSnapshot1);

        VersionedFlowSnapshotMetadata latestMetadata = registryService.getLatestFlowSnapshotMetadata(existingBucket.getId(), existingFlow.getId());
        assertNotNull(latestMetadata);
        assertEquals(1, latestMetadata.getVersion());
    }

    @Test
    public void testGetLatestSnapshotMetadataWhenNoVersionsExist() {
        final BucketEntity existingBucket = new BucketEntity();
        existingBucket.setId("b1");
        existingBucket.setName("My Bucket");
        existingBucket.setDescription("This is my bucket");
        existingBucket.setCreated(new Date());

        when(metadataService.getBucketById(existingBucket.getId())).thenReturn(existingBucket);

        // return a flow with the existing snapshot when getFlowById is called
        final FlowEntity existingFlow = new FlowEntity();
        existingFlow.setId("flow1");
        existingFlow.setName("My Flow");
        existingFlow.setDescription("This is my flow.");
        existingFlow.setCreated(new Date());
        existingFlow.setModified(new Date());
        existingFlow.setBucketId(existingBucket.getId());

        when(metadataService.getFlowById(existingFlow.getId())).thenReturn(existingFlow);

        final FlowSnapshotEntity existingSnapshot1 = new FlowSnapshotEntity();
        existingSnapshot1.setVersion(1);
        existingSnapshot1.setFlowId(existingFlow.getId());
        existingSnapshot1.setCreatedBy("user1");
        existingSnapshot1.setCreated(new Date());
        existingSnapshot1.setComments("This is snapshot 1");

        when(metadataService.getLatestSnapshot(existingFlow.getId())).thenReturn(null);

        try {
            registryService.getLatestFlowSnapshotMetadata(existingBucket.getId(), existingFlow.getId());
            Assert.fail("Should have thrown exception");
        } catch (ResourceNotFoundException e) {
            assertEquals("The specified flow ID has no versions", e.getMessage());
        }
    }

    @Test(expected = ResourceNotFoundException.class)
    public void testGetSnapshotDoesNotExistInMetadataProvider() {
        final String bucketId = "b1";
        final String flowId = "flow1";
        final Integer version = 1;
        when(metadataService.getFlowSnapshot(flowId, version)).thenReturn(null);
        registryService.getFlowSnapshot(bucketId, flowId, version);
    }

    @Test(expected = IllegalStateException.class)
    public void testGetSnapshotDoesNotExistInPersistenceProvider() {
        final BucketEntity existingBucket = createBucketEntity("b1");
        final FlowEntity existingFlow = createFlowEntity(existingBucket.getId());
        final FlowSnapshotEntity existingSnapshot = createFlowSnapshotEntity(existingFlow.getId());

        existingFlow.setSnapshotCount(10);

        when(metadataService.getBucketById(existingBucket.getId()))
                .thenReturn(existingBucket);

        when(metadataService.getFlowByIdWithSnapshotCounts(existingFlow.getId()))
                .thenReturn(existingFlow);

        when(metadataService.getFlowSnapshot(existingFlow.getId(), existingSnapshot.getVersion()))
                .thenReturn(existingSnapshot);

        when(flowPersistenceProvider.getFlowContent(
                existingBucket.getId(),
                existingSnapshot.getFlowId(),
                existingSnapshot.getVersion()
        )).thenReturn(null);

        registryService.getFlowSnapshot(existingBucket.getId(), existingSnapshot.getFlowId(), existingSnapshot.getVersion());
    }

    @Test
    public void testGetSnapshotExists() {
        final BucketEntity existingBucket = createBucketEntity("b1");
        final FlowEntity existingFlow = createFlowEntity(existingBucket.getId());
        final FlowSnapshotEntity existingSnapshot = createFlowSnapshotEntity(existingFlow.getId());

        existingFlow.setSnapshotCount(10);

        when(metadataService.getBucketById(existingBucket.getId()))
                .thenReturn(existingBucket);

        when(metadataService.getFlowByIdWithSnapshotCounts(existingFlow.getId()))
                .thenReturn(existingFlow);

        when(metadataService.getFlowSnapshot(existingFlow.getId(), existingSnapshot.getVersion()))
                .thenReturn(existingSnapshot);

        // return a non-null, non-zero-length array so something gets passed to the serializer
        when(flowPersistenceProvider.getFlowContent(
                existingBucket.getId(),
                existingSnapshot.getFlowId(),
                existingSnapshot.getVersion()
        )).thenReturn(new byte[10]);

        final FlowContent flowContent = new FlowContent();
        flowContent.setFlowSnapshot(createSnapshot());
        when(flowContentSerializer.readDataModelVersion(any(InputStream.class))).thenReturn(3);
        when(flowContentSerializer.deserializeFlowContent(eq(3), any(InputStream.class))).thenReturn(flowContent);

        final VersionedFlowSnapshot returnedSnapshot = registryService.getFlowSnapshot(
                existingBucket.getId(), existingSnapshot.getFlowId(), existingSnapshot.getVersion());
        assertNotNull(returnedSnapshot);
        assertNotNull(returnedSnapshot.getSnapshotMetadata());

        final VersionedFlowSnapshotMetadata snapshotMetadata = returnedSnapshot.getSnapshotMetadata();
        assertEquals(existingSnapshot.getVersion().intValue(), snapshotMetadata.getVersion());
        assertEquals(existingBucket.getId(), snapshotMetadata.getBucketIdentifier());
        assertEquals(existingSnapshot.getFlowId(), snapshotMetadata.getFlowIdentifier());
        assertEquals(existingSnapshot.getCreated(), new Date(snapshotMetadata.getTimestamp()));
        assertEquals(existingSnapshot.getCreatedBy(), snapshotMetadata.getAuthor());
        assertEquals(existingSnapshot.getComments(), snapshotMetadata.getComments());

        final VersionedFlow versionedFlow = returnedSnapshot.getFlow();
        assertNotNull(versionedFlow);
        assertNotNull(versionedFlow.getVersionCount());
        assertTrue(versionedFlow.getVersionCount() > 0);

        final Bucket bucket = returnedSnapshot.getBucket();
        assertNotNull(bucket);
    }

    @Test(expected = ResourceNotFoundException.class)
    public void testDeleteSnapshotDoesNotExist() {
        final String bucketId = "b1";
        final String flowId = "flow1";
        final Integer version = 1;
        when(metadataService.getFlowSnapshot(flowId, version)).thenReturn(null);
        registryService.deleteFlowSnapshot(bucketId, flowId, version);
    }

    @Test
    public void testDeleteSnapshotExists() {
        final BucketEntity existingBucket = createBucketEntity("b1");
        final FlowEntity existingFlow = createFlowEntity(existingBucket.getId());
        final FlowSnapshotEntity existingSnapshot = createFlowSnapshotEntity(existingFlow.getId());

        when(metadataService.getBucketById(existingBucket.getId()))
                .thenReturn(existingBucket);

        when(metadataService.getFlowById(existingFlow.getId()))
                .thenReturn(existingFlow);

        when(metadataService.getFlowSnapshot(existingSnapshot.getFlowId(), existingSnapshot.getVersion()))
                .thenReturn(existingSnapshot);

        final VersionedFlowSnapshotMetadata deletedSnapshot = registryService.deleteFlowSnapshot(
                existingBucket.getId(), existingSnapshot.getFlowId(), existingSnapshot.getVersion());

        assertNotNull(deletedSnapshot);
        assertEquals(existingSnapshot.getFlowId(), deletedSnapshot.getFlowIdentifier());

        verify(flowPersistenceProvider, times(1)).deleteFlowContent(
                existingBucket.getId(),
                existingSnapshot.getFlowId(),
                existingSnapshot.getVersion()
        );

        verify(metadataService, times(1)).deleteFlowSnapshot(existingSnapshot);
    }

    private FlowSnapshotEntity createFlowSnapshotEntity(final String flowId) {
        final FlowSnapshotEntity existingSnapshot = new FlowSnapshotEntity();
        existingSnapshot.setVersion(1);
        existingSnapshot.setFlowId(flowId);
        existingSnapshot.setComments("This is an existing snapshot");
        existingSnapshot.setCreated(new Date());
        existingSnapshot.setCreatedBy("test-user");
        return existingSnapshot;
    }

    private FlowEntity createFlowEntity(final String bucketId) {
        final FlowEntity existingFlow = new FlowEntity();
        existingFlow.setId("flow1");
        existingFlow.setName("My Flow");
        existingFlow.setDescription("This is my flow.");
        existingFlow.setCreated(new Date());
        existingFlow.setModified(new Date());
        existingFlow.setBucketId(bucketId);
        return existingFlow;
    }

    private BucketEntity createBucketEntity(final String bucketId) {
        final BucketEntity existingBucket = new BucketEntity();
        existingBucket.setId(bucketId);
        existingBucket.setName("My Bucket");
        existingBucket.setDescription("This is my bucket");
        existingBucket.setCreated(new Date());
        return existingBucket;
    }

    // -----------------Test Flow Diff Service Method---------------------
    @Test
    public void testGetDiffReturnsRemovedComponentChanges() {
        when(flowPersistenceProvider.getFlowContent(
                anyString(), anyString(), anyInt()
        )).thenReturn(new byte[10], new byte[10]);

        final VersionedProcessGroup pgA = createVersionedProcessGroupA();
        final VersionedProcessGroup pgB = createVersionedProcessGroupB();
        when(flowContentSerializer.readDataModelVersion(any(InputStream.class))).thenReturn(2);
        when(flowContentSerializer.isProcessGroupVersion(eq(2))).thenReturn(true);
        when(flowContentSerializer.deserializeProcessGroup(eq(2),any())).thenReturn(pgA, pgB);

        final VersionedFlowDifference diff = registryService.getFlowDiff(
                "bucketIdentifier", "flowIdentifier", 1, 2);

        assertNotNull(diff);
        Optional<ComponentDifferenceGroup> removedComponent = diff.getComponentDifferenceGroups().stream()
                .filter(p->p.getComponentId().equals("ID-pg1")).findFirst();

        assertTrue(removedComponent.isPresent());
        assertTrue(removedComponent.get().getDifferences().iterator().next().getDifferenceType().equals("COMPONENT_REMOVED"));
    }

    @Test
    public void testGetDiffReturnsChangesInChronologicalOrder() {
        when(flowPersistenceProvider.getFlowContent(
                anyString(), anyString(), anyInt()
        )).thenReturn(new byte[10], new byte[10]);

        final VersionedProcessGroup pgA = createVersionedProcessGroupA();
        final VersionedProcessGroup pgB = createVersionedProcessGroupB();
        when(flowContentSerializer.readDataModelVersion(any(InputStream.class))).thenReturn(2);
        when(flowContentSerializer.isProcessGroupVersion(eq(2))).thenReturn(true);
        when(flowContentSerializer.deserializeProcessGroup(eq(2),any())).thenReturn(pgA, pgB);

        // getFlowDiff orders the changes in ascending order of version number regardless of param order
        final VersionedFlowDifference diff = registryService.getFlowDiff(
                "bucketIdentifier", "flowIdentifier", 2,1);

        assertNotNull(diff);
        Optional<ComponentDifferenceGroup> nameChangedComponent = diff.getComponentDifferenceGroups().stream()
                .filter(p->p.getComponentId().equals("ProcessorFirstV1")).findFirst();

        assertTrue(nameChangedComponent.isPresent());

        ComponentDifference nameChangeDifference = nameChangedComponent.get().getDifferences().stream()
                .filter(d-> d.getDifferenceType().equals("NAME_CHANGED")).findFirst().get();

        assertEquals("ProcessorFirstV1", nameChangeDifference.getValueA());
        assertEquals("ProcessorFirstV2", nameChangeDifference.getValueB());
    }

    private VersionedProcessGroup createVersionedProcessGroupA() {
        VersionedProcessGroup root = new VersionedProcessGroup();
        root.setProcessGroups(new HashSet<>(Arrays.asList(createProcessGroup("ID-pg1"), createProcessGroup("ID-pg2"))));
        // Add processors
        root.setProcessors(new HashSet<>(Arrays.asList(createVersionedProcessor("ProcessorFirstV1"), createVersionedProcessor("ProcessorSecondV1"))));
        return root;
    }

    private VersionedProcessGroup createProcessGroup(String identifier){
        VersionedProcessGroup processGroup = new VersionedProcessGroup();
        processGroup.setIdentifier(identifier);
        return processGroup;
    }
    private VersionedProcessGroup createVersionedProcessGroupB() {
        VersionedProcessGroup updated = createVersionedProcessGroupA();
        // remove a process group
        updated.getProcessGroups().removeIf(pg->pg.getIdentifier().equals("ID-pg1"));
        // change the name of a processor
        updated.getProcessors().stream().forEach(p->p.setPenaltyDuration(p.getName().equals("ProcessorFirstV1") ? "1" : "2"));
        updated.getProcessors().stream().forEach(p->p.setName(p.getName().equals("ProcessorFirstV1") ? "ProcessorFirstV2" : p.getName()));
        return updated;
    }

    private VersionedProcessor createVersionedProcessor(String name){
        VersionedProcessor processor = new VersionedProcessor();
        processor.setName(name);
        processor.setIdentifier(name);
        processor.setProperties(new HashMap<>());
        return processor;
    }
    // -------------------------------------------------------------------

    private Answer<BucketEntity> createBucketAnswer() {
        return (InvocationOnMock invocation) -> {
            BucketEntity bucketEntity = (BucketEntity) invocation.getArguments()[0];
            return bucketEntity;
        };
    }

    private Answer<BucketEntity> updateBucketAnswer() {
        return (InvocationOnMock invocation) -> {
            BucketEntity bucketEntity = (BucketEntity) invocation.getArguments()[0];
            return bucketEntity;
        };
    }

    private Answer<FlowEntity> createFlowAnswer() {
        return (InvocationOnMock invocation) -> {
            final FlowEntity flowEntity = (FlowEntity) invocation.getArguments()[0];
            return flowEntity;
        };
    }

    private Answer<FlowEntity> updateFlowAnswer() {
        return (InvocationOnMock invocation) -> {
            final FlowEntity flowEntity = (FlowEntity) invocation.getArguments()[0];
            return flowEntity;
        };
    }
}
