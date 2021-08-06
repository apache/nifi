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
package org.apache.nifi.registry.revision.entity;

import org.apache.nifi.registry.revision.api.InvalidRevisionException;
import org.apache.nifi.registry.revision.api.RevisionManager;
import org.apache.nifi.registry.revision.naive.NaiveRevisionManager;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TestStandardRevisableEntityService {

    private RevisionManager revisionManager;
    private RevisableEntityService entityService;

    @Before
    public void setup() {
        revisionManager = new NaiveRevisionManager();
        entityService = new StandardRevisableEntityService(revisionManager);
    }

    @Test
    public void testCreate() {
        final String clientId = "client1";
        final RevisionInfo requestRevision = new RevisionInfo(clientId, 0L);

        final String userIdentity = "user1";

        final String entityId = "1";
        final RevisableEntity requestEntity = new TestEntity(entityId, requestRevision);

        final RevisableEntity createdEntity = entityService.create(
                requestEntity, userIdentity, () -> new TestEntity(entityId, null));
        assertNotNull(createdEntity);
        assertEquals(requestEntity.getIdentifier(), createdEntity.getIdentifier());

        final RevisionInfo createdRevision = createdEntity.getRevision();
        assertNotNull(createdRevision);
        assertEquals(requestRevision.getVersion().longValue() + 1, createdRevision.getVersion().longValue());
        assertEquals(clientId, createdRevision.getClientId());
        assertEquals(userIdentity, createdRevision.getLastModifier());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateWhenMissingRevision() {
        final RevisableEntity requestEntity = new TestEntity("1", null);
        entityService.create(requestEntity, "user1", () -> requestEntity);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateWhenNonZeroRevision() {
        final RevisionInfo requestRevision = new RevisionInfo(null, 99L);
        final RevisableEntity requestEntity = new TestEntity("1", requestRevision);
        entityService.create(requestEntity, "user1", () -> requestEntity);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateWhenTaskThrowsException() {
        final RevisionInfo requestRevision = new RevisionInfo("client1", 0L);
        final RevisableEntity requestEntity = new TestEntity("1", requestRevision);
        entityService.create(requestEntity, "user1", () -> {
            throw new IllegalArgumentException("");
        });
    }

    @Test
    public void testGetEntityWhenExists() {
        final RevisableEntity entity = entityService.get(() -> new TestEntity("1", null));
        assertNotNull(entity);

        final RevisionInfo revision = entity.getRevision();
        assertNotNull(revision);
        assertEquals(0, revision.getVersion().longValue());
    }

    @Test
    public void testGetEntityWhenDoesNotExist() {
        final RevisableEntity entity = entityService.get(() -> null);
        assertNull(entity);
    }

    @Test
    public void testGetEntities() {
        final TestEntity entity1 = new TestEntity("1", null);
        final TestEntity entity2 = new TestEntity("2", null);
        final List<TestEntity> entities = Arrays.asList(entity1, entity2);

        final List<TestEntity> resultEntities = entityService.getEntities(() -> entities);
        assertNotNull(resultEntities);
        resultEntities.forEach(e -> {
            assertNotNull(e.getRevision());
            assertEquals(0, e.getRevision().getVersion().longValue());
        });
    }

    @Test
    public void testUpdate() {
        final RevisionInfo revisionInfo = new RevisionInfo(null, 0L);
        final TestEntity requestEntity = new TestEntity("1", revisionInfo);

        final RevisableEntity createdEntity = entityService.create(
                requestEntity, "user1", () -> requestEntity);
        assertNotNull(createdEntity);
        assertEquals(requestEntity.getIdentifier(), createdEntity.getIdentifier());
        assertNotNull(createdEntity.getRevision());
        assertEquals(1, createdEntity.getRevision().getVersion().longValue());

        final RevisableEntity updatedEntity = entityService.update(
                createdEntity, "user2", () -> createdEntity);
        assertNotNull(updatedEntity.getRevision());
        assertEquals(2, updatedEntity.getRevision().getVersion().longValue());
        assertEquals("user2", updatedEntity.getRevision().getLastModifier());
    }

    @Test
    public void testUpdateWithClientId() {
        final RevisionInfo revisionInfo = new RevisionInfo("client-1", 0L);
        final TestEntity requestEntity = new TestEntity("1", revisionInfo);

        final RevisableEntity createdEntity = entityService.create(
                requestEntity, "user1", () -> requestEntity);
        assertNotNull(createdEntity);
        assertEquals(requestEntity.getIdentifier(), createdEntity.getIdentifier());
        assertNotNull(createdEntity.getRevision());
        assertEquals(1, createdEntity.getRevision().getVersion().longValue());

        final RevisableEntity updatedEntity = entityService.update(
                createdEntity, "user2", () -> createdEntity);
        assertNotNull(updatedEntity.getRevision());
        assertEquals(2, updatedEntity.getRevision().getVersion().longValue());
        assertEquals("user2", updatedEntity.getRevision().getLastModifier());

        // set the version back to 0 to prove that we can update based on client id being the same
        updatedEntity.getRevision().setVersion(0L);

        final RevisableEntity updatedEntity2 = entityService.update(
                createdEntity, "user3", () -> updatedEntity);
        assertNotNull(updatedEntity2.getRevision());
        assertEquals(3, updatedEntity2.getRevision().getVersion().longValue());
        assertEquals("user3", updatedEntity2.getRevision().getLastModifier());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUpdateWhenMissingRevision() {
        final RevisionInfo revisionInfo = new RevisionInfo(null, 0L);
        final TestEntity requestEntity = new TestEntity("1", revisionInfo);

        final RevisableEntity createdEntity = entityService.create(
                requestEntity, "user1", () -> requestEntity);
        assertNotNull(createdEntity);
        assertEquals(requestEntity.getIdentifier(), createdEntity.getIdentifier());
        assertNotNull(createdEntity.getRevision());
        assertEquals(1, createdEntity.getRevision().getVersion().longValue());

        createdEntity.setRevision(null);
        entityService.update(createdEntity, "user2", () -> createdEntity);
    }

    @Test
    public void testDelete() {
        final RevisionInfo revisionInfo = new RevisionInfo(null, 0L);
        final TestEntity requestEntity = new TestEntity("1", revisionInfo);

        final RevisableEntity createdEntity = entityService.create(
                requestEntity, "user1", () -> requestEntity);
        assertNotNull(createdEntity);

        final RevisableEntity deletedEntity = entityService.delete(
                createdEntity.getIdentifier(), createdEntity.getRevision(), () -> createdEntity);
        assertNotNull(deletedEntity);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDeleteWhenMissingRevision() {
        final RevisionInfo revisionInfo = new RevisionInfo(null, 0L);
        final TestEntity requestEntity = new TestEntity("1", revisionInfo);

        final RevisableEntity createdEntity = entityService.create(
                requestEntity, "user1", () -> requestEntity);
        assertNotNull(createdEntity);
        assertNotNull(createdEntity.getRevision());

        createdEntity.setRevision(null);
        entityService.delete(createdEntity.getIdentifier(), createdEntity.getRevision(), () -> createdEntity);
    }

    @Test(expected = InvalidRevisionException.class)
    public void testDeleteWhenDoesNotExist() {
        final RevisionInfo revisionInfo = new RevisionInfo(null, 1L);
        final RevisableEntity deletedEntity = entityService.delete("1", revisionInfo, () -> null);
        assertNull(deletedEntity);
    }

    /**
     * A RevisableEntity for testing.
     */
    static class TestEntity implements RevisableEntity {

        private String identifier;
        private RevisionInfo revisionInfo;

        public TestEntity(String identifier, RevisionInfo revisionInfo) {
            this.identifier = identifier;
            this.revisionInfo = revisionInfo;
        }

        @Override
        public String getIdentifier() {
            return identifier;
        }

        @Override
        public void setIdentifier(String identifier) {
            this.identifier = identifier;
        }

        @Override
        public RevisionInfo getRevision() {
            return revisionInfo;
        }

        @Override
        public void setRevision(RevisionInfo revision) {
            this.revisionInfo = revision;
        }
    }
}
