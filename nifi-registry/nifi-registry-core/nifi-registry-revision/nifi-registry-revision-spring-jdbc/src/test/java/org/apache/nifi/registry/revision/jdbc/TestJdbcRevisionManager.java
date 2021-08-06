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
package org.apache.nifi.registry.revision.jdbc;

import org.apache.nifi.registry.TestApplication;
import org.apache.nifi.registry.revision.api.DeleteRevisionTask;
import org.apache.nifi.registry.revision.api.EntityModification;
import org.apache.nifi.registry.revision.api.InvalidRevisionException;
import org.apache.nifi.registry.revision.api.Revision;
import org.apache.nifi.registry.revision.api.RevisionClaim;
import org.apache.nifi.registry.revision.api.RevisionManager;
import org.apache.nifi.registry.revision.api.RevisionUpdate;
import org.apache.nifi.registry.revision.api.UpdateRevisionTask;
import org.apache.nifi.registry.revision.standard.StandardRevisionClaim;
import org.apache.nifi.registry.revision.standard.StandardUpdateResult;
import org.flywaydb.core.internal.jdbc.DatabaseType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;
import org.springframework.test.context.transaction.TransactionalTestExecutionListener;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@Transactional
@RunWith(SpringRunner.class)
@SpringBootTest(classes = TestApplication.class, webEnvironment = SpringBootTest.WebEnvironment.NONE)
@TestExecutionListeners({DependencyInjectionTestExecutionListener.class, TransactionalTestExecutionListener.class})
public class TestJdbcRevisionManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestJdbcRevisionManager.class);

    private static final String CREATE_TABLE_SQL_DEFAULT =
            "CREATE TABLE REVISION (\n" +
                    "    ENTITY_ID VARCHAR(50) NOT NULL,\n" +
                    "    VERSION BIGINT NOT NULL DEFAULT (0),\n" +
                    "    CLIENT_ID VARCHAR(100),\n" +
                    "    CONSTRAINT PK__REVISION_ENTITY_ID PRIMARY KEY (ENTITY_ID)\n" +
                    ")";

    private static final String CREATE_TABLE_SQL_MYSQL =
            "CREATE TABLE REVISION (\n" +
                    "    ENTITY_ID VARCHAR(50) NOT NULL,\n" +
                    "    VERSION BIGINT NOT NULL DEFAULT 0,\n" +
                    "    CLIENT_ID VARCHAR(100),\n" +
                    "    CONSTRAINT PK__REVISION_ENTITY_ID PRIMARY KEY (ENTITY_ID)\n" +
                    ")";

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private RevisionManager revisionManager;

    @Before
    public void setup() throws SQLException {
        revisionManager = new JdbcRevisionManager(jdbcTemplate);

        // Create the REVISION table if it does not exist
        final DataSource dataSource = jdbcTemplate.getDataSource();
        LOGGER.info("#### DataSource class is {}", new Object[]{dataSource.getClass().getCanonicalName()});

        try (final Connection connection = dataSource.getConnection()) {
            final String createTableSql;
            final DatabaseType databaseType = DatabaseType.fromJdbcConnection(connection);
            if (databaseType == DatabaseType.MYSQL) {
                createTableSql = CREATE_TABLE_SQL_MYSQL;
            } else {
                createTableSql = CREATE_TABLE_SQL_DEFAULT;
            }

            final DatabaseMetaData meta = connection.getMetaData();
            try (final ResultSet res = meta.getTables(null, null, "REVISION", new String[]{"TABLE"})) {
                if (!res.next()) {
                    jdbcTemplate.execute(createTableSql);
                }
            }
        }
    }

    @Test
    public void testGetRevisionWhenDoesNotExist() {
        final String entityId = "entity1";
        final Revision revision = revisionManager.getRevision(entityId);
        assertNotNull(revision);
        assertEquals(entityId, revision.getEntityId());
        assertEquals(0L, revision.getVersion().longValue());
        assertNull(revision.getClientId());
    }

    @Test
    public void testGetRevisionWhenExists() {
        final String entityId = "entity1";
        final Long version = new Long(99);
        createRevision(entityId, version, null);

        final Revision revision = revisionManager.getRevision(entityId);
        assertNotNull(revision);
        assertEquals(entityId, revision.getEntityId());
        assertEquals(version.longValue(), revision.getVersion().longValue());
        assertNull(revision.getClientId());
    }

    @Test
    public void testUpdateRevisionWithCurrentVersionNoClientId() {
        // create the revision being sent in by the client
        final String entityId = "entity-1";
        final Revision revision = new Revision(99L, null, entityId);
        final RevisionClaim revisionClaim = new StandardRevisionClaim(revision);

        // seed the database with a matching revision
        createRevision(revision.getEntityId(), revision.getVersion(), null);

        // perform an update task
        final RevisionUpdate<RevisableEntity> revisionUpdate = revisionManager.updateRevision(
                revisionClaim, createUpdateTask(entityId));
        assertNotNull(revisionUpdate);

        // version should go to 100 since it was 99 before
        verifyRevisionUpdate(entityId, revisionUpdate, new Long(100), null);
    }

    @Test(expected = InvalidRevisionException.class)
    public void testUpdateRevisionWithStaleVersionNoClientId() {
        // create the revision being sent in by the client
        final String entityId = "entity-1";
        final Revision revision = new Revision(99L, null, entityId);
        final RevisionClaim revisionClaim = new StandardRevisionClaim(revision);

        // seed the database with a revision that has a newer version
        createRevision(revision.getEntityId(), revision.getVersion() + 1, null);

        // perform an update task which should throw InvalidRevisionException
        revisionManager.updateRevision(revisionClaim, createUpdateTask(entityId));
    }

    @Test
    public void testUpdateRevisionWithStaleVersionAndSameClientId() {
        // create the revision being sent in by the client
        final String entityId = "entity-1";
        final String clientId = "client-1";
        final Revision revision = new Revision(99L, clientId, entityId);
        final RevisionClaim revisionClaim = new StandardRevisionClaim(revision);

        // seed the database with a revision that has a newer version
        createRevision(revision.getEntityId(), revision.getVersion() + 1, clientId);

        // perform an update task
        final RevisionUpdate<RevisableEntity> revisionUpdate = revisionManager.updateRevision(
                revisionClaim, createUpdateTask(entityId));
        assertNotNull(revisionUpdate);

        // client in 99 which was not latest version, but since client id was the same the update was allowed
        // and the incremented version should be based on the version in the DB which was 100, so it goes to 101
        verifyRevisionUpdate(entityId, revisionUpdate, new Long(101), clientId);
    }

    @Test
    public void testUpdateRevisionWhenDoesNotExist() {
        // create the revision being sent in by the client
        final String entityId = "entity-1";
        final String clientId = "client-new";
        final Revision revision = new Revision(0L, clientId, entityId);
        final RevisionClaim revisionClaim = new StandardRevisionClaim(revision);

        // perform an update task
        final RevisionUpdate<RevisableEntity> revisionUpdate = revisionManager.updateRevision(
                revisionClaim, createUpdateTask(entityId));
        assertNotNull(revisionUpdate);

        // version should go to 1 and client id should be updated to client-new
        verifyRevisionUpdate(entityId, revisionUpdate, new Long(1), clientId);
    }

    @Test
    public void testUpdateRevisionWithCurrentVersionAndNewClientId() {
        // create the revision being sent in by the client
        final String entityId = "entity-1";
        final String clientId = "client-new";
        final Revision revision = new Revision(99L, clientId, entityId);
        final RevisionClaim revisionClaim = new StandardRevisionClaim(revision);

        // seed the database with a revision that has same version but a different client id
        createRevision(revision.getEntityId(), revision.getVersion(), "client-old");

        // perform an update task
        final RevisionUpdate<RevisableEntity> revisionUpdate = revisionManager.updateRevision(
                revisionClaim, createUpdateTask(entityId));
        assertNotNull(revisionUpdate);

        // version should go to 100 and client id should be updated to client-new
        verifyRevisionUpdate(entityId, revisionUpdate, new Long(100), clientId);
    }

    @Test
    public void testDeleteRevisionWithCurrentVersionAndNoClientId() {
        // create the revision being sent in by the client
        final String entityId = "entity-1";
        final Revision revision = new Revision(99L, null, entityId);
        final RevisionClaim revisionClaim = new StandardRevisionClaim(revision);

        // seed the database with a matching revision
        createRevision(revision.getEntityId(), revision.getVersion(), null);

        // perform an update task
        final RevisableEntity deletedEntity = revisionManager.deleteRevision(
                revisionClaim, createDeleteTask(entityId));
        assertNotNull(deletedEntity);
        assertEquals(entityId, deletedEntity.getId());
    }

    @Test(expected = InvalidRevisionException.class)
    public void testDeleteRevisionWithStaleVersionAndNoClientId() {
        // create the revision being sent in by the client
        final String entityId = "entity-1";
        final Revision revision = new Revision(99L, null, entityId);
        final RevisionClaim revisionClaim = new StandardRevisionClaim(revision);

        // seed the database with a revision that has a newer version
        createRevision(revision.getEntityId(), revision.getVersion() + 1, null);

        // perform an update task which should throw InvalidRevisionException
        revisionManager.deleteRevision(revisionClaim, createDeleteTask(entityId));
    }

    @Test
    public void testDeleteRevisionWithStaleVersionAndSameClientId() {
        // create the revision being sent in by the client
        final String entityId = "entity-1";
        final String clientId = "client-1";
        final Revision revision = new Revision(99L, clientId, entityId);
        final RevisionClaim revisionClaim = new StandardRevisionClaim(revision);

        // seed the database with a revision that has a newer version
        createRevision(revision.getEntityId(), revision.getVersion() + 1, clientId);

        // perform the delete
        final RevisableEntity deletedEntity = revisionManager.deleteRevision(
                revisionClaim, createDeleteTask(entityId));
        assertNotNull(deletedEntity);
        assertEquals(entityId, deletedEntity.getId());
    }

    @Test
    public void testDeleteRevisionWithCurrentVersionAndNewClientId() {
        // create the revision being sent in by the client
        final String entityId = "entity-1";
        final String clientId = "client-new";
        final Revision revision = new Revision(99L, clientId, entityId);
        final RevisionClaim revisionClaim = new StandardRevisionClaim(revision);

        // seed the database with a revision that has same version but a different client id
        createRevision(revision.getEntityId(), revision.getVersion(), "client-old");

        // perform the delete
        final RevisableEntity deletedEntity = revisionManager.deleteRevision(
                revisionClaim, createDeleteTask(entityId));
        assertNotNull(deletedEntity);
        assertEquals(entityId, deletedEntity.getId());
    }

    @Test
    public void testGetAllAndReset() {
        createRevision("entity1", new Long(1), null);
        createRevision("entity2", new Long(1), null);

        final List<Revision> allRevisions = revisionManager.getAllRevisions();
        assertNotNull(allRevisions);
        assertEquals(2, allRevisions.size());

        final Revision resetRevision1 = new Revision(10L, null, "resetEntity1");
        final Revision resetRevision2 = new Revision(50L, null, "resetEntity2");
        final Revision resetRevision3 = new Revision(20L, "client1", "resetEntity3");
        revisionManager.reset(Arrays.asList(resetRevision1, resetRevision2, resetRevision3));

        final List<Revision> afterResetRevisions = revisionManager.getAllRevisions();
        assertNotNull(afterResetRevisions);
        assertEquals(3, afterResetRevisions.size());

        assertTrue(afterResetRevisions.contains(resetRevision1));
        assertTrue(afterResetRevisions.contains(resetRevision2));
        assertTrue(afterResetRevisions.contains(resetRevision3));
    }

    @Test
    public void testGetRevisionMap() {
        createRevision("entity1", new Long(1), null);
        createRevision("entity2", new Long(1), null);

        final Map<String,Revision> revisions = revisionManager.getRevisionMap();
        assertNotNull(revisions);
        assertEquals(2, revisions.size());

        final Revision revision1 = revisions.get("entity1");
        assertNotNull(revision1);
        assertEquals("entity1", revision1.getEntityId());

        final Revision revision2 = revisions.get("entity2");
        assertNotNull(revision2);
        assertEquals("entity2", revision2.getEntityId());
    }

    private DeleteRevisionTask<RevisableEntity> createDeleteTask(final String entityId) {
        return () -> {
            // normally we would retrieve the entity from some kind of service/dao
            final RevisableEntity entity = new RevisableEntity();
            entity.setId(entityId);
            return entity;
        };
    }

    private UpdateRevisionTask<RevisableEntity> createUpdateTask(final String entityId) {
        return () -> {
            // normally we would retrieve the entity from some kind of service/dao
            final RevisableEntity entity = new RevisableEntity();
            entity.setId(entityId);

            return new StandardUpdateResult<>(entity, entityId,"user1");
        };
    }

    private void verifyRevisionUpdate(final String entityId, final RevisionUpdate<RevisableEntity> revisionUpdate,
                                      final Long expectedVersion, final String expectedClientId) {
        // verify we got back the entity we expected
        final RevisableEntity updatedEntity = revisionUpdate.getEntity();
        assertNotNull(updatedEntity);
        assertEquals(entityId, updatedEntity.getId());

        // verify the entity modification is correctly populated
        final EntityModification entityModification = revisionUpdate.getLastModification();
        assertNotNull(entityModification);
        Assert.assertEquals("user1", entityModification.getLastModifier());

        // verify the revision in the entity modification is set and is the updated revision (i.e. version of 100, not 99)
        final Revision updatedRevision = entityModification.getRevision();
        assertNotNull(updatedRevision);
        assertEquals(entityId, updatedRevision.getEntityId());
        assertEquals(expectedVersion, updatedRevision.getVersion());
        assertEquals(expectedClientId, updatedRevision.getClientId());

        // verify the updated revisions is correctly populated and matches the updated entity revision
        final Set<Revision> updatedRevisions = revisionUpdate.getUpdatedRevisions();
        assertNotNull(updatedRevisions);
        assertEquals(1, updatedRevisions.size());
        assertEquals(updatedRevision, updatedRevisions.stream().findFirst().get());
    }

    private void createRevision(final String entityId, final Long version, final String clientId) {
        jdbcTemplate.update("INSERT INTO REVISION(ENTITY_ID, VERSION, CLIENT_ID) VALUES(?, ?, ?)", entityId, version, clientId);
    }

    /**
     * Test object to represent a model/entity that has a revision field.
     */
    private static class RevisableEntity {

        private String id;

        private Revision revision;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public Revision getRevision() {
            return revision;
        }

        public void setRevision(Revision revision) {
            this.revision = revision;
        }
    }
}
