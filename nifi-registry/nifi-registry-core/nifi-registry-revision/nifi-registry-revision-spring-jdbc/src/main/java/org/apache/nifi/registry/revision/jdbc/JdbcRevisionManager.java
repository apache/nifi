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

import org.apache.nifi.registry.revision.api.DeleteRevisionTask;
import org.apache.nifi.registry.revision.api.EntityModification;
import org.apache.nifi.registry.revision.api.ExpiredRevisionClaimException;
import org.apache.nifi.registry.revision.api.InvalidRevisionException;
import org.apache.nifi.registry.revision.api.Revision;
import org.apache.nifi.registry.revision.api.RevisionClaim;
import org.apache.nifi.registry.revision.api.RevisionManager;
import org.apache.nifi.registry.revision.api.RevisionUpdate;
import org.apache.nifi.registry.revision.api.UpdateResult;
import org.apache.nifi.registry.revision.api.UpdateRevisionTask;
import org.apache.nifi.registry.revision.standard.RevisionComparator;
import org.apache.nifi.registry.revision.standard.StandardRevisionUpdate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A database implementation of {@link RevisionManager} that use's Spring's {@link JdbcTemplate}.
 *
 * It is expected that the database has a table named REVISION with the following schema, but it is up to consumers
 * of this library to manage the creation of this table:
 *
 * <pre>
 * {@code
 *  CREATE TABLE REVISION (
 *     ENTITY_ID VARCHAR(50) NOT NULL,
 *     VERSION BIGINT NOT NULL DEFAULT(0),
 *     CLIENT_ID VARCHAR(100),
 *     CONSTRAINT PK__REVISION_ENTITY_ID PRIMARY KEY (ENTITY_ID)
 *  );
 * }
 * </pre>
 *
 * This implementation leverages the transactional semantics of a relational database to implement an optimistic-locking strategy.
 *
 * In order to function correctly, this must be used with in a transaction with an isolation level of at least READ_COMMITTED.
 */
public class JdbcRevisionManager implements RevisionManager {

    private static Logger LOGGER = LoggerFactory.getLogger(JdbcRevisionManager.class);

    private final JdbcTemplate jdbcTemplate;

    public JdbcRevisionManager(final JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = Objects.requireNonNull(jdbcTemplate);
    }

    @Override
    public Revision getRevision(final String entityId) {
        final Revision revision = retrieveRevision(entityId);
        if (revision == null) {
            return createRevision(entityId);
        } else {
            return revision;
        }
    }

    private Revision retrieveRevision(final String entityId) {
        try {
            final String selectSql = "SELECT * FROM REVISION WHERE ENTITY_ID = ?";
            return jdbcTemplate.queryForObject(selectSql, new Object[] {entityId}, new RevisionRowMapper());
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    private Revision createRevision(final String entityId) {
        final Revision revision = new Revision(0L, null, entityId);
        final String insertSql = "INSERT INTO REVISION(ENTITY_ID, VERSION) VALUES (?, ?)";
        jdbcTemplate.update(insertSql, revision.getEntityId(), revision.getVersion());
        return revision;
    }

    @Override
    public <T> RevisionUpdate<T> updateRevision(final RevisionClaim claim, final UpdateRevisionTask<T> task) {
        LOGGER.debug("Attempting to update revision using {}", claim);

        final List<Revision> revisionList = new ArrayList<>(claim.getRevisions());
        revisionList.sort(new RevisionComparator());

        // Update each revision which increments the version and locks the row.
        // Since we are in transaction these changes won't be committed unless the entire task completes successfully.
        // It is important this happens first so that the task won't execute unless the revision can be updated.
        // This prevents any other changes from happening that might not be part of the database transaction.
        final Set<Revision> incrementedRevisions = new HashSet<>();
        for (final Revision incomingRevision : revisionList) {
            final String entityId = incomingRevision.getEntityId();

            // calling getRevision here will lazily create an initial revision
            getRevision(entityId);
            updateRevision(incomingRevision);

            // retrieve the updated revision since the incoming revision may have matched on the client id
            // and may not have the latest version which we want to return with the result
            final Revision incrementedRevision = getRevision(entityId);
            incrementedRevisions.add(incrementedRevision);
        }

        // We successfully verified all revisions.
        LOGGER.debug("Successfully verified Revision Claim for all revisions");

        // Perform the update
        final UpdateResult<T> updateResult = task.update();
        LOGGER.debug("Update task completed");

        // Create the result with the updated entity and updated revisions
        final T updatedEntity = updateResult.getEntity();
        final String updaterIdentity = updateResult.updaterIdentity();

        final Revision updatedEntityRevision = getRevision(updateResult.getEntityId());
        final EntityModification entityModification = new EntityModification(updatedEntityRevision, updaterIdentity);

        return new StandardRevisionUpdate<>(updatedEntity, entityModification, incrementedRevisions);
    }

    /*
     * Issue an update that increments the version, but only if the incoming version OR client id match the existing revision.
     *
     * If no rows were updated, then the incoming revision is stale and an exception is thrown.
     *
     * If a row was updated, then the incoming revision is good and that row is no locked in the DB, and we can proceed.
     */
    private void updateRevision(final Revision incomingRevision) {
        final String sql =
                "UPDATE REVISION SET " +
                        "VERSION = (VERSION + 1), " +
                        "CLIENT_ID = ? " +
                "WHERE " +
                        "ENTITY_ID = ? AND (" +
                        "VERSION = ? OR CLIENT_ID = ? " +
                ")";

        final String entityId = incomingRevision.getEntityId();
        final String clientId = incomingRevision.getClientId();
        final Long version = incomingRevision.getVersion();

        final int rowsUpdated = jdbcTemplate.update(sql, clientId, entityId, version, clientId);
        if (rowsUpdated <= 0) {
            throw new InvalidRevisionException("Invalid Revision was given for entity with ID '" + entityId + "'");
        }
    }

    @Override
    public <T> T deleteRevision(final RevisionClaim claim, final DeleteRevisionTask<T> task)
            throws ExpiredRevisionClaimException {
        LOGGER.debug("Attempting to delete revision using {}", claim);

        final List<Revision> revisionList = new ArrayList<>(claim.getRevisions());
        revisionList.sort(new RevisionComparator());

        // Issue the delete for each revision
        // Since we are in transaction these changes won't be committed unless the entire task completes successfully.
        // It is important this happens first so that the task won't execute unless the revision can be deleted.
        // This prevents any other changes from happening that might not be part of the database transaction.
        for (final Revision revision : revisionList) {
            deleteRevision(revision);
        }

        // Perform the action provided
        final T taskResult = task.performTask();
        LOGGER.debug("Delete task completed");

        return taskResult;
    }

    /*
     * Issue a delete for a revision of a given entity, but only if the incoming version OR client id match the existing revision.
     *
     * If no rows were updated, then the incoming revision is stale and an exception is thrown.
     *
     * If a row was deleted, then the incoming revision is good and that row is no locked in the DB, and we can proceed.
     */
    private void deleteRevision(final Revision revision) {
        final String sql =
                "DELETE FROM REVISION WHERE " +
                        "ENTITY_ID = ? AND (" +
                            "VERSION = ? OR CLIENT_ID = ? " +
                        ")";

        final String entityId = revision.getEntityId();
        final String clientId = revision.getClientId();
        final Long version = revision.getVersion();

        final int rowsUpdated = jdbcTemplate.update(sql, entityId, version, clientId);
        if (rowsUpdated <= 0) {
            throw new ExpiredRevisionClaimException("Invalid Revision was given for entity with ID '" + entityId + "'");
        }
    }

    @Override
    public void reset(final Collection<Revision> revisions) {
        // delete all revisions
        jdbcTemplate.update("DELETE FROM REVISION");

        // insert all the provided revisions
        final String insertSql = "INSERT INTO REVISION(ENTITY_ID, VERSION, CLIENT_ID) VALUES (?, ?, ?)";
        for (final Revision revision : revisions) {
            jdbcTemplate.update(insertSql, revision.getEntityId(), revision.getVersion(), revision.getClientId());
        }
    }

    @Override
    public List<Revision> getAllRevisions() {
        return jdbcTemplate.query("SELECT * FROM REVISION", new RevisionRowMapper());
    }

    @Override
    public Map<String, Revision> getRevisionMap() {
        final Map<String,Revision> revisionMap = new HashMap<>();
        final RevisionRowMapper rowMapper = new RevisionRowMapper();

        jdbcTemplate.query("SELECT * FROM REVISION", (rs) -> {
            final Revision revision = rowMapper.mapRow(rs, 0);
            revisionMap.put(revision.getEntityId(), revision);
        });

        return revisionMap;
    }
}
