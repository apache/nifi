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

import org.apache.nifi.registry.revision.api.EntityModification;
import org.apache.nifi.registry.revision.api.Revision;
import org.apache.nifi.registry.revision.api.RevisionClaim;
import org.apache.nifi.registry.revision.api.RevisionManager;
import org.apache.nifi.registry.revision.api.RevisionUpdate;
import org.apache.nifi.registry.revision.standard.StandardRevisionClaim;
import org.apache.nifi.registry.revision.standard.StandardUpdateResult;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Standard implementation of RevisableEntityService.
 */
public class StandardRevisableEntityService implements RevisableEntityService {

    private final RevisionManager revisionManager;

    public StandardRevisableEntityService(final RevisionManager revisionManager) {
        this.revisionManager = revisionManager;
    }

    @Override
    public <T extends RevisableEntity> T create(final T requestEntity, final String creatorIdentity, final Supplier<T> createEntity) {
        if (requestEntity == null) {
            throw new IllegalArgumentException("Request entity is required");
        }

        if (requestEntity.getRevision() == null || requestEntity.getRevision().getVersion() == null) {
            throw new IllegalArgumentException("Revision info is required");
        }

        if (requestEntity.getRevision().getVersion() != 0) {
            throw new IllegalArgumentException("A revision version of 0 must be specified when creating a new entity");
        }

        return createOrUpdate(requestEntity, creatorIdentity, createEntity);
    }

    @Override
    public <T extends RevisableEntity> T get(final Supplier<T> getEntity) {
        final T entity = getEntity.get();
        if (entity != null) {
            populateRevision(entity);
        }
        return entity;
    }

    @Override
    public <T extends RevisableEntity> List<T> getEntities(final Supplier<List<T>> getEntities) {
        final List<T> entities = getEntities.get();
        populateRevisableEntityRevisions(entities);
        return entities;
    }

    @Override
    public <T extends RevisableEntity> T update(final T requestEntity, final String updaterIdentity, final Supplier<T> updateEntity) {
        return createOrUpdate(requestEntity, updaterIdentity, updateEntity);
    }

    private <T extends RevisableEntity> T createOrUpdate(final T requestEntity, final String userIdentity, final Supplier<T> createOrUpdateEntity) {
        if (requestEntity == null) {
            throw new IllegalArgumentException("Request entity is required");
        }

        if (requestEntity.getRevision() == null || requestEntity.getRevision().getVersion() == null) {
            throw new IllegalArgumentException("Revision info is required");
        }

        if (userIdentity == null || userIdentity.trim().isEmpty()) {
            throw new IllegalArgumentException("User identity is required");
        }

        final Revision revision = createRevision(requestEntity.getIdentifier(), requestEntity.getRevision());
        final RevisionClaim claim = new StandardRevisionClaim(revision);

        final RevisionUpdate<T> revisionUpdate = revisionManager.updateRevision(claim, () -> {
            final T updatedEntity = createOrUpdateEntity.get();
            return new StandardUpdateResult<>(updatedEntity, updatedEntity.getIdentifier(), userIdentity);
        });

        final T resultEntity = revisionUpdate.getEntity();
        resultEntity.setRevision(createRevisionInfo(revisionUpdate.getLastModification()));
        return resultEntity;
    }

    @Override
    public <T extends RevisableEntity> T delete(final String entityIdentifier, final RevisionInfo revisionInfo, final Supplier<T> deleteEntity) {
        if (entityIdentifier == null || entityIdentifier.trim().isEmpty()) {
            throw new IllegalArgumentException("Entity identifier is required");
        }

        if (revisionInfo == null || revisionInfo.getVersion() == null) {
            throw new IllegalArgumentException("Revision info is required");
        }

        final Revision revision = createRevision(entityIdentifier, revisionInfo);
        final RevisionClaim claim = new StandardRevisionClaim(revision);
        return revisionManager.deleteRevision(claim, () -> deleteEntity.get());
    }

    @Override
    public void populateRevisions(final Collection<?> entities) {
        if (entities == null || entities.isEmpty()) {
            return;
        }

        // Note: This might be inefficient to retrieve all the revisions when there are lots of revisions
        // and only a few entities that we might need revisions for, we could consider allowing a set of
        // entity ids to be passed in, but then we also might end up with a massive OR statement when selecting
        final Map<String,Revision> revisionMap = revisionManager.getRevisionMap();

        for (final Object obj : entities) {
            if (obj instanceof RevisableEntity) {
                populateRevision(revisionMap, (RevisableEntity) obj);
            }
        }
    }

    private <T extends RevisableEntity> void populateRevisableEntityRevisions(final Collection<T> revisableEntities) {
        if (revisableEntities == null) {
            return;
        }

        final Map<String,Revision> revisionMap = revisionManager.getRevisionMap();
        revisableEntities.forEach(e -> {
            populateRevision(revisionMap, e);
        });
    }

    private void populateRevision(final Map<String, Revision> revisionMap, final RevisableEntity revisableEntity) {
        final Revision revision = revisionMap.get(revisableEntity.getIdentifier());
        if (revision != null) {
            final RevisionInfo revisionInfo = createRevisionInfo(revision);
            revisableEntity.setRevision(revisionInfo);
        } else {
            // need to make sure that if there isn't an entry in the map, we call getRevision which will cause a
            // revision to be created in the RevisionManager
            populateRevision(revisableEntity);
        }
    }

    private void populateRevision(final RevisableEntity e) {
        if (e == null) {
            return;
        }

        // get or create the revision
        final Revision entityRevision = revisionManager.getRevision(e.getIdentifier());
        final RevisionInfo revisionInfo = createRevisionInfo(entityRevision);
        e.setRevision(revisionInfo);
    }

    private Revision createRevision(final String entityId, final RevisionInfo revisionInfo) {
        return new Revision(revisionInfo.getVersion(), revisionInfo.getClientId(), entityId);
    }

    private RevisionInfo createRevisionInfo(final Revision revision) {
        return createRevisionInfo(revision, null);
    }

    private RevisionInfo createRevisionInfo(final EntityModification entityModification) {
        return createRevisionInfo(entityModification.getRevision(), entityModification);
    }

    private RevisionInfo createRevisionInfo(final Revision revision, final EntityModification entityModification) {
        final RevisionInfo revisionInfo = new RevisionInfo();
        revisionInfo.setVersion(revision.getVersion());
        revisionInfo.setClientId(revision.getClientId());
        if (entityModification != null) {
            revisionInfo.setLastModifier(entityModification.getLastModifier());
        }
        return revisionInfo;
    }

}
