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

import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

/**
 * A service to perform CRUD operations on a RevisableEntity.
 */
public interface RevisableEntityService {

    /**
     * Creates an entity using the RevisionManager.
     *
     * @param requestEntity the entity to create
     * @param creatorIdentity the identity of the user performing the create operation
     * @param createEntity a function that creates the entity and returns the created reference
     * @param <T> the type of RevisableEntity
     * @return the created entity
     */
    <T extends RevisableEntity> T create(T requestEntity, String creatorIdentity, Supplier<T> createEntity);

    /**
     * Retrieves a RevisableEntity and populates the RevisionInfo.
     *
     * @param getEntity a function that retrieves an entity
     * @param <T> the type of RevisableEntity
     * @return the retrieved entity
     */
    <T extends RevisableEntity> T get(Supplier<T> getEntity);

    /**
     * Retrieves a List of RevisableEntity instances and populates the RevisionInfo.
     *
     * @param getEntities a function that retrieves a list of entities
     * @param <T> the type of RevisableEntity
     * @return the list of retrieved entity
     */
    <T extends RevisableEntity> List<T> getEntities(Supplier<List<T>> getEntities);

    /**
     * Updates a RevisableEntity using the RevisionManager.
     *
     * @param requestEntity the entity to update
     * @param updaterIdentity the identity of the user performing the update operation
     * @param updateEntity a function that updates the entity and returns the updated reference
     * @param <T> the type of RevisableEntity
     * @return the updated entity
     */
    <T extends RevisableEntity> T update(T requestEntity, String updaterIdentity, Supplier<T> updateEntity);

    /**
     * Deletes a RevisableEntity using the RevisionManager.
     *
     * @param entityIdentifier the identifier of the entity to delete
     * @param revisionInfo the RevisionInfo for the entity to delete
     * @param deleteEntity a function that deletes the entity and returns the deleted reference
     * @param <T> the type of RevisableEntity
     * @return the deleted entity
     */
    <T extends RevisableEntity> T delete(String entityIdentifier, RevisionInfo revisionInfo, Supplier<T> deleteEntity);

    /**
     * Populates RevisionInfo on any objects in the collection that implement RevisableEntity.
     *
     * @param entities the entities collection which may contain one or more RevisableEntity instances
     */
    void populateRevisions(Collection<?> entities);

}
