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
package org.apache.nifi.registry.revision.standard;

import org.apache.nifi.registry.revision.api.UpdateResult;

public class StandardUpdateResult<T> implements UpdateResult<T> {

    private final T entity;
    private final String entityId;
    private final String updaterIdentity;

    public StandardUpdateResult(final T entity, final String entityId, final String updaterIdentity) {
        this.entity = entity;
        this.entityId = entityId;
        this.updaterIdentity = updaterIdentity;

        if (this.entity == null) {
            throw new IllegalArgumentException("Entity is required");
        }

        if (this.entityId == null || this.entityId.trim().isEmpty()) {
            throw new IllegalArgumentException("Entity id is required");
        }

        if (this.updaterIdentity == null || this.updaterIdentity.trim().isEmpty()) {
            throw new IllegalArgumentException("Updater identity is required");
        }
    }

    @Override
    public T getEntity() {
        return entity;
    }

    @Override
    public String getEntityId() {
        return entityId;
    }

    @Override
    public String updaterIdentity() {
        return updaterIdentity;
    }
}
