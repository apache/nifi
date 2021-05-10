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

/**
 * An entity that supports revision tracking.
 */
public interface RevisableEntity {

    /**
     * @return the identifier for the entity
     */
    String getIdentifier();

    /**
     * Sets the identifier for the entity.
     *
     * @param identifier the identifier
     */
    void setIdentifier(String identifier);

    /**
     * @return the revision information for the entity
     */
    RevisionInfo getRevision();

    /**
     * Sets the revision info for the entity.
     *
     * @param revision the revision info
     */
    void setRevision(RevisionInfo revision);

}
