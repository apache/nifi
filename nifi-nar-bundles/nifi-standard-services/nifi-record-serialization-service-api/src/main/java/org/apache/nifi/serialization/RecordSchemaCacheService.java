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
package org.apache.nifi.serialization;

import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.serialization.record.RecordSchema;

import java.util.Optional;

public interface RecordSchemaCacheService extends ControllerService {
    public static final String CACHE_IDENTIFIER_ATTRIBUTE = "schema.cache.identifier";

    /**
     * Updates the cache to include the given Record Schema and returns an identifier
     * for the Schema. If the schema already exists in the cache, the existing identifier
     * is returned. Otherwise, the schema is added to the cache and a new identifier is
     * created and returned. This identifier can then be used to retrieve the Record Schema
     * via the {@link #getSchema(String)} method
     *
     * @param schema the schema to cache
     * @return a unique identifier for the schema
     */
    String cacheSchema(RecordSchema schema);

    /**
     * Returns the Schema with the given identifier, if it can be found in the cache.
     * Note that the cache may choose to evict schemas for any number of reasons and, as such,
     * the service may return an empty Optional even immediately after the Schema is cached
     * via the {@link #cacheSchema(RecordSchema)}.
     *
     * @param schemaIdentifier the identifier of the schema
     * @return an Optional holding the Record Schema with the given identifier, if it can be found,
     * or an empty Optional if the schema cannot be found
     */
    Optional<RecordSchema> getSchema(String schemaIdentifier);

}
