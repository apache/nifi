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
package org.apache.nifi.schema.inference;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaAccessStrategy;
import org.apache.nifi.schema.access.SchemaField;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSchemaCacheService;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.io.InputStream;
import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class CachedSchemaAccessStrategy implements SchemaAccessStrategy {
    private final RecordSchemaCacheService schemaCacheService;
    private final SchemaAccessStrategy backupStrategy;
    private final ComponentLog logger;

    public CachedSchemaAccessStrategy(final RecordSchemaCacheService schemaCacheService, final SchemaAccessStrategy backupStrategy, final ComponentLog logger) {
        this.schemaCacheService = schemaCacheService;
        this.backupStrategy = backupStrategy;
        this.logger = logger;
    }

    @Override
    public RecordSchema getSchema(final Map<String, String> variables, final InputStream contentStream, final RecordSchema readSchema) throws SchemaNotFoundException, IOException {
        final String cacheIdentifier = variables.get(RecordSchemaCacheService.CACHE_IDENTIFIER_ATTRIBUTE);
        if (cacheIdentifier == null) {
            logger.debug("Cache Identifier not found. Will delegate to backup Schema Access Strategy");
            return backupStrategy.getSchema(variables, contentStream, readSchema);
        }

        final Optional<RecordSchema> schemaOption = schemaCacheService.getSchema(cacheIdentifier);
        if (schemaOption.isPresent()) {
            logger.debug("Found Cached Record Schema with identifier {}", new Object[] {cacheIdentifier});
            return schemaOption.get();
        }

        logger.debug("Encountered Cache Miss with identifier {}. Will delegate to backup Schema Access Strategy", new Object[] {cacheIdentifier});
        return backupStrategy.getSchema(variables, contentStream, readSchema);
    }

    @Override
    public Set<SchemaField> getSuppliedSchemaFields() {
        return EnumSet.noneOf(SchemaField.class);
    }
}
