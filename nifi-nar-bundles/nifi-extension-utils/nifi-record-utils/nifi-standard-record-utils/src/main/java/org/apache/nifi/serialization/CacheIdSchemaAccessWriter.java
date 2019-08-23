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

import org.apache.nifi.schema.access.NopSchemaAccessWriter;
import org.apache.nifi.schema.access.SchemaAccessWriter;
import org.apache.nifi.schema.access.SchemaField;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class CacheIdSchemaAccessWriter implements SchemaAccessWriter {
    private final RecordSchemaCacheService cache;
    private final SchemaAccessWriter delegate;

    public CacheIdSchemaAccessWriter(final RecordSchemaCacheService cache, final SchemaAccessWriter delegate) {
        this.cache = cache;
        this.delegate = delegate == null ? new NopSchemaAccessWriter() : delegate;
    }

    @Override
    public void writeHeader(final RecordSchema schema, final OutputStream out) throws IOException {
        delegate.writeHeader(schema, out);
    }

    @Override
    public Map<String, String> getAttributes(final RecordSchema schema) {
        final Map<String, String> attributes = new HashMap<>(delegate.getAttributes(schema));
        final String identifier = cache.cacheSchema(schema);
        attributes.put(RecordSchemaCacheService.CACHE_IDENTIFIER_ATTRIBUTE, identifier);
        return attributes;
    }

    @Override
    public void validateSchema(final RecordSchema schema) throws SchemaNotFoundException {
        delegate.validateSchema(schema);
    }

    @Override
    public Set<SchemaField> getRequiredSchemaFields() {
        return delegate.getRequiredSchemaFields();
    }
}
