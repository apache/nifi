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
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.stream.io.NonCloseableInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

public class InferSchemaAccessStrategy<T> implements SchemaAccessStrategy {
    private final RecordSourceFactory<T> recordSourceFactory;
    private final SchemaInferenceEngine<T> schemaInference;
    private final ComponentLog logger;

    public InferSchemaAccessStrategy(final RecordSourceFactory<T> recordSourceFactory, final SchemaInferenceEngine<T> schemaInference, final ComponentLog logger) {
        this.recordSourceFactory = recordSourceFactory;
        this.schemaInference = schemaInference;
        this.logger = logger;
    }

    @Override
    public RecordSchema getSchema(final Map<String, String> variables, final InputStream contentStream, final RecordSchema readSchema) throws IOException {
        // We expect to be able to mark/reset any length because we expect that the underlying stream here will be a ContentClaimInputStream, which is able to
        // re-read the content regardless of how much data is read.
        contentStream.mark(10_000_000);
        try {
            final RecordSource<T> recordSource = recordSourceFactory.create(variables, new NonCloseableInputStream(contentStream));
            final RecordSchema schema = schemaInference.inferSchema(recordSource);

            logger.debug("Successfully inferred schema {}", new Object[] {schema});
            return schema;
        } finally {
            contentStream.reset();
        }
    }

    @Override
    public Set<SchemaField> getSuppliedSchemaFields() {
        return EnumSet.noneOf(SchemaField.class);
    }
}
