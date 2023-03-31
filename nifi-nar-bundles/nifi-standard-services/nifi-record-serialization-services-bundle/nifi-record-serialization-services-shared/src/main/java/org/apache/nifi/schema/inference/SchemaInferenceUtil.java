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

import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyDescriptor.Builder;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaAccessStrategy;
import org.apache.nifi.serialization.RecordSchemaCacheService;

import java.util.function.Supplier;

public class SchemaInferenceUtil {
    public static final AllowableValue INFER_SCHEMA = new AllowableValue("infer-schema", "Infer Schema",
        "The Schema of the data will be inferred automatically when the data is read. See component Usage and Additional Details for information about how the schema is inferred.");

    public static final PropertyDescriptor SCHEMA_CACHE = new Builder()
        .name("schema-inference-cache")
        .displayName("Schema Inference Cache")
        .description("Specifies a Schema Cache to use when inferring the schema. If not populated, the schema will be inferred each time. " +
            "However, if a cache is specified, the cache will first be consulted and if the applicable schema can be found, it will be used instead of inferring the schema.")
        .required(false)
        .identifiesControllerService(RecordSchemaCacheService.class)
        .build();


    public static <T> SchemaAccessStrategy getSchemaAccessStrategy(final String strategy, final PropertyContext context,  final ComponentLog logger,
                                                                   final RecordSourceFactory<T> recordSourceFactory, final Supplier<SchemaInferenceEngine<T>> inferenceSupplier,
                                                                   final Supplier<SchemaAccessStrategy> defaultSupplier) {
        if (INFER_SCHEMA.getValue().equalsIgnoreCase(strategy)) {
            final SchemaAccessStrategy inferenceStrategy = new InferSchemaAccessStrategy<>(recordSourceFactory, inferenceSupplier.get(), logger);
            final RecordSchemaCacheService schemaCache = context.getProperty(SCHEMA_CACHE).asControllerService(RecordSchemaCacheService.class);
            if (schemaCache == null) {
                return inferenceStrategy;
            }

            return new CachedSchemaAccessStrategy(schemaCache, inferenceStrategy, logger);
        }

        return defaultSupplier.get();
    }

}
