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

import avro.shaded.com.google.common.annotations.VisibleForTesting;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyDescriptor.Builder;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.serialization.RecordSchemaCacheService;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.RecordDataType;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.apache.nifi.expression.ExpressionLanguageScope.VARIABLE_REGISTRY;
import static org.apache.nifi.processor.util.StandardValidators.POSITIVE_INTEGER_VALIDATOR;

@CapabilityDescription("Provides a Schema Cache that evicts elements based on a Least-Recently-Used algorithm. This cache is not persisted, so any restart of NiFi will result in " +
    "the cache being cleared. Additionally, the cache will be cleared any time that the Controller Service is stopped and restarted.")
@Tags({"record", "schema", "cache"})
public class VolatileSchemaCache extends AbstractControllerService implements RecordSchemaCacheService {

    static final PropertyDescriptor MAX_SIZE = new Builder()
        .name("max-cache-size")
        .displayName("Maximum Cache Size")
        .description("The maximum number of Schemas to cache.")
        .required(true)
        .addValidator(POSITIVE_INTEGER_VALIDATOR)
        .expressionLanguageSupported(VARIABLE_REGISTRY)
        .defaultValue("100")
        .build();

    private static final Base64.Encoder ENCODER = Base64.getEncoder().withoutPadding();

    private volatile Cache<String, RecordSchema> cache;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Collections.singletonList(MAX_SIZE);
    }

    @OnEnabled
    public void setup(final ConfigurationContext context) {
        final int maxSize = context.getProperty(MAX_SIZE).evaluateAttributeExpressions().asInteger();

        cache = Caffeine.newBuilder()
            .maximumSize(maxSize)
            .build();
    }


    @Override
    public String cacheSchema(final RecordSchema schema) {
        final String identifier = createIdentifier(schema);
        final RecordSchema existingSchema = cache.get(identifier, id -> schema);

        if (existingSchema == null) {
            // We successfully inserted into the cache.
            getLogger().debug("Successfully cached schema with ID {} (no existing schema with this ID)", new Object[] {identifier});
            return identifier;
        }

        // There was already a Schema in the cache with that identifier.
        if (existingSchema.equals(schema)) {
            // Schemas match. Already cached successfully.
            getLogger().debug("Successfully cached schema with ID {} (existing schema with this ID was equal)", new Object[] {identifier});
            return identifier;
        }

        // Schemas hashed to same value but do not equal one another. Append a randomly generated UUID
        // and add that to the cache.
        final String updatedIdentifier = identifier + "-" + UUID.randomUUID().toString();
        cache.put(updatedIdentifier, schema);

        getLogger().debug("Schema with ID {} conflicted with new Schema. Resolved by using generated identifier {}", new Object[] {identifier, updatedIdentifier});
        return updatedIdentifier;
    }

    @Override
    public Optional<RecordSchema> getSchema(final String schemaIdentifier) {
        final RecordSchema cachedSchema = cache.getIfPresent(schemaIdentifier);
        return Optional.ofNullable(cachedSchema);
    }

    @VisibleForTesting
    protected String createIdentifier(final RecordSchema schema) {
        final MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new AssertionError(e);
        }

        final Optional<String> suppliedText = schema.getSchemaText();
        if (suppliedText.isPresent()) {
            digest.update(suppliedText.get().getBytes(StandardCharsets.UTF_8));
        } else {
            computeHash(schema, digest);
        }

        final byte[] digestBytes = digest.digest();

        return ENCODER.encodeToString(digestBytes);
    }

    private void computeHash(final RecordSchema schema, final MessageDigest digest) {
        for (final RecordField field : schema.getFields()) {
            digest.update(field.getFieldName().getBytes(StandardCharsets.UTF_8));

            final DataType dataType = field.getDataType();
            final RecordFieldType fieldType = dataType.getFieldType();
            digest.update(fieldType.name().getBytes(StandardCharsets.UTF_8));

            final String format = dataType.getFormat();
            if (format != null) {
                digest.update(format.getBytes(StandardCharsets.UTF_8));
            }

            if (fieldType == RecordFieldType.RECORD) {
                final RecordSchema childSchema = ((RecordDataType) dataType).getChildSchema();
                if (childSchema != null) {
                    computeHash(childSchema, digest);
                }
            }
        }
    }
}
