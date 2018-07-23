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
package org.apache.nifi.schemaregistry.hortonworks;

import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaField;
import org.apache.nifi.schemaregistry.services.SchemaRegistry;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.apache.nifi.util.Tuple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

@Tags({"schema", "registry", "avro", "hortonworks", "hwx"})
@CapabilityDescription("Provides a Schema Registry Service that interacts with a Hortonworks Schema Registry, available at https://github.com/hortonworks/registry")
public class HortonworksSchemaRegistry extends AbstractControllerService implements SchemaRegistry {
    private static final Set<SchemaField> schemaFields = EnumSet.of(SchemaField.SCHEMA_NAME, SchemaField.SCHEMA_BRANCH_NAME, SchemaField.SCHEMA_TEXT,
        SchemaField.SCHEMA_TEXT_FORMAT, SchemaField.SCHEMA_IDENTIFIER, SchemaField.SCHEMA_VERSION);

    private final ConcurrentMap<Tuple<SchemaIdentifier, String>, RecordSchema> schemaNameToSchemaMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<Tuple<String,String>, Tuple<SchemaVersionInfo, Long>> schemaVersionByNameCache = new ConcurrentHashMap<>();
    private final ConcurrentMap<SchemaVersionKey, Tuple<SchemaVersionInfo, Long>> schemaVersionByKeyCache = new ConcurrentHashMap<>();

    private volatile long versionInfoCacheNanos;

    static final PropertyDescriptor URL = new PropertyDescriptor.Builder()
        .name("url")
        .displayName("Schema Registry URL")
        .description("URL of the schema registry that this Controller Service should connect to, including version. For example, http://localhost:9090/api/v1")
        .addValidator(StandardValidators.URL_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .required(true)
        .build();

    static final PropertyDescriptor CACHE_SIZE = new PropertyDescriptor.Builder()
        .name("cache-size")
        .displayName("Cache Size")
        .description("Specifies how many Schemas should be cached from the Hortonworks Schema Registry")
        .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
        .defaultValue("1000")
        .required(true)
        .build();

    static final PropertyDescriptor CACHE_EXPIRATION = new PropertyDescriptor.Builder()
        .name("cache-expiration")
        .displayName("Cache Expiration")
        .description("Specifies how long a Schema that is cached should remain in the cache. Once this time period elapses, a "
            + "cached version of a schema will no longer be used, and the service will have to communicate with the "
            + "Hortonworks Schema Registry again in order to obtain the schema.")
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .defaultValue("1 hour")
        .required(true)
        .build();

    private volatile SchemaRegistryClient schemaRegistryClient;
    private volatile boolean initialized;
    private volatile Map<String, Object> schemaRegistryConfig;


    @OnEnabled
    public void enable(final ConfigurationContext context) throws InitializationException {
        schemaRegistryConfig = new HashMap<>();

        versionInfoCacheNanos = context.getProperty(CACHE_EXPIRATION).asTimePeriod(TimeUnit.NANOSECONDS);

        // The below properties may or may not need to be exposed to the end
        // user. We just need to watch usage patterns to see if sensible default
        // can satisfy NiFi requirements
        String urlValue = context.getProperty(URL).evaluateAttributeExpressions().getValue();
        if (urlValue == null || urlValue.trim().isEmpty()) {
            throw new IllegalArgumentException("'Schema Registry URL' must not be null or empty.");
        }

        schemaRegistryConfig.put(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), urlValue);
        schemaRegistryConfig.put(SchemaRegistryClient.Configuration.CLASSLOADER_CACHE_SIZE.name(), 10L);
        schemaRegistryConfig.put(SchemaRegistryClient.Configuration.CLASSLOADER_CACHE_EXPIRY_INTERVAL_SECS.name(), context.getProperty(CACHE_EXPIRATION).asTimePeriod(TimeUnit.SECONDS));
        schemaRegistryConfig.put(SchemaRegistryClient.Configuration.SCHEMA_VERSION_CACHE_SIZE.name(), context.getProperty(CACHE_SIZE).asInteger());
        schemaRegistryConfig.put(SchemaRegistryClient.Configuration.SCHEMA_VERSION_CACHE_EXPIRY_INTERVAL_SECS.name(), context.getProperty(CACHE_EXPIRATION).asTimePeriod(TimeUnit.SECONDS));
    }



    @OnDisabled
    public void close() {
        if (schemaRegistryClient != null) {
            schemaRegistryClient.close();
        }

        initialized = false;
    }


    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(URL);
        properties.add(CACHE_SIZE);
        properties.add(CACHE_EXPIRATION);
        return properties;
    }


    protected synchronized SchemaRegistryClient getClient() {
        if (!initialized) {
            schemaRegistryClient = new SchemaRegistryClient(schemaRegistryConfig);
            initialized = true;
        }

        return schemaRegistryClient;
    }


    private SchemaVersionInfo getLatestSchemaVersionInfo(final SchemaRegistryClient client, final String schemaName, final String branchName)
            throws org.apache.nifi.schema.access.SchemaNotFoundException {
        try {
            // Try to fetch the SchemaVersionInfo from the cache.
            final Tuple<String,String> nameAndBranch = new Tuple<>(schemaName, branchName);
            final Tuple<SchemaVersionInfo, Long> timestampedVersionInfo = schemaVersionByNameCache.get(nameAndBranch);

            // Determine if the timestampedVersionInfo is expired
            boolean fetch = false;
            if (timestampedVersionInfo == null) {
                fetch = true;
            } else {
                final long minTimestamp = System.nanoTime() - versionInfoCacheNanos;
                fetch = timestampedVersionInfo.getValue() < minTimestamp;
            }

            // If not expired, use what we got from the cache
            if (!fetch) {
                return timestampedVersionInfo.getKey();
            }

            // schema version info was expired or not found in cache. Fetch from schema registry
            final SchemaVersionInfo versionInfo;
            if (StringUtils.isBlank(branchName)) {
                versionInfo = client.getLatestSchemaVersionInfo(schemaName);
            } else {
                versionInfo = client.getLatestSchemaVersionInfo(branchName, schemaName);
            }

            if (versionInfo == null) {
                throw new org.apache.nifi.schema.access.SchemaNotFoundException("Could not find schema with name '" + schemaName + "'");
            }

            // Store new version in cache.
            final Tuple<SchemaVersionInfo, Long> tuple = new Tuple<>(versionInfo, System.nanoTime());
            schemaVersionByNameCache.put(nameAndBranch, tuple);
            return versionInfo;
        } catch (final SchemaNotFoundException e) {
            throw new org.apache.nifi.schema.access.SchemaNotFoundException(e);
        }
    }

    private SchemaVersionInfo getSchemaVersionInfo(final SchemaRegistryClient client, final SchemaVersionKey key) throws org.apache.nifi.schema.access.SchemaNotFoundException {
        try {
            // Try to fetch the SchemaVersionInfo from the cache.
            final Tuple<SchemaVersionInfo, Long> timestampedVersionInfo = schemaVersionByKeyCache.get(key);

            // Determine if the timestampedVersionInfo is expired
            boolean fetch = false;
            if (timestampedVersionInfo == null) {
                fetch = true;
            } else {
                final long minTimestamp = System.nanoTime() - versionInfoCacheNanos;
                fetch = timestampedVersionInfo.getValue() < minTimestamp;
            }

            // If not expired, use what we got from the cache
            if (!fetch) {
                return timestampedVersionInfo.getKey();
            }

            // schema version info was expired or not found in cache. Fetch from schema registry
            final SchemaVersionInfo versionInfo = client.getSchemaVersionInfo(key);
            if (versionInfo == null) {
                throw new org.apache.nifi.schema.access.SchemaNotFoundException("Could not find schema with name '" + key.getSchemaName() + "' and version " + key.getVersion());
            }

            // Store new version in cache.
            final Tuple<SchemaVersionInfo, Long> tuple = new Tuple<>(versionInfo, System.nanoTime());
            schemaVersionByKeyCache.put(key, tuple);
            return versionInfo;
        } catch (final SchemaNotFoundException e) {
            throw new org.apache.nifi.schema.access.SchemaNotFoundException(e);
        }
    }

    private RecordSchema retrieveSchemaByName(final SchemaIdentifier schemaIdentifier) throws org.apache.nifi.schema.access.SchemaNotFoundException, IOException {

        final SchemaRegistryClient client = getClient();

        final SchemaVersionInfo versionInfo;
        final Long schemaId;

        final Optional<String> schemaName = schemaIdentifier.getName();
        if (!schemaName.isPresent()) {
            throw new org.apache.nifi.schema.access.SchemaNotFoundException("Cannot retrieve schema because Schema Name is not present");
        }

        final Optional<String> schemaBranchName = schemaIdentifier.getBranch();
        final OptionalInt schemaVersion = schemaIdentifier.getVersion();

        try {
            final SchemaMetadataInfo metadataInfo = client.getSchemaMetadataInfo(schemaName.get());
            if (metadataInfo == null) {
                throw new org.apache.nifi.schema.access.SchemaNotFoundException("Could not find schema with name '" + schemaName + "'");
            }

            schemaId = metadataInfo.getId();
            if (schemaId == null) {
                throw new org.apache.nifi.schema.access.SchemaNotFoundException("Could not find schema with name '" + schemaName + "'");
            }

            // possible scenarios are name only, name + branch, or name + version
            if (schemaVersion.isPresent()) {
                final SchemaVersionKey schemaVersionKey = new SchemaVersionKey(schemaName.get(), schemaVersion.getAsInt());
                versionInfo = getSchemaVersionInfo(client, schemaVersionKey);
            } else {
                versionInfo = getLatestSchemaVersionInfo(client, schemaName.get(), schemaBranchName.orElse(null));
            }

            if (versionInfo == null || versionInfo.getVersion() == null) {
                final String message = createErrorMessage("Could not find schema", schemaName, schemaBranchName, schemaVersion);
                throw new org.apache.nifi.schema.access.SchemaNotFoundException(message);
            }

        } catch (final Exception e) {
            final String message = createErrorMessage("Failed to retrieve schema", schemaName, schemaBranchName, schemaVersion);
            handleException(message, e);
            return null;
        }

        final String schemaText = versionInfo.getSchemaText();

        final SchemaIdentifier resultSchemaIdentifier = SchemaIdentifier.builder()
                .id(schemaId)
                .name(schemaName.get())
                .branch(schemaBranchName.orElse(null))
                .version(versionInfo.getVersion())
                .build();

        final Tuple<SchemaIdentifier, String> tuple = new Tuple<>(resultSchemaIdentifier, schemaText);
        return schemaNameToSchemaMap.computeIfAbsent(tuple, t -> {
            final Schema schema = new Schema.Parser().parse(schemaText);
            return AvroTypeUtil.createSchema(schema, schemaText, resultSchemaIdentifier);
        });
    }

    private RecordSchema retrieveSchemaByIdAndVersion(final SchemaIdentifier schemaIdentifier) throws org.apache.nifi.schema.access.SchemaNotFoundException, IOException {
        final SchemaRegistryClient client = getClient();

        final String schemaName;
        final SchemaVersionInfo versionInfo;

        final OptionalLong schemaId = schemaIdentifier.getIdentifier();
        if (!schemaId.isPresent()) {
            throw new org.apache.nifi.schema.access.SchemaNotFoundException("Cannot retrieve schema because Schema Id is not present");
        }

        final OptionalInt version = schemaIdentifier.getVersion();
        if (!version.isPresent()) {
            throw new org.apache.nifi.schema.access.SchemaNotFoundException("Cannot retrieve schema because Schema Version is not present");
        }

        try {
            final SchemaMetadataInfo info = client.getSchemaMetadataInfo(schemaId.getAsLong());
            if (info == null) {
                throw new org.apache.nifi.schema.access.SchemaNotFoundException("Could not find schema with ID '" + schemaId + "' and version '" + version + "'");
            }

            final SchemaMetadata metadata = info.getSchemaMetadata();
            schemaName = metadata.getName();

            final SchemaVersionKey schemaVersionKey = new SchemaVersionKey(schemaName, version.getAsInt());
            versionInfo = getSchemaVersionInfo(client, schemaVersionKey);
            if (versionInfo == null) {
                throw new org.apache.nifi.schema.access.SchemaNotFoundException("Could not find schema with ID '" + schemaId + "' and version '" + version + "'");
            }
        } catch (final Exception e) {
            handleException("Failed to retrieve schema with ID '" + schemaId + "' and version '" + version + "'", e);
            return null;
        }

        final String schemaText = versionInfo.getSchemaText();

        final SchemaIdentifier resultSchemaIdentifier = SchemaIdentifier.builder()
                .name(schemaName)
                .id(schemaId.getAsLong())
                .version(version.getAsInt())
                .build();

        final Tuple<SchemaIdentifier, String> tuple = new Tuple<>(resultSchemaIdentifier, schemaText);
        return schemaNameToSchemaMap.computeIfAbsent(tuple, t -> {
            final Schema schema = new Schema.Parser().parse(schemaText);
            return AvroTypeUtil.createSchema(schema, schemaText, resultSchemaIdentifier);
        });
    }

    @Override
    public RecordSchema retrieveSchema(final SchemaIdentifier schemaIdentifier) throws IOException, org.apache.nifi.schema.access.SchemaNotFoundException {
        if (schemaIdentifier.getIdentifier().isPresent()) {
            return retrieveSchemaByIdAndVersion(schemaIdentifier);
        } else {
            return retrieveSchemaByName(schemaIdentifier);
        }
    }

    private String createErrorMessage(final String baseMessage, final Optional<String> schemaName, final Optional<String> branchName, final OptionalInt version) {
        final StringBuilder builder = new StringBuilder(baseMessage)
                .append(" with name '")
                .append(schemaName.orElse("null"))
                .append("'");

        if (branchName.isPresent()) {
            builder.append(" and branch '").append(branchName.get()).append("'");
        }

        if (version.isPresent()) {
            builder.append(" and version '").append(version.getAsInt()).append("'");
        }

        return builder.toString();
    }

    // The schema registry client wraps all IOExceptions in RuntimeException. So if an IOException occurs, we don't know
    // that it was an IO problem. So we will look through the Exception's cause chain to see if there is an IOException present.
    private void handleException(final String message, final Exception e) throws IOException, org.apache.nifi.schema.access.SchemaNotFoundException {
        if (containsIOException(e)) {
            throw new IOException(message, e);
        }

        throw new org.apache.nifi.schema.access.SchemaNotFoundException(message, e);
    }

    private boolean containsIOException(final Throwable t) {
        if (t == null) {
            return false;
        }

        if (t instanceof IOException) {
            return true;
        }

        final Throwable cause = t.getCause();
        if (cause == null) {
            return false;
        }

        return containsIOException(cause);
    }

    @Override
    public Set<SchemaField> getSuppliedSchemaFields() {
        return schemaFields;
    }
}
