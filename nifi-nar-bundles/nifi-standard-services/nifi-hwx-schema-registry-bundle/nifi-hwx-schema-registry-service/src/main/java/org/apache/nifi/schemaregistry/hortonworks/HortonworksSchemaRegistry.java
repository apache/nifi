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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaField;
import org.apache.nifi.schemaregistry.services.SchemaRegistry;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.apache.nifi.util.Tuple;

import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;

@Tags({"schema", "registry", "avro", "hortonworks", "hwx"})
@CapabilityDescription("Provides a Schema Registry Service that interacts with a Hortonworks Schema Registry, available at https://github.com/hortonworks/registry")
public class HortonworksSchemaRegistry extends AbstractControllerService implements SchemaRegistry {
    private static final Set<SchemaField> schemaFields = EnumSet.of(SchemaField.SCHEMA_NAME, SchemaField.SCHEMA_TEXT,
        SchemaField.SCHEMA_TEXT_FORMAT, SchemaField.SCHEMA_IDENTIFIER, SchemaField.SCHEMA_VERSION);
    private final ConcurrentMap<Tuple<SchemaIdentifier, String>, RecordSchema> schemaNameToSchemaMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Tuple<SchemaVersionInfo, Long>> schemaVersionCache = new ConcurrentHashMap<>();

    private static final String LOGICAL_TYPE_DATE = "date";
    private static final String LOGICAL_TYPE_TIME_MILLIS = "time-millis";
    private static final String LOGICAL_TYPE_TIME_MICROS = "time-micros";
    private static final String LOGICAL_TYPE_TIMESTAMP_MILLIS = "timestamp-millis";
    private static final String LOGICAL_TYPE_TIMESTAMP_MICROS = "timestamp-micros";
    private static final long VERSION_INFO_CACHE_NANOS = TimeUnit.MINUTES.toNanos(1L);

    static final PropertyDescriptor URL = new PropertyDescriptor.Builder()
        .name("url")
        .displayName("Schema Registry URL")
        .description("URL of the schema registry that this Controller Service should connect to, including version. For example, http://localhost:9090/api/v1")
        .addValidator(StandardValidators.URL_VALIDATOR)
        .expressionLanguageSupported(true)
        .required(true)
        .build();


    private static final List<PropertyDescriptor> propertyDescriptors = Collections.singletonList(URL);
    private volatile SchemaRegistryClient schemaRegistryClient;
    private volatile boolean initialized;
    private volatile Map<String, Object> schemaRegistryConfig;

    public HortonworksSchemaRegistry() {
    }


    @OnEnabled
    public void enable(final ConfigurationContext context) throws InitializationException {
        schemaRegistryConfig = new HashMap<>();

        // The below properties may or may not need to be exposed to the end
        // user. We just need to watch usage patterns to see if sensible default
        // can satisfy NiFi requirements
        String urlValue = context.getProperty(URL).evaluateAttributeExpressions().getValue();
        if (urlValue == null || urlValue.trim().length() == 0){
            throw new IllegalArgumentException("'Schema Registry URL' must not  be nul or empty.");
        }

        schemaRegistryConfig.put(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), urlValue);
        schemaRegistryConfig.put(SchemaRegistryClient.Configuration.CLASSLOADER_CACHE_SIZE.name(), 10L);
        schemaRegistryConfig.put(SchemaRegistryClient.Configuration.CLASSLOADER_CACHE_EXPIRY_INTERVAL_SECS.name(), 5000L);
        schemaRegistryConfig.put(SchemaRegistryClient.Configuration.SCHEMA_VERSION_CACHE_SIZE.name(), 1000L);
        schemaRegistryConfig.put(SchemaRegistryClient.Configuration.SCHEMA_VERSION_CACHE_EXPIRY_INTERVAL_SECS.name(), 60 * 60 * 1000L);
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
        return propertyDescriptors;
    }


    private synchronized SchemaRegistryClient getClient() {
        if (!initialized) {
            schemaRegistryClient = new SchemaRegistryClient(schemaRegistryConfig);
            initialized = true;
        }

        return schemaRegistryClient;
    }

    private SchemaVersionInfo getLatestSchemaVersionInfo(final SchemaRegistryClient client, final String schemaName) throws org.apache.nifi.schema.access.SchemaNotFoundException {
        try {
            // Try to fetch the SchemaVersionInfo from the cache.
            final Tuple<SchemaVersionInfo, Long> timestampedVersionInfo = schemaVersionCache.get(schemaName);

            // Determine if the timestampedVersionInfo is expired
            boolean fetch = false;
            if (timestampedVersionInfo == null) {
                fetch = true;
            } else {
                final long minTimestamp = System.nanoTime() - VERSION_INFO_CACHE_NANOS;
                fetch = timestampedVersionInfo.getValue() < minTimestamp;
            }

            // If not expired, use what we got from the cache
            if (!fetch) {
                return timestampedVersionInfo.getKey();
            }

            // schema version info was expired or not found in cache. Fetch from schema registry
            final SchemaVersionInfo versionInfo = client.getLatestSchemaVersionInfo(schemaName);
            if (versionInfo == null) {
                throw new org.apache.nifi.schema.access.SchemaNotFoundException("Could not find schema with name '" + schemaName + "'");
            }

            // Store new version in cache.
            final Tuple<SchemaVersionInfo, Long> tuple = new Tuple<>(versionInfo, System.nanoTime());
            schemaVersionCache.put(schemaName, tuple);
            return versionInfo;
        } catch (final SchemaNotFoundException e) {
            throw new org.apache.nifi.schema.access.SchemaNotFoundException(e);
        }
    }

    @Override
    public String retrieveSchemaText(final String schemaName) throws org.apache.nifi.schema.access.SchemaNotFoundException {
        final SchemaVersionInfo latest = getLatestSchemaVersionInfo(getClient(), schemaName);
        return latest.getSchemaText();
    }


    @Override
    public RecordSchema retrieveSchema(final String schemaName) throws org.apache.nifi.schema.access.SchemaNotFoundException {
        final SchemaRegistryClient client = getClient();
        final SchemaMetadataInfo metadataInfo = client.getSchemaMetadataInfo(schemaName);
        if (metadataInfo == null) {
            throw new org.apache.nifi.schema.access.SchemaNotFoundException("Could not find schema with name '" + schemaName + "'");
        }

        final Long schemaId = metadataInfo.getId();
        if (schemaId == null) {
            throw new org.apache.nifi.schema.access.SchemaNotFoundException("Could not find schema with name '" + schemaName + "'");
        }

        final SchemaVersionInfo versionInfo = getLatestSchemaVersionInfo(client, schemaName);
        final Integer version = versionInfo.getVersion();
        if (version == null) {
            throw new org.apache.nifi.schema.access.SchemaNotFoundException("Could not find schema with name '" + schemaName + "'");
        }

        final String schemaText = versionInfo.getSchemaText();
        final SchemaIdentifier schemaIdentifier = (schemaId == null || version == null) ? SchemaIdentifier.ofName(schemaName) : SchemaIdentifier.of(schemaName, schemaId, version);

        final Tuple<SchemaIdentifier, String> tuple = new Tuple<>(schemaIdentifier, schemaText);
        return schemaNameToSchemaMap.computeIfAbsent(tuple, t -> {
            final Schema schema = new Schema.Parser().parse(schemaText);
            return createRecordSchema(schema, schemaText, schemaIdentifier);
        });
    }


    @Override
    public String retrieveSchemaText(final long schemaId, final int version) throws IOException, org.apache.nifi.schema.access.SchemaNotFoundException {
        try {
            final SchemaRegistryClient client = getClient();
            final SchemaMetadataInfo info = client.getSchemaMetadataInfo(schemaId);
            if (info == null) {
                throw new org.apache.nifi.schema.access.SchemaNotFoundException("Could not find schema with ID '" + schemaId + "' and version '" + version + "'");
            }

            final SchemaMetadata metadata = info.getSchemaMetadata();
            final String schemaName = metadata.getName();

            final SchemaVersionKey schemaVersionKey = new SchemaVersionKey(schemaName, version);
            final SchemaVersionInfo versionInfo = client.getSchemaVersionInfo(schemaVersionKey);
            if (versionInfo == null) {
                throw new org.apache.nifi.schema.access.SchemaNotFoundException("Could not find schema with ID '" + schemaId + "' and version '" + version + "'");
            }

            return versionInfo.getSchemaText();
        } catch (final SchemaNotFoundException e) {
            throw new org.apache.nifi.schema.access.SchemaNotFoundException(e);
        }
    }

    @Override
    public RecordSchema retrieveSchema(final long schemaId, final int version) throws IOException, org.apache.nifi.schema.access.SchemaNotFoundException {
        try {
            final SchemaRegistryClient client = getClient();
            final SchemaMetadataInfo info = client.getSchemaMetadataInfo(schemaId);
            if (info == null) {
                throw new org.apache.nifi.schema.access.SchemaNotFoundException("Could not find schema with ID '" + schemaId + "' and version '" + version + "'");
            }

            final SchemaMetadata metadata = info.getSchemaMetadata();
            final String schemaName = metadata.getName();

            final SchemaVersionKey schemaVersionKey = new SchemaVersionKey(schemaName, version);
            final SchemaVersionInfo versionInfo = client.getSchemaVersionInfo(schemaVersionKey);
            if (versionInfo == null) {
                throw new org.apache.nifi.schema.access.SchemaNotFoundException("Could not find schema with ID '" + schemaId + "' and version '" + version + "'");
            }

            final String schemaText = versionInfo.getSchemaText();

            final SchemaIdentifier schemaIdentifier = SchemaIdentifier.of(schemaName, schemaId, version);
            final Tuple<SchemaIdentifier, String> tuple = new Tuple<>(schemaIdentifier, schemaText);
            return schemaNameToSchemaMap.computeIfAbsent(tuple, t -> {
                final Schema schema = new Schema.Parser().parse(schemaText);
                return createRecordSchema(schema, schemaText, schemaIdentifier);
            });
        } catch (final SchemaNotFoundException e) {
            throw new org.apache.nifi.schema.access.SchemaNotFoundException(e);
        }
    }


    /**
     * Converts an Avro Schema to a RecordSchema
     *
     * @param avroSchema the Avro Schema to convert
     * @param text the textual representation of the schema
     * @param schemaId the id of the schema
     * @return the Corresponding Record Schema
     */
    private RecordSchema createRecordSchema(final Schema avroSchema, final String text, final SchemaIdentifier schemaId) {
        final List<RecordField> recordFields = new ArrayList<>(avroSchema.getFields().size());
        for (final Field field : avroSchema.getFields()) {
            final String fieldName = field.name();
            final DataType dataType = determineDataType(field.schema());

            recordFields.add(new RecordField(fieldName, dataType, field.defaultVal(), field.aliases()));
        }

        final RecordSchema recordSchema = new SimpleRecordSchema(recordFields, text, "avro", schemaId);
        return recordSchema;
    }

    /**
     * Returns a DataType for the given Avro Schema
     *
     * @param avroSchema the Avro Schema to convert
     * @return a Data Type that corresponds to the given Avro Schema
     */
    private DataType determineDataType(final Schema avroSchema) {
        final Type avroType = avroSchema.getType();

        final LogicalType logicalType = avroSchema.getLogicalType();
        if (logicalType != null) {
            final String logicalTypeName = logicalType.getName();
            switch (logicalTypeName) {
                case LOGICAL_TYPE_DATE:
                    return RecordFieldType.DATE.getDataType();
                case LOGICAL_TYPE_TIME_MILLIS:
                case LOGICAL_TYPE_TIME_MICROS:
                    return RecordFieldType.TIME.getDataType();
                case LOGICAL_TYPE_TIMESTAMP_MILLIS:
                case LOGICAL_TYPE_TIMESTAMP_MICROS:
                    return RecordFieldType.TIMESTAMP.getDataType();
            }
        }

        switch (avroType) {
            case ARRAY:
                return RecordFieldType.ARRAY.getArrayDataType(determineDataType(avroSchema.getElementType()));
            case BYTES:
            case FIXED:
                return RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BYTE.getDataType());
            case BOOLEAN:
                return RecordFieldType.BOOLEAN.getDataType();
            case DOUBLE:
                return RecordFieldType.DOUBLE.getDataType();
            case ENUM:
            case STRING:
                return RecordFieldType.STRING.getDataType();
            case FLOAT:
                return RecordFieldType.FLOAT.getDataType();
            case INT:
                return RecordFieldType.INT.getDataType();
            case LONG:
                return RecordFieldType.LONG.getDataType();
            case RECORD: {
                final List<Field> avroFields = avroSchema.getFields();
                final List<RecordField> recordFields = new ArrayList<>(avroFields.size());

                for (final Field field : avroFields) {
                    final String fieldName = field.name();
                    final Schema fieldSchema = field.schema();
                    final DataType fieldType = determineDataType(fieldSchema);
                    recordFields.add(new RecordField(fieldName, fieldType, field.defaultVal(), field.aliases()));
                }

                final RecordSchema recordSchema = new SimpleRecordSchema(recordFields, avroSchema.toString(), "avro", SchemaIdentifier.EMPTY);
                return RecordFieldType.RECORD.getRecordDataType(recordSchema);
            }
            case NULL:
                return RecordFieldType.STRING.getDataType();
            case MAP:
                final Schema valueSchema = avroSchema.getValueType();
                final DataType valueType = determineDataType(valueSchema);
                return RecordFieldType.MAP.getMapDataType(valueType);
            case UNION: {
                final List<Schema> nonNullSubSchemas = avroSchema.getTypes().stream()
                    .filter(s -> s.getType() != Type.NULL)
                    .collect(Collectors.toList());

                if (nonNullSubSchemas.size() == 1) {
                    return determineDataType(nonNullSubSchemas.get(0));
                }

                final List<DataType> possibleChildTypes = new ArrayList<>(nonNullSubSchemas.size());
                for (final Schema subSchema : nonNullSubSchemas) {
                    final DataType childDataType = determineDataType(subSchema);
                    possibleChildTypes.add(childDataType);
                }

                return RecordFieldType.CHOICE.getChoiceDataType(possibleChildTypes);
            }
        }

        return null;
    }


    @Override
    public Set<SchemaField> getSuppliedSchemaFields() {
        return schemaFields;
    }
}