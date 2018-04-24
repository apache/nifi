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

package org.apache.nifi.mongodb;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.lookup.LookupService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schemaregistry.services.SchemaRegistry;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.apache.nifi.util.StringUtils;
import org.bson.Document;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@Tags({"mongo", "mongodb", "lookup", "record"})
@CapabilityDescription(
    "Provides a lookup service based around MongoDB. Each key that is specified \n" +
    "will be added to a query as-is. For example, if you specify the two keys, \n" +
    "user and email, the resulting query will be { \"user\": \"tester\", \"email\": \"tester@test.com\" }.\n" +
    "The query is limited to the first result (findOne in the Mongo documentation). If no \"Lookup Value Field\" is specified " +
    "then the entire MongoDB result document minus the _id field will be returned as a record."
)
public class MongoDBLookupService extends MongoDBControllerService implements LookupService<Object> {

    public static final PropertyDescriptor LOOKUP_VALUE_FIELD = new PropertyDescriptor.Builder()
        .name("mongo-lookup-value-field")
        .displayName("Lookup Value Field")
        .description("The field whose value will be returned when the lookup key(s) match a record. If not specified then the entire " +
                "MongoDB result document minus the _id field will be returned as a record.")
        .addValidator(Validator.VALID)
        .required(false)
        .build();
    public static final PropertyDescriptor SCHEMA_REGISTRY = new PropertyDescriptor.Builder()
        .name("mongo-lookup-schema-registry")
        .displayName("Schema Registry")
        .description("If specified, this avro schema will be used for all objects loaded from MongoDB using this service. If left blank, " +
                "the service will attempt to determine the schema from the results.")
        .required(false)
        .identifiesControllerService(SchemaRegistry.class)
        .build();
    public static final PropertyDescriptor RECORD_SCHEMA_NAME = new PropertyDescriptor.Builder()
        .name("mongo-lookup-record-schema-name")
        .displayName("Record Schema Name")
        .description("If specified, the value will be used to lookup a schema in the configured schema registry.")
        .required(false)
        .addValidator(Validator.VALID)
        .build();
    public static final PropertyDescriptor PROJECTION = new PropertyDescriptor.Builder()
        .name("mongo-lookup-projection")
        .displayName("Projection")
        .description("Specifies a projection for limiting which fields will be returned.")
        .required(false)
        .build();

    private String lookupValueField;

    private static final List<PropertyDescriptor> lookupDescriptors;

    static {
        lookupDescriptors = new ArrayList<>();
        lookupDescriptors.addAll(descriptors);
        lookupDescriptors.add(LOOKUP_VALUE_FIELD);
        lookupDescriptors.add(SCHEMA_REGISTRY);
        lookupDescriptors.add(RECORD_SCHEMA_NAME);
        lookupDescriptors.add(PROJECTION);
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        List<ValidationResult> problems = new ArrayList<>();

        PropertyValue registry = validationContext.getProperty(SCHEMA_REGISTRY);
        PropertyValue schemaName = validationContext.getProperty(RECORD_SCHEMA_NAME);

        if (registry.isSet() && !schemaName.isSet()) {
            problems.add(new ValidationResult.Builder()
                .explanation("If the registry is set, the schema name parameter must be set too.")
                .build());
        } else if (!registry.isSet() && schemaName.isSet()) {
            problems.add(new ValidationResult.Builder()
                .explanation("If the schema name is set, the schema registry parameter must be set too.")
                .build());
        }

        return problems;
    }

    @Override
    public Optional<Object> lookup(Map<String, Object> coordinates) throws LookupFailureException {
        Map<String, Object> clean = new HashMap<>();
        clean.putAll(coordinates);
        Document query = new Document(clean);

        if (coordinates.size() == 0) {
            throw new LookupFailureException("No keys were configured. Mongo query would return random documents.");
        }

        try {
            Document result = projection != null ? this.findOne(query, projection) : this.findOne(query);

            if(result == null) {
                return Optional.empty();
            } else if (!StringUtils.isEmpty(lookupValueField)) {
                return Optional.ofNullable(result.get(lookupValueField));
            } else {
                RecordSchema toUse = schema != null ? schema : convertSchema(result);
                return Optional.ofNullable(new MapRecord(toUse, result));
            }
        } catch (Exception ex) {
            getLogger().error("Error during lookup {}", new Object[]{ query.toJson() }, ex);
            throw new LookupFailureException(ex);
        }
    }

    private RecordSchema convertSchema(Map<String, Object> result) {
        List<RecordField> fields = new ArrayList<>();
        for (Map.Entry<String, Object> entry : result.entrySet()) {

            RecordField field;
            if (entry.getValue() instanceof Integer) {
                field = new RecordField(entry.getKey(), RecordFieldType.INT.getDataType());
            } else if (entry.getValue() instanceof Long) {
                field = new RecordField(entry.getKey(), RecordFieldType.LONG.getDataType());
            } else if (entry.getValue() instanceof Boolean) {
                field = new RecordField(entry.getKey(), RecordFieldType.BOOLEAN.getDataType());
            } else if (entry.getValue() instanceof Double) {
                field = new RecordField(entry.getKey(), RecordFieldType.DOUBLE.getDataType());
            } else if (entry.getValue() instanceof Date) {
                field = new RecordField(entry.getKey(), RecordFieldType.DATE.getDataType());
            } else if (entry.getValue() instanceof List) {
                field = new RecordField(entry.getKey(), RecordFieldType.ARRAY.getDataType());
            } else if (entry.getValue() instanceof Map) {
                RecordSchema nestedSchema = convertSchema((Map)entry.getValue());
                RecordDataType rdt = new RecordDataType(nestedSchema);
                field = new RecordField(entry.getKey(), rdt);
            } else {
                field = new RecordField(entry.getKey(), RecordFieldType.STRING.getDataType());
            }
            fields.add(field);
        }

        return new SimpleRecordSchema(fields);
    }

    private volatile RecordSchema schema;
    private volatile Document projection;

    @Override
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException, IOException, InterruptedException {
        this.lookupValueField = context.getProperty(LOOKUP_VALUE_FIELD).getValue();

        SchemaRegistry registry = context.getProperty(SCHEMA_REGISTRY).asControllerService(SchemaRegistry.class);
        final String name = context.getProperty(RECORD_SCHEMA_NAME).getValue();

        if (registry != null) {
            try {
                SchemaIdentifier identifier = SchemaIdentifier.builder()
                        .name(name)
                        .build();
                schema = registry.retrieveSchema(identifier);
            } catch (SchemaNotFoundException e) {
                getLogger().error(String.format("Could not find schema named %s", name), e);
                throw new InitializationException(e);
            }
        }

        String configuredProjection = context.getProperty(PROJECTION).isSet()
            ? context.getProperty(PROJECTION).getValue()
            : null;
        if (!StringUtils.isBlank(configuredProjection)) {
            projection = Document.parse(configuredProjection);
        }

        super.onEnabled(context);
    }

    @Override
    public Class<?> getValueType() {
        return Record.class;
    }

    @Override
    public Set<String> getRequiredKeys() {
        return Collections.emptySet();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return lookupDescriptors;
    }
}
