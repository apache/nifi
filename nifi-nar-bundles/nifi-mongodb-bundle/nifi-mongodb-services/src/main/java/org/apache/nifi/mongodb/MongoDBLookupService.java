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

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.lookup.LookupService;
import org.apache.nifi.processor.util.JsonValidator;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.serialization.JsonInferenceSchemaRegistryService;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.StringUtils;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.nifi.schema.access.SchemaAccessUtils.INFER_SCHEMA;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_BRANCH_NAME;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_NAME;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_NAME_PROPERTY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_REGISTRY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_TEXT;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_TEXT_PROPERTY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_VERSION;

@Tags({"mongo", "mongodb", "lookup", "record"})
@CapabilityDescription(
    "Provides a lookup service based around MongoDB. Each key that is specified \n" +
    "will be added to a query as-is. For example, if you specify the two keys, \n" +
    "user and email, the resulting query will be { \"user\": \"tester\", \"email\": \"tester@test.com\" }.\n" +
    "The query is limited to the first result (findOne in the Mongo documentation). If no \"Lookup Value Field\" is specified " +
    "then the entire MongoDB result document minus the _id field will be returned as a record."
)
public class MongoDBLookupService extends JsonInferenceSchemaRegistryService implements LookupService<Object> {
    private volatile String databaseName;
    private volatile String collection;

    public static final PropertyDescriptor CONTROLLER_SERVICE = new PropertyDescriptor.Builder()
        .name("mongo-lookup-client-service")
        .displayName("Client Service")
        .description("A MongoDB controller service to use with this lookup service.")
        .required(true)
        .identifiesControllerService(MongoDBClientService.class)
        .build();
    public static final PropertyDescriptor DATABASE_NAME = new PropertyDescriptor.Builder()
        .name("mongo-db-name")
        .displayName("Mongo Database Name")
        .description("The name of the database to use")
        .required(true)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();
    public static final PropertyDescriptor COLLECTION_NAME = new PropertyDescriptor.Builder()
        .name("mongo-collection-name")
        .displayName("Mongo Collection Name")
        .description("The name of the collection to use")
        .required(true)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();
    public static final PropertyDescriptor LOOKUP_VALUE_FIELD = new PropertyDescriptor.Builder()
        .name("mongo-lookup-value-field")
        .displayName("Lookup Value Field")
        .description("The field whose value will be returned when the lookup key(s) match a record. If not specified then the entire " +
                "MongoDB result document minus the _id field will be returned as a record.")
        .addValidator(Validator.VALID)
        .required(false)
        .build();
    public static final PropertyDescriptor PROJECTION = new PropertyDescriptor.Builder()
        .name("mongo-lookup-projection")
        .displayName("Projection")
        .description("Specifies a projection for limiting which fields will be returned.")
        .required(false)
        .addValidator(JsonValidator.INSTANCE)
        .build();

    private String lookupValueField;

    @Override
    public Optional<Object> lookup(Map<String, Object> coordinates) throws LookupFailureException {
        /*
         * Unless the user hard-coded schema.name or schema.text into the schema access options, this is going
         * to force schema detection.
         */
        return lookup(coordinates, new HashMap<>());
    }

    @Override
    public Optional<Object> lookup(Map<String, Object> coordinates, Map<String, String> context) throws LookupFailureException {
        Map<String, Object> clean = coordinates.entrySet().stream()
            .filter(e -> !schemaNameProperty.equals(String.format("${%s}", e.getKey())))
            .collect(Collectors.toMap(
                e -> e.getKey(),
                e -> e.getValue()
            ));
        Document query = new Document(clean);

        if (coordinates.size() == 0) {
            throw new LookupFailureException("No keys were configured. Mongo query would return random documents.");
        }

        try {
            Document result = findOne(query, projection);

            if(result == null) {
                return Optional.empty();
            } else if (!StringUtils.isEmpty(lookupValueField)) {
                return Optional.ofNullable(result.get(lookupValueField));
            } else {
                RecordSchema schema = loadSchema(context, result);

                return Optional.ofNullable(new MapRecord(schema, result));
            }
        } catch (Exception ex) {
            getLogger().error("Error during lookup {}", new Object[]{ query.toJson() }, ex);
            throw new LookupFailureException(ex);
        }
    }

    private RecordSchema loadSchema(Map<String, String> context, Document doc) {
        try {
            return getSchema(context, doc, null);
        } catch (Exception ex) {
            return null;
        }
    }

    private volatile Document projection;
    private MongoDBClientService controllerService;
    private String schemaNameProperty;

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        this.lookupValueField = context.getProperty(LOOKUP_VALUE_FIELD).getValue();
        this.controllerService = context.getProperty(CONTROLLER_SERVICE).asControllerService(MongoDBClientService.class);

        this.schemaNameProperty = context.getProperty(SchemaAccessUtils.SCHEMA_NAME).getValue();

        this.databaseName = context.getProperty(DATABASE_NAME).evaluateAttributeExpressions().getValue();
        this.collection   = context.getProperty(COLLECTION_NAME).evaluateAttributeExpressions().getValue();

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
        AllowableValue[] strategies = new AllowableValue[] {
            SCHEMA_NAME_PROPERTY, SCHEMA_TEXT_PROPERTY, INFER_SCHEMA
        };
        List<PropertyDescriptor> _temp = new ArrayList<>();
        _temp.add(new PropertyDescriptor.Builder()
                .fromPropertyDescriptor(SCHEMA_ACCESS_STRATEGY)
                .allowableValues(strategies)
                .defaultValue(getDefaultSchemaAccessStrategy().getValue())
                .build());

        _temp.add(SCHEMA_REGISTRY);
        _temp.add(SCHEMA_NAME);
        _temp.add(SCHEMA_VERSION);
        _temp.add(SCHEMA_BRANCH_NAME);
        _temp.add(SCHEMA_TEXT);
        _temp.add(CONTROLLER_SERVICE);
        _temp.add(DATABASE_NAME);
        _temp.add(COLLECTION_NAME);
        _temp.add(LOOKUP_VALUE_FIELD);
        _temp.add(PROJECTION);

        return Collections.unmodifiableList(_temp);
    }

    private Document findOne(Document query, Document projection) {
        MongoCollection col = controllerService.getDatabase(databaseName).getCollection(collection);
        MongoCursor<Document> it = (projection != null ? col.find(query).projection(projection) : col.find(query)).iterator();
        Document retVal = it.hasNext() ? it.next() : null;
        it.close();

        return retVal;
    }
}
