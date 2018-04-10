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

package org.apache.nifi.elasticsearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.lookup.LookupService;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.RecordDataType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class ElasticSearchLookupService extends AbstractControllerService implements LookupService {
    public static final PropertyDescriptor CLIENT_SERVICE = new PropertyDescriptor.Builder()
        .name("el-rest-client-service")
        .displayName("Client Service")
        .description("An ElasticSearch client service to use for running queries.")
        .identifiesControllerService(ElasticSearchClientService.class)
        .required(true)
        .build();
    public static final PropertyDescriptor INDEX = new PropertyDescriptor.Builder()
        .name("el-lookup-index")
        .displayName("Index")
        .description("The name of the index to read from")
        .required(true)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    public static final PropertyDescriptor TYPE = new PropertyDescriptor.Builder()
        .name("el-lookup-type")
        .displayName("Type")
        .description("The type of this document (used by Elasticsearch for indexing and searching)")
        .required(false)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    public static final PropertyDescriptor SCHEMA = new PropertyDescriptor.Builder()
        .name("el-lookup-schema")
        .displayName("Schema")
        .description("The record schema to be applied to results returned from ElasticSearch.")
        .required(false)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .addValidator((subject, input, context) -> {
            ValidationResult result;
            try {
                new Schema.Parser().parse(input);
                result = new ValidationResult.Builder().input(input).valid(true).build();
            } catch(Exception ex) {
                result = new ValidationResult.Builder().input(input).valid(false).explanation(ex.getMessage()).build();
            }
            return result;
        })
        .build();

    static final List<PropertyDescriptor> lookupDescriptors;

    static {
        List<PropertyDescriptor> _desc = new ArrayList<>();
        _desc.add(CLIENT_SERVICE);
        _desc.add(INDEX);
        _desc.add(TYPE);
        _desc.add(SCHEMA);

        lookupDescriptors = Collections.unmodifiableList(_desc);
    }

    private ElasticSearchClientService clientService;

    private String index;
    private String type;
    private ObjectMapper mapper;
    private RecordSchema recordSchema;

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        clientService = context.getProperty(CLIENT_SERVICE).asControllerService(ElasticSearchClientService.class);
        index = context.getProperty(INDEX).getValue();
        type  = context.getProperty(TYPE).getValue();
        mapper = new ObjectMapper();

        if (context.getProperty(SCHEMA).isSet()) {
            Schema schema = new Schema.Parser().parse(context.getProperty(SCHEMA).getValue());
            recordSchema = AvroTypeUtil.createSchema(schema);
        }
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return lookupDescriptors;
    }

    @Override
    public Optional lookup(Map coordinates) throws LookupFailureException {
        validateCoordinates(coordinates);

        try {
            Record record;
            if (coordinates.containsKey("_id")) {
                record = getById((String)coordinates.get("_id"));
            } else {
                record = getByQuery((String)coordinates.get("query"));
            }

            return record == null ? Optional.empty() : Optional.of(record);
        } catch (IOException ex) {
            getLogger().error("Error during lookup.", ex);
            throw new LookupFailureException(ex);
        }
    }

    private void validateCoordinates(Map coordinates) throws LookupFailureException {
        List<String> reasons = new ArrayList<>();

        if (coordinates.containsKey("_id") && !(coordinates.get("_id") instanceof String)) {
            reasons.add("_id was supplied, but it was not a String.");
        } else if (coordinates.containsKey("query") && !(coordinates.get("query") instanceof String)) {
            reasons.add("query was supplied, but it was not a String.");
        } else if (!coordinates.containsKey("_id") && !coordinates.containsKey("query")) {
            reasons.add("Either \"_id\" or \"query\" must be supplied as keys to lookup(Map)");
        } else if (coordinates.containsKey("_id") && coordinates.containsKey("query")) {
            reasons.add("\"_id\" and \"query\" cannot be used at the same time as keys.");
        }

        if (reasons.size() > 0) {
            String error = String.join("\n", reasons);
            throw new LookupFailureException(error);
        }
    }

    private Record getById(final String _id) throws IOException, LookupFailureException {
        Map<String, Object> query = new HashMap<String, Object>(){{
            put("query", new HashMap<String, Object>() {{
                put("match", new HashMap<String, String>(){{
                    put("_id", _id);
                }});
            }});
        }};

        String json = mapper.writeValueAsString(query);

        SearchResponse response = clientService.search(json, index, type);

        if (response.getNumberOfHits() > 1) {
            throw new LookupFailureException(String.format("Expected 1 response, got %d for query %s",
                response.getNumberOfHits(), json));
        } else if (response.getNumberOfHits() == 0) {
            return null;
        }

        final Map<String, Object> source = (Map)response.getHits().get(0).get("_source");

        RecordSchema toUse = recordSchema != null ? recordSchema : convertSchema(source);

        return new MapRecord(toUse, source);
    }

    private Record getByQuery(final String query) throws LookupFailureException {
        Map<String, Object> parsed;
        try {
            parsed = mapper.readValue(query, Map.class);
            parsed.remove("from");
            parsed.remove("aggs");
            parsed.put("size", 1);

            final String json = mapper.writeValueAsString(parsed);

            SearchResponse response = clientService.search(json, index, type);

            if (response.getNumberOfHits() == 0) {
                return null;
            } else {
                final Map<String, Object> source = (Map)response.getHits().get(0).get("_source");
                RecordSchema toUse = recordSchema != null ? recordSchema : convertSchema(source);
                return new MapRecord(toUse, source);
            }

        } catch (IOException e) {
            throw new LookupFailureException(e);
        }
    }

    private RecordSchema convertSchema(Map<String, Object> result) {
        List<RecordField> fields = new ArrayList<>();
        for (Map.Entry<String, Object> entry : result.entrySet()) {

            RecordField field;
            if (entry.getValue() instanceof Integer || entry.getValue() instanceof Long) {
                field = new RecordField(entry.getKey(), RecordFieldType.LONG.getDataType());
            } else if (entry.getValue() instanceof Boolean) {
                field = new RecordField(entry.getKey(), RecordFieldType.BOOLEAN.getDataType());
            } else if (entry.getValue() instanceof Float || entry.getValue() instanceof Double) {
                field = new RecordField(entry.getKey(), RecordFieldType.DOUBLE.getDataType());
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

    @Override
    public Class<?> getValueType() {
        return Record.class;
    }

    @Override
    public Set<String> getRequiredKeys() {
        return Collections.emptySet();
    }
}
